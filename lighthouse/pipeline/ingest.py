"""
Full ingestion pipeline — runs all collectors in dependency order.
Members must be ingested first (FK root), then bills/votes, then financial data.
"""
import hashlib
import json
import logging
import re
from datetime import datetime
from pathlib import Path
from typing import Optional

from sqlalchemy import bindparam, create_engine, text
from sqlalchemy.orm import Session

from ..collectors.congress_api import CongressApiCollector
from ..collectors.fec import FecCollector, normalize_contribution
from ..collectors.govinfo import GovInfoCollector
from ..collectors.house_votes import HouseVoteCollector
from ..collectors.house_disclosures import HouseDisclosuresCollector
from ..collectors.house_stocks import HouseStocksCollector, normalize_house_transaction
from ..collectors.senate_disclosures import SenateDisclosuresCollector
from ..collectors.senate_stocks import SenateStocksCollector, normalize_senate_transaction
from ..config import Config
from ..db import queries as q
from ..db.models import (
    Bill, BillCosponsor, CampaignContribution, CommitteeMembership,
    Conflict, FinancialDisclosure, IngestionLog, MemberVote, RawBillStatusFile,
    RawFecCandidateCommitteeLinkage, RawFecCommittee, RawFecIndividualContribution,
    RawVoteFile, StockTransaction, Vote,
)
from ..detection.industry_map import bill_sectors, ticker_to_sector
from ..parsers.bill_parser import (
    extract_billstatus_identity,
    parse_congress_bill_summary,
    parse_billstatus_content,
    parse_cosponsors_from_content,
)
from ..parsers.disclosure_parser import parse_html_disclosure, parse_pdf_disclosure
from ..parsers.house_vote_parser import (
    extract_house_vote_identity,
    parse_house_member_votes,
    parse_house_vote_content,
)
from ..parsers.legislator_parser import (
    iter_legislator_rows,
    parse_legislator_identifiers,
    parse_legislator_row,
)
from ..parsers.member_parser import parse_committee_membership, parse_member
from ..parsers.transaction_parser import parse_house_ptr_pdf
from ..parsers.vote_parser import parse_member_votes, parse_vote

log = logging.getLogger(__name__)


class IngestPipeline:

    def __init__(self, session: Session, config: Config):
        self.session = session
        self.config = config
        self.cache_dir = config.cache_dir
        self.congress = config.congress.current

        self._congress_api = CongressApiCollector(
            api_key=config.api_keys.congress_gov,
            cache_dir=self.cache_dir,
            rate=config.rate_limits.congress_api,
        )
        self._govinfo = GovInfoCollector(
            cache_dir=self.cache_dir,
            rate=config.rate_limits.govinfo,
        )
        self._house_stocks = HouseStocksCollector(
            cache_dir=self.cache_dir,
            rate=config.rate_limits.house_watcher,
        )
        self._house_votes = HouseVoteCollector(
            cache_dir=self.cache_dir,
            rate=config.rate_limits.house_watcher,
        )
        self._senate_stocks = SenateStocksCollector(
            cache_dir=self.cache_dir,
            rate=config.rate_limits.senate_watcher,
        )

    def run(self, sources: Optional[list[str]] = None):
        """
        Run the full ingestion pipeline.
        sources: list of source names to run, or None for all.
        Order matters — members must run before bills/votes.
        """
        all_sources = ["members", "identities", "committees", "bills", "votes", "stocks", "disclosures", "fec"]
        selected = sources or all_sources

        for source in all_sources:
            if source not in selected:
                continue
            log.info("=== Ingesting: %s ===", source)
            try:
                getattr(self, f"_ingest_{source}")()
                self._log_success(source)
            except Exception as exc:
                log.error("Failed to ingest %s: %s", source, exc, exc_info=True)
                self._log_error(source, str(exc))

        self.session.commit()

    def _ingest_members(self):
        added = updated = 0
        for raw in self._congress_api.get_members(self.congress):
            data = parse_member(raw)
            obj = q.upsert_member(self.session, data)
            if obj in self.session.new:
                added += 1
            else:
                updated += 1

        self.session.flush()
        log.info("Members: %d added, %d updated", added, updated)

    def _ingest_committees(self):
        # Delete existing memberships for this congress then re-insert
        self.session.query(CommitteeMembership).filter(
            CommitteeMembership.congress == self.congress
        ).delete()

        # Get member bioguide_ids from DB
        from ..db.models import Member
        members = (
            self.session.query(Member)
            .filter(
                Member.is_active.is_(True),
                Member.chamber == "house",
            )
            .all()
        )
        count = 0
        for member in members:
            committees = self._congress_api.get_member_committees(member.bioguide_id)
            for c in committees:
                data = parse_committee_membership(member.bioguide_id, c, self.congress)
                self.session.add(CommitteeMembership(**data))
                count += 1

        self.session.flush()
        log.info("Committee memberships: %d inserted", count)

    def _ingest_identities(self):
        path = _resolve_legislators_path(self.config.data.legislators_path)
        if not path:
            log.warning("Legislator identity CSV not found, skipping.")
            return

        added = updated = identifiers = 0
        for row in iter_legislator_rows(path):
            member_data = parse_legislator_row(row)
            obj = q.upsert_member(self.session, member_data)
            if obj in self.session.new:
                added += 1
            else:
                updated += 1

            q.replace_member_identifiers(
                self.session,
                member_data["bioguide_id"],
                parse_legislator_identifiers(row, member_data["chamber"]),
                source="legislators_csv",
            )
            identifiers += 1

        self.session.flush()
        log.info(
            "Member identities from %s: %d added, %d updated, %d rows mapped",
            path,
            added,
            updated,
            identifiers,
        )

    def _ingest_bills(self):
        raw_count = self._ingest_billstatus_raw()

        added = updated = 0
        raw_rows = (
            self.session.query(RawBillStatusFile)
            .filter(RawBillStatusFile.congress == self.congress)
            .all()
        )
        for raw_row in raw_rows:
            bill_data = parse_billstatus_content(raw_row.xml_content)
            if not bill_data:
                continue

            bill_data["govinfo_url"] = raw_row.source_url

            subjects = json.loads(bill_data.get("subjects_json") or "[]")
            sectors = bill_sectors(bill_data.get("policy_area") or "", subjects)
            bill_data["industries_json"] = json.dumps(sectors)

            obj = q.upsert_bill(self.session, bill_data)
            if obj in self.session.new:
                added += 1
            else:
                updated += 1

            self.session.query(BillCosponsor).filter(
                BillCosponsor.bill_id == bill_data["bill_id"]
            ).delete()
            for cs in parse_cosponsors_from_content(raw_row.xml_content):
                from ..db.models import Member
                if self.session.get(Member, cs["bioguide_id"]):
                    self.session.add(BillCosponsor(**cs))

        self.session.flush()
        api_added, api_updated = self._ingest_member_legislation_bills()
        log.info("Bills: %d raw files, %d added, %d updated", raw_count, added, updated)
        if api_added or api_updated:
            log.info("Congress API bill supplement: %d added, %d updated", api_added, api_updated)

    def _ingest_member_legislation_bills(self) -> tuple[int, int]:
        if not self.config.api_keys.congress_gov:
            return 0, 0

        from ..db.models import Member

        added = updated = 0
        members = self.session.query(Member).filter(Member.is_active.is_(True)).all()
        for member in members:
            for raw in self._congress_api.get_member_sponsored_legislation(member.bioguide_id):
                if raw.get("congress") != self.congress:
                    continue
                bill_data = parse_congress_bill_summary(raw, sponsor_bioguide=member.bioguide_id)
                if not bill_data:
                    continue
                bill_data["industries_json"] = json.dumps(
                    bill_sectors(bill_data.get("policy_area") or "", [])
                )
                obj = q.upsert_bill(self.session, bill_data)
                if obj in self.session.new:
                    added += 1
                else:
                    updated += 1

            for raw in self._congress_api.get_member_cosponsored_legislation(member.bioguide_id):
                if raw.get("congress") != self.congress:
                    continue
                bill_data = parse_congress_bill_summary(raw)
                if not bill_data:
                    continue
                bill_data["industries_json"] = json.dumps(
                    bill_sectors(bill_data.get("policy_area") or "", [])
                )
                obj = q.upsert_bill(self.session, bill_data)
                if obj in self.session.new:
                    added += 1
                else:
                    updated += 1

                if self.session.get(BillCosponsor, (bill_data["bill_id"], member.bioguide_id)) is None:
                    self.session.add(BillCosponsor(
                        bill_id=bill_data["bill_id"],
                        bioguide_id=member.bioguide_id,
                        cosponsor_date=None,
                    ))

        self.session.flush()
        return added, updated

    def _ingest_billstatus_raw(self) -> int:
        imported = 0

        for xml_path in self._iter_billstatus_sources():
            try:
                xml_content = xml_path.read_text(encoding="utf-8", errors="replace")
            except Exception as exc:
                log.warning("Could not read BILLSTATUS XML %s: %s", xml_path, exc)
                continue

            identity = extract_billstatus_identity(xml_content)
            if not identity or identity.get("congress") != self.congress:
                continue

            xml_sha256 = hashlib.sha256(xml_content.encode("utf-8")).hexdigest()
            obj = self.session.get(RawBillStatusFile, identity["bill_id"])
            if obj is None:
                obj = RawBillStatusFile(
                    bill_id=identity["bill_id"],
                    congress=identity["congress"],
                    bill_type=identity["bill_type"],
                    bill_number=identity["bill_number"],
                    source_path=str(xml_path),
                    source_url=_billstatus_source_url(identity),
                    file_name=xml_path.name,
                    xml_sha256=xml_sha256,
                    xml_content=xml_content,
                )
                self.session.add(obj)
            elif obj.xml_sha256 != xml_sha256:
                obj.bill_type = identity["bill_type"]
                obj.bill_number = identity["bill_number"]
                obj.congress = identity["congress"]
                obj.source_path = str(xml_path)
                obj.source_url = _billstatus_source_url(identity)
                obj.file_name = xml_path.name
                obj.xml_sha256 = xml_sha256
                obj.xml_content = xml_content

            imported += 1

        self.session.flush()
        return imported

    def _iter_billstatus_sources(self):
        archive_dir = _resolve_billstatus_dir(self.config.data.billstatus_xml_dir)
        if archive_dir:
            yield from sorted(archive_dir.glob("*.xml"))
            return

        for session_num in [1, 2]:
            yield from self._govinfo.download_all(self.congress, session_num)

    def _ingest_votes(self):
        raw_count = self._ingest_house_votes_raw()
        count = 0

        raw_rows = (
            self.session.query(RawVoteFile)
            .filter(
                RawVoteFile.congress == self.congress,
                RawVoteFile.chamber == "house",
            )
            .all()
        )

        for raw_row in raw_rows:
            vote_data = parse_house_vote_content(raw_row.raw_content)
            if not vote_data:
                continue

            vote_data["source_url"] = raw_row.source_url
            if vote_data.get("bill_id") and not self.session.get(Bill, vote_data["bill_id"]):
                vote_data["bill_id"] = None
            vote_id = vote_data["vote_id"]

            existing = self.session.get(Vote, vote_id)
            if existing:
                for k, v in vote_data.items():
                    setattr(existing, k, v)
            else:
                self.session.add(Vote(**vote_data))

            self.session.query(MemberVote).filter(
                MemberVote.vote_id == vote_id
            ).delete()
            for mv in parse_house_member_votes(raw_row.raw_content, vote_id):
                from ..db.models import Member
                if self.session.get(Member, mv["bioguide_id"]):
                    self.session.add(MemberVote(**mv))

            count += 1

        if self.config.api_keys.congress_gov:
            log.warning("Senate vote ingestion is not yet migrated off the Congress API; only House votes were loaded.")
        else:
            log.warning("Senate vote ingestion skipped: no Congress API key configured and the public Senate feed blocks scripted access here.")

        self.session.flush()
        log.info("Votes: %d raw house files, %d normalized house votes", raw_count, count)

    def _ingest_house_votes_raw(self) -> int:
        imported = 0

        for session_num in [1, 2]:
            year = _congress_start_year(self.congress) + (session_num - 1)
            for xml_path in self._house_votes.download_votes(year):
                try:
                    raw_content = xml_path.read_text(encoding="utf-8", errors="replace")
                except Exception as exc:
                    log.warning("Could not read House vote XML %s: %s", xml_path, exc)
                    continue

                identity = extract_house_vote_identity(raw_content)
                if not identity or identity["congress"] != self.congress or identity["session"] != session_num:
                    continue

                content_sha256 = hashlib.sha256(raw_content.encode("utf-8")).hexdigest()
                source_url = f"https://clerk.house.gov/evs/{year}/roll{identity['vote_number']:03d}.xml"

                obj = self.session.get(RawVoteFile, identity["vote_id"])
                if obj is None:
                    obj = RawVoteFile(
                        vote_id=identity["vote_id"],
                        chamber="house",
                        congress=identity["congress"],
                        session=identity["session"],
                        vote_number=identity["vote_number"],
                        source_url=source_url,
                        source_format="xml",
                        content_sha256=content_sha256,
                        raw_content=raw_content,
                    )
                    self.session.add(obj)
                elif obj.content_sha256 != content_sha256:
                    obj.source_url = source_url
                    obj.content_sha256 = content_sha256
                    obj.raw_content = raw_content

                imported += 1

        self.session.flush()
        return imported

    def _ingest_stocks(self):
        from ..db.models import Member
        members = self.session.query(Member).all()
        self.session.query(StockTransaction).delete()

        # Build multi-variant name → bioguide lookup for watcher APIs
        lookup = _build_name_lookup(members)

        count = 0

        # Try watcher aggregator APIs first (fast but often offline)
        try:
            for raw in self._house_stocks.get_all_transactions():
                txn = normalize_house_transaction(raw, lookup)
                if not txn:
                    continue
                _enrich_sector(txn)
                self.session.add(StockTransaction(**txn))
                count += 1
            log.info("House watcher: %d transactions ingested", count)
        except Exception as exc:
            log.warning("House watcher ingestion failed: %s", exc)

        senate_count = 0
        try:
            for raw in self._senate_stocks.get_all_transactions():
                txn = normalize_senate_transaction(raw, lookup)
                if not txn:
                    continue
                _enrich_sector(txn)
                self.session.add(StockTransaction(**txn))
                senate_count += 1
            log.info("Senate watcher: %d transactions ingested", senate_count)
            count += senate_count
        except Exception as exc:
            log.warning("Senate watcher ingestion failed: %s", exc)

        # Always supplement with House PTR PDFs directly from the clerk.
        # Use the bulk year-wide listing (one CSRF fetch) instead of per-member searches.
        house_disc = HouseDisclosuresCollector(cache_dir=self.cache_dir)
        ptr_count = 0

        # Build office-code → bioguide_id map for fast lookup
        office_to_bioguide: dict[str, str] = {}
        for m in members:
            if m.chamber == "house":
                code = _house_office_code(m)
                if code:
                    office_to_bioguide[code] = m.bioguide_id

        try:
            all_filings = list(house_disc.get_all_filings_for_year(self.config.data.ptr_year))
            ptr_filings = [f for f in all_filings if f.get("document_type") == "ptr"]
            log.info("House PTR bulk listing: %d PTR filings found", len(ptr_filings))

            for filing in ptr_filings:
                office = _normalize_house_office(filing.get("office") or "")
                bioguide_id = office_to_bioguide.get(office)
                if not bioguide_id:
                    # Fallback: match by last name substring
                    name_field = (filing.get("name") or "").lower()
                    for m in members:
                        if m.chamber == "house" and m.last_name and m.last_name.lower() in name_field:
                            bioguide_id = m.bioguide_id
                            break
                if not bioguide_id:
                    continue
                try:
                    dest = house_disc.download_filing(filing)
                    for txn in parse_house_ptr_pdf(dest, bioguide_id):
                        _enrich_sector(txn)
                        self.session.add(StockTransaction(**txn))
                        ptr_count += 1
                except Exception as exc:
                    log.warning(
                        "House PTR parse failed for filing %s: %s",
                        filing.get("doc_id"), exc,
                    )
        except Exception as exc:
            log.warning("House PTR bulk listing failed, trying per-member fallback: %s", exc)
            for member in members:
                if member.chamber != "house":
                    continue
                try:
                    filings = house_disc.search_member(
                        member.last_name, self.config.data.ptr_year,
                        state=member.state or "", district=member.district,
                    )
                    for filing in filings:
                        if filing.get("document_type") != "ptr":
                            continue
                        dest = house_disc.download_filing(filing)
                        for txn in parse_house_ptr_pdf(dest, member.bioguide_id):
                            _enrich_sector(txn)
                            self.session.add(StockTransaction(**txn))
                            ptr_count += 1
                except Exception as exc2:
                    log.warning("House PTR per-member failed for %s: %s", member.bioguide_id, exc2)

        count += ptr_count
        self.session.flush()
        log.info("Stock transactions: %d total (%d from PTR PDFs)", count, ptr_count)

    def _ingest_disclosures(self):
        house_coll = HouseDisclosuresCollector(cache_dir=self.cache_dir)
        senate_coll = SenateDisclosuresCollector(cache_dir=self.cache_dir)

        from ..db.models import Asset, Member
        # Delete in correct dependency order: conflicts first, then assets and disclosures
        self.session.query(Conflict).delete()
        self.session.query(Asset).delete()
        self.session.query(FinancialDisclosure).delete()
        members = self.session.query(Member).filter(Member.is_active.is_(True)).all()
        disc_count = asset_count = 0
        senate_failures = 0
        skip_remaining_senate = False

        house_filings_by_office: dict[str, list[dict]] = {}
        try:
            all_house_filings = list(house_coll.get_all_filings_for_year(self.config.data.disclosure_year))
            for filing in all_house_filings:
                office = _normalize_house_office(filing.get("office") or "")
                if not office:
                    continue
                house_filings_by_office.setdefault(office, []).append(filing)
            log.info("House filing index built: %d filings", len(all_house_filings))
        except Exception as exc:
            log.warning("House year-wide disclosure listing failed, trying cached House search pages: %s", exc)
            try:
                cached_house_filings = house_coll.get_cached_filings_for_year(self.config.data.disclosure_year)
                for filing in cached_house_filings:
                    office = _normalize_house_office(filing.get("office") or "")
                    if not office:
                        continue
                    house_filings_by_office.setdefault(office, []).append(filing)
                log.info("House filing index built from cache: %d filings", len(cached_house_filings))
            except Exception as cache_exc:
                log.warning("Cached House listing parse failed, falling back to per-member searches: %s", cache_exc)
                house_filings_by_office = {}

        for member in members:
            year = self.config.data.disclosure_year
            if skip_remaining_senate and member.chamber == "senate":
                continue
            try:
                if member.chamber == "house":
                    office = _house_office_code(member)
                    filings = house_filings_by_office.get(office, []) if house_filings_by_office else house_coll.search_member(
                        member.last_name,
                        year,
                        state=member.state or "",
                        district=member.district,
                    )

                    if filings:
                        filings = [
                            filing
                            for filing in filings
                            if member.last_name.lower() in (filing.get("name") or "").lower()
                        ]

                    annual_filings = [f for f in filings if f.get("document_type") == "financial"]
                    for filing in annual_filings[:1]:
                        dest = house_coll.download_filing(filing)
                        disc_obj = FinancialDisclosure(
                            bioguide_id=member.bioguide_id,
                            filer_name=filing.get("name"),
                            filer_type="member",
                            filing_type=filing.get("filing_type", "annual"),
                            year=year,
                            filed_date=_parse_us_date(filing.get("filed_date")),
                            source="house",
                            source_url=filing.get("source_url"),
                            raw_file_path=str(dest),
                        )
                        self.session.add(disc_obj)
                        self.session.flush()
                        disc_count += 1

                        assets = parse_pdf_disclosure(dest, member.bioguide_id, disc_obj.id)
                        for a in assets:
                            _enrich_asset_sector(a)
                            self.session.add(Asset(**a))
                            asset_count += 1

                else:  # senate
                    filings = senate_coll.search_member(member.first_name, member.last_name)
                    for filing in filings[:1]:
                        dest = senate_coll.download_report(filing["report_url"], filing["report_id"])
                        disc_obj = FinancialDisclosure(
                            bioguide_id=member.bioguide_id,
                            filer_name=f"{filing.get('first_name')} {filing.get('last_name')}",
                            filer_type="member",
                            filing_type=filing.get("report_type", "annual"),
                            year=year,
                            filed_date=_parse_us_date(filing.get("filed_date")),
                            source="senate",
                            raw_file_path=str(dest),
                        )
                        self.session.add(disc_obj)
                        self.session.flush()
                        disc_count += 1

                        assets = parse_html_disclosure(dest, member.bioguide_id, disc_obj.id)
                        for a in assets:
                            _enrich_asset_sector(a)
                            self.session.add(Asset(**a))
                            asset_count += 1
                    senate_failures = 0

            except Exception as exc:
                log.warning("Disclosure ingestion failed for %s: %s", member.bioguide_id, exc)
                if member.chamber == "senate":
                    senate_failures += 1
                    if senate_failures >= 5:
                        skip_remaining_senate = True
                        log.warning("Skipping remaining senate disclosure fetches after %d consecutive failures", senate_failures)

        self.session.flush()
        log.info("Disclosures: %d filings, %d assets", disc_count, asset_count)


    def _ingest_fec(self):
        if self.config.fec_warehouse.prefer_local_db and self.config.fec_warehouse.source_db_url:
            imported = self._ingest_fec_from_local_db()
            log.info("Campaign contributions imported from local FEC warehouse: %d", imported)
            return

        if not self.config.api_keys.fec:
            log.warning("FEC API key not configured, skipping.")
            return

        fec = FecCollector(
            api_key=self.config.api_keys.fec,
            cache_dir=self.cache_dir,
            rate=self.config.rate_limits.fec_api,
        )

        from ..db.models import Member
        members = self.session.query(Member).filter(
            Member.is_active.is_(True),
        ).all()

        count = 0
        for member in members:
            try:
                cycle = self.config.data.fec_cycle
                seen_committee_ids = set()
                fec_ids = q.get_member_fec_ids(self.session, member.bioguide_id)
                if not fec_ids:
                    continue

                for fec_candidate_id in fec_ids:
                    committees = fec.get_candidate_committees(fec_candidate_id, cycle)
                    for committee in committees:
                        cid = committee.get("committee_id")
                        if not cid or cid in seen_committee_ids:
                            continue
                        seen_committee_ids.add(cid)
                        for contrib_raw in fec.get_contributions_to_committee(cid, cycle):
                            data = normalize_contribution(contrib_raw, member.bioguide_id)
                            self.session.add(CampaignContribution(**data))
                            count += 1
            except Exception as exc:
                log.warning("FEC ingestion failed for %s: %s", member.bioguide_id, exc)

        self.session.flush()
        log.info("Campaign contributions: %d ingested", count)

    def _ingest_fec_from_local_db(self) -> int:
        source_url = self.config.fec_warehouse.source_db_url
        cycles = sorted(set(self.config.fec_warehouse.cycles or [self.config.data.fec_cycle]))
        source_engine = create_engine(source_url, echo=False)

        from ..db.models import Member
        members = self.session.query(Member).filter(Member.is_active.is_(True)).all()
        member_ids = [m.bioguide_id for m in members]

        candidate_to_member: dict[str, str] = {}
        for member in members:
            for fec_id in q.get_member_fec_ids(self.session, member.bioguide_id):
                if fec_id:
                    candidate_to_member[fec_id] = member.bioguide_id

        candidate_ids = sorted(candidate_to_member.keys())
        if not candidate_ids:
            log.warning("No member FEC IDs available, skipping local FEC import.")
            return 0

        self.session.query(CampaignContribution).filter(
            CampaignContribution.bioguide_id.in_(member_ids),
            CampaignContribution.election_cycle.in_(cycles),
        ).delete(synchronize_session=False)

        linkage_stmt = text("""
            SELECT
                ccl.cand_id,
                ccl.cand_election_yr,
                ccl.fec_election_yr,
                ccl.cmte_id,
                ccl.cmte_tp,
                ccl.cmte_dsgn,
                ccl.linkage_id,
                ccl.file_year,
                cm.cmte_nm,
                cm.org_tp,
                cm.connected_org_nm,
                cm.cand_id AS committee_candidate_id
            FROM candidate_committee_linkages ccl
            LEFT JOIN committee_master cm
              ON cm.cmte_id = ccl.cmte_id
             AND cm.file_year = ccl.file_year
            WHERE ccl.file_year = :cycle
              AND ccl.cand_id IN :candidate_ids
        """).bindparams(bindparam("candidate_ids", expanding=True))

        contribution_stmt = text("""
            SELECT
                ic.cmte_id,
                ic.name,
                ic.employer,
                ic.occupation,
                ic.state,
                ic.entity_tp,
                ic.transaction_tp,
                ic.transaction_dt,
                ic.transaction_amt,
                ic.other_id,
                ic.image_num,
                ic.memo_text,
                ic.sub_id,
                ic.file_year,
                cm.cmte_nm
            FROM individual_contributions ic
            LEFT JOIN committee_master cm
              ON cm.cmte_id = ic.cmte_id
             AND cm.file_year = ic.file_year
            WHERE ic.file_year = :cycle
              AND ic.cmte_id IN :committee_ids
              AND COALESCE(ic.transaction_amt, 0) > 0
        """).bindparams(bindparam("committee_ids", expanding=True))

        imported = 0
        with source_engine.connect() as conn:
            for cycle in cycles:
                committee_map: dict[str, tuple[str, str]] = {}
                for chunk in _chunked(candidate_ids, 200):
                    rows = conn.execute(
                        linkage_stmt,
                        {"cycle": cycle, "candidate_ids": chunk},
                    ).mappings()
                    for row in rows:
                        candidate_id = row["cand_id"]
                        committee_id = row["cmte_id"]
                        if not candidate_id or not committee_id:
                            continue
                        bioguide_id = candidate_to_member.get(candidate_id)
                        if not bioguide_id:
                            continue

                        committee_map[committee_id] = (candidate_id, bioguide_id)
                        self._upsert_raw_fec_committee(row)
                        self._upsert_raw_fec_linkage(row, bioguide_id)

                if not committee_map:
                    continue

                for chunk in _chunked(sorted(committee_map.keys()), 200):
                    rows = conn.execute(
                        contribution_stmt,
                        {"cycle": cycle, "committee_ids": chunk},
                    ).mappings()
                    for row in rows:
                        committee_id = row["cmte_id"]
                        mapping = committee_map.get(committee_id)
                        if not mapping:
                            continue
                        candidate_id, bioguide_id = mapping
                        raw_obj = self._upsert_raw_fec_contribution(row, bioguide_id, candidate_id)
                        self.session.add(CampaignContribution(
                            bioguide_id=bioguide_id,
                            fec_committee_id=committee_id,
                            contributor_name=row["name"],
                            contributor_employer=row["employer"],
                            contributor_industry=raw_obj.derived_sector,
                            amount=float(row["transaction_amt"]) if row["transaction_amt"] else None,
                            contribution_date=raw_obj.transaction_date,
                            election_cycle=cycle,
                            contribution_type=_fec_contribution_type(row["entity_tp"], row["other_id"]),
                        ))
                        imported += 1

        self.session.flush()
        return imported

    def _upsert_raw_fec_committee(self, row):
        obj = self.session.get(RawFecCommittee, (row["cmte_id"], row["file_year"]))
        payload = {
            "committee_name": row["cmte_nm"],
            "committee_designation": row["cmte_dsgn"],
            "committee_type": row["cmte_tp"],
            "organization_type": row["org_tp"],
            "connected_org_name": row["connected_org_nm"],
            "candidate_id": row["committee_candidate_id"],
        }
        if obj is None:
            obj = RawFecCommittee(
                committee_id=row["cmte_id"],
                file_year=row["file_year"],
                **payload,
            )
            self.session.add(obj)
        else:
            for key, value in payload.items():
                setattr(obj, key, value)
        return obj

    def _upsert_raw_fec_linkage(self, row, bioguide_id: str):
        key = (row["cand_id"], row["cmte_id"], row["file_year"])
        obj = self.session.get(RawFecCandidateCommitteeLinkage, key)
        payload = {
            "candidate_election_year": int(row["cand_election_yr"]) if row["cand_election_yr"] else None,
            "fec_election_year": int(row["fec_election_yr"]) if row["fec_election_yr"] else None,
            "committee_type": row["cmte_tp"],
            "committee_designation": row["cmte_dsgn"],
            "linkage_id": str(row["linkage_id"]) if row["linkage_id"] is not None else None,
            "committee_name": row["cmte_nm"],
            "member_bioguide_id": bioguide_id,
        }
        if obj is None:
            obj = RawFecCandidateCommitteeLinkage(
                candidate_id=row["cand_id"],
                committee_id=row["cmte_id"],
                file_year=row["file_year"],
                **payload,
            )
            self.session.add(obj)
        else:
            for key_name, value in payload.items():
                setattr(obj, key_name, value)
        return obj

    def _upsert_raw_fec_contribution(self, row, bioguide_id: str, candidate_id: str):
        key = (str(row["sub_id"]), row["file_year"])
        obj = self.session.get(RawFecIndividualContribution, key)
        contributor_name = row["name"]
        employer = row["employer"]
        occupation = row["occupation"]
        committee_name = row["cmte_nm"]
        entity_type = row["entity_tp"]
        transaction_dt_raw = row["transaction_dt"]
        transaction_date = _parse_fec_transaction_date(transaction_dt_raw)
        derived_sector = _classify_fec_sector(
            contributor_name=contributor_name,
            employer=employer,
            occupation=occupation,
            committee_name=committee_name,
            entity_type=entity_type,
        )
        source_hash = hashlib.sha256(
            "|".join([
                str(row.get("sub_id") or ""),
                str(row.get("file_year") or ""),
                str(row.get("cmte_id") or ""),
                str(row.get("transaction_amt") or ""),
                str(transaction_dt_raw or ""),
            ]).encode("utf-8")
        ).hexdigest()

        payload = {
            "member_bioguide_id": bioguide_id,
            "candidate_id": candidate_id,
            "committee_id": row["cmte_id"],
            "committee_name": committee_name,
            "contributor_name": contributor_name,
            "contributor_employer": employer,
            "contributor_occupation": occupation,
            "contributor_state": row["state"],
            "entity_type": entity_type,
            "transaction_type": row["transaction_tp"],
            "transaction_dt_raw": transaction_dt_raw,
            "transaction_date": transaction_date,
            "amount": row["transaction_amt"],
            "other_id": row["other_id"],
            "image_num": row["image_num"],
            "memo_text": row["memo_text"],
            "derived_sector": derived_sector,
            "source_record_hash": source_hash,
        }
        if obj is None:
            obj = RawFecIndividualContribution(
                source_sub_id=str(row["sub_id"]),
                file_year=row["file_year"],
                **payload,
            )
            self.session.add(obj)
        else:
            for key_name, value in payload.items():
                setattr(obj, key_name, value)
        return obj

    def _log_success(self, source: str):
        obj = self.session.get(IngestionLog, source)
        if not obj:
            obj = IngestionLog(source=source)
            self.session.add(obj)
        obj.last_run = datetime.utcnow()
        obj.status = "ok"
        obj.error_message = None

    def _log_error(self, source: str, message: str):
        obj = self.session.get(IngestionLog, source)
        if not obj:
            obj = IngestionLog(source=source)
            self.session.add(obj)
        obj.last_run = datetime.utcnow()
        obj.status = "error"
        obj.error_message = message


def _build_name_lookup(members) -> dict[str, str]:
    """Build a name→bioguide_id dict with multiple name variants per member."""
    lookup: dict[str, str] = {}
    for m in members:
        bioguide = m.bioguide_id
        variants = set()
        if m.full_name:
            variants.add(m.full_name.lower().strip())
        if m.first_name and m.last_name:
            variants.add(f"{m.first_name} {m.last_name}".lower())
            variants.add(f"{m.last_name}, {m.first_name}".lower())
            variants.add(f"{m.last_name},{m.first_name}".lower())
        if m.last_name:
            # Last-name-only as a weak fallback (only if unique)
            last = m.last_name.lower().strip()
            if last not in lookup:
                lookup[last] = bioguide
        for v in variants:
            lookup[v] = bioguide
            # Strip honorifics
            for prefix in ("hon. ", "mr. ", "ms. ", "dr. ", "rep. ", "sen. "):
                if v.startswith(prefix):
                    lookup[v[len(prefix):]] = bioguide
    return lookup


def _normalize_house_office(office: str) -> str:
    return re.sub(r"\s+", "", office or "").upper()


def _house_office_code(member) -> str:
    if not member.state or member.district in (None, "", 0):
        return ""
    return f"{member.state.upper()}{int(member.district):02d}"


def _enrich_sector(txn: dict):
    if txn.get("sector"):
        return
    ticker = (txn.get("ticker") or "").upper()
    if ticker:
        sector = ticker_to_sector(ticker)
        if sector != "unknown":
            txn["sector"] = sector


def _enrich_asset_sector(asset: dict):
    if asset.get("sector"):
        return
    ticker = (asset.get("ticker") or "").upper()
    sector = None
    if ticker:
        sector = ticker_to_sector(ticker)
    if sector and sector != "unknown":
        asset["sector"] = sector


def _parse_us_date(raw: Optional[str]):
    if not raw:
        return None
    try:
        return datetime.strptime(raw.strip(), "%m/%d/%Y").date()
    except ValueError:
        return None


def _parse_fec_transaction_date(raw: Optional[str]):
    if not raw:
        return None
    value = "".join(ch for ch in str(raw) if ch.isdigit())
    if len(value) != 8:
        return None
    try:
        return datetime.strptime(value, "%m%d%Y").date()
    except ValueError:
        return None


def _fec_contribution_type(entity_type: Optional[str], other_id: Optional[str]) -> str:
    kind = (entity_type or "").upper()
    if other_id or kind in {"PAC", "COM", "CCM", "PTY", "CAN"}:
        return "pac"
    return "individual"


def _classify_fec_sector(
    *,
    contributor_name: Optional[str],
    employer: Optional[str],
    occupation: Optional[str],
    committee_name: Optional[str],
    entity_type: Optional[str],
) -> str:
    haystack = " ".join(filter(None, [
        contributor_name or "",
        employer or "",
        occupation or "",
        committee_name or "",
        entity_type or "",
    ])).lower()

    sector_keywords = {
        "financials": ["bank", "capital", "asset", "invest", "finance", "equity", "venture", "insurance", "lending"],
        "health_care": ["health", "medical", "hospital", "pharma", "biotech", "doctor", "physician", "nurse"],
        "information_technology": ["software", "tech", "technology", "data", "cloud", "semiconductor", "computer", "ai"],
        "energy": ["energy", "oil", "gas", "petroleum", "pipeline", "drilling", "solar", "wind"],
        "communication_services": ["media", "telecom", "wireless", "broadcast", "internet", "streaming"],
        "industrials": ["defense", "aerospace", "manufacturing", "construction", "transport", "logistics", "engineering"],
        "real_estate": ["real estate", "realtor", "property", "developer", "mortgage"],
        "utilities": ["utility", "electric", "water", "power"],
        "consumer_discretionary": ["retail", "restaurant", "hospitality", "automotive", "dealer"],
        "materials": ["mining", "chemical", "steel", "metals", "lumber"],
        "labor": ["union", "labor federation", "workers"],
        "legal": ["law", "attorney", "lawyer"],
        "government": ["government", "public service", "military", "teacher", "education"],
    }
    for sector, keywords in sector_keywords.items():
        if any(keyword in haystack for keyword in keywords):
            return sector

    if (entity_type or "").upper() == "IND":
        return "individual_other"
    return "committee_other"


def _chunked(values: list[str], size: int):
    for idx in range(0, len(values), size):
        yield values[idx: idx + size]


def _resolve_legislators_path(configured_path: str) -> Optional[Path]:
    candidates = [
        Path(configured_path),
        Path("legislators-current.csv"),
        Path(__file__).resolve().parents[2] / "legislators-current.csv",
        Path(__file__).resolve().parents[3] / "legislators-current.csv",
    ]
    for candidate in candidates:
        if candidate.exists():
            return candidate
    return None


def _resolve_billstatus_dir(configured_path: str) -> Optional[Path]:
    candidates = [
        Path(configured_path),
        Path("billstatus_xml"),
        Path("data/billstatus_xml"),
        Path("/Users/theo/billstatus_xml"),
    ]
    for candidate in candidates:
        if candidate.exists() and candidate.is_dir():
            return candidate
    return None


def _billstatus_source_url(identity: dict) -> str:
    return (
        "https://www.govinfo.gov/bulkdata/BILLSTATUS/"
        f"{identity['congress']}/{identity['bill_type']}/"
        f"BILLSTATUS-{identity['congress']}{identity['bill_type']}{identity['bill_number']}.xml"
    )


def _congress_start_year(congress: int) -> int:
    return 1789 + (congress - 1) * 2
