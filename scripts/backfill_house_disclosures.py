#!/usr/bin/env python3
"""
Backfill House annual disclosures into assets with per-member commits.

This script avoids long full-pipeline runs by:
- building one year-wide House filing index,
- matching by office + last name,
- committing each successful member immediately.
"""
import re
import sys
from pathlib import Path

import click

sys.path.insert(0, str(Path(__file__).parent.parent))

from lighthouse.collectors.house_disclosures import HouseDisclosuresCollector
from lighthouse.config import load_config
from lighthouse.db.models import Asset, FinancialDisclosure, Member, get_session
from lighthouse.parsers.disclosure_parser import parse_pdf_disclosure
from lighthouse.pipeline.ingest import _enrich_asset_sector, _parse_us_date


def _office_code(member: Member) -> str:
    if not member.state or member.district in (None, "", 0):
        return ""
    return f"{member.state.upper()}{int(member.district):02d}"


def _norm_office(office: str) -> str:
    return re.sub(r"\s+", "", office or "").upper()


@click.command()
@click.option("--year", default=2024, type=int, show_default=True)
@click.option("--limit", default=50, type=int, show_default=True, help="Max members to add in one run")
@click.option("--config", "config_path", default=None, help="Path to config.yml")
def cli(year: int, limit: int, config_path: str | None):
    cfg = load_config(config_path)
    session = get_session(cfg.database.url)
    collector = HouseDisclosuresCollector(cache_dir=cfg.cache_dir)

    filings = list(collector.get_all_filings_for_year(year))
    by_office: dict[str, list[dict]] = {}
    for filing in filings:
        office = _norm_office(filing.get("office") or "")
        if office:
            by_office.setdefault(office, []).append(filing)

    members = (
        session.query(Member)
        .filter(Member.is_active == True, Member.chamber == "house")
        .order_by(Member.bioguide_id)
        .all()
    )

    added_members = 0
    added_disclosures = 0
    added_assets = 0

    for member in members:
        if added_members >= limit:
            break

        existing = (
            session.query(FinancialDisclosure)
            .filter(
                FinancialDisclosure.bioguide_id == member.bioguide_id,
                FinancialDisclosure.source == "house",
                FinancialDisclosure.year == year,
            )
            .first()
        )
        if existing:
            continue

        office = _office_code(member)
        candidates = by_office.get(office, [])
        candidates = [
            filing
            for filing in candidates
            if filing.get("document_type") == "financial"
            and member.last_name.lower() in (filing.get("name") or "").lower()
        ]
        if not candidates:
            continue

        filing = candidates[0]

        try:
            pdf_path = collector.download_filing(filing)

            disclosure = FinancialDisclosure(
                bioguide_id=member.bioguide_id,
                filer_name=filing.get("name") or member.full_name,
                filer_type="member",
                filing_type=filing.get("filing_type", "annual"),
                year=year,
                filed_date=_parse_us_date(filing.get("filed_date")),
                source="house",
                source_url=filing.get("source_url"),
                raw_file_path=str(pdf_path),
            )
            session.add(disclosure)
            session.flush()

            assets = parse_pdf_disclosure(pdf_path, member.bioguide_id, disclosure.id)
            if not assets:
                session.delete(disclosure)
                session.commit()
                continue

            for asset in assets:
                _enrich_asset_sector(asset)
                session.add(Asset(**asset))
                added_assets += 1

            session.commit()
            added_members += 1
            added_disclosures += 1
            click.echo(f"added {member.bioguide_id} {member.full_name} assets={len(assets)}")

        except Exception as exc:
            session.rollback()
            click.echo(f"skip {member.bioguide_id} {member.full_name}: {exc}")

    click.echo("--- summary ---")
    click.echo(f"house_filings={len(filings)}")
    click.echo(f"added_members={added_members}")
    click.echo(f"added_disclosures={added_disclosures}")
    click.echo(f"added_assets={added_assets}")
    click.echo(f"members_with_assets={session.query(Asset.bioguide_id).distinct().count()}")


if __name__ == "__main__":
    cli()
