"""
Rule: Member voted on a bill whose industry sector overlaps with their held stock.
This is one of the strongest public-data signal patterns, but not proof of wrongdoing.
"""
import json
from dataclasses import dataclass, field
from typing import Optional

from ...detection.industry_map import (
    bill_sectors,
    ticker_to_sector,
    committee_sectors as _committee_sectors,
)


@dataclass
class ConflictCandidate:
    conflict_type: str
    raw_score: float
    vote_id: Optional[str] = None
    bill_id: Optional[str] = None
    asset_id: Optional[int] = None
    transaction_id: Optional[int] = None
    contribution_id: Optional[int] = None
    evidence: dict = field(default_factory=dict)


def detect(member_votes: list[dict], assets: list[dict], bills: dict, committee_memberships: list[dict] | None = None) -> list[ConflictCandidate]:
    """
    member_votes: list of {vote_id, bill_id, position, vote_date, policy_area, subjects_json}
    assets: list of {id, ticker, asset_name, asset_type, value_max, sector, year, owner}
    bills: dict of bill_id → bill record (pre-fetched for efficiency)
    """
    results = []

    # Build the set of sectors regulated by this member's committee assignments
    committee_regulated: set[str] = set()
    for cm in (committee_memberships or []):
        for s in _committee_sectors(cm.get("committee_code", "")):
            committee_regulated.add(s)

    # Build sector index for assets (skip diversified funds and zero-value holdings)
    asset_sectors: dict[str, list[dict]] = {}  # sector → [asset]
    for asset in assets:
        if asset.get("value_max") is not None and float(asset.get("value_max") or 0) < 1000:
            continue

        # Determine sector
        sector = asset.get("sector")
        if not sector and asset.get("ticker"):
            sector = ticker_to_sector(asset["ticker"])
        if not sector:
            continue

        if sector == "diversified" or sector == "unknown":
            continue

        asset_sectors.setdefault(sector, []).append(asset)

    for vote_rec in member_votes:
        bill_id = vote_rec.get("bill_id")
        if not bill_id:
            continue

        bill = bills.get(bill_id)
        if not bill:
            continue

        policy_area = bill.get("policy_area") or ""
        subjects = json.loads(bill.get("subjects_json") or "[]")
        sectors = bill_sectors(policy_area, subjects)
        bill_text = " ".join(
            part for part in [bill.get("title"), bill.get("short_title")] if part
        ).lower()

        if not sectors:
            continue

        position = vote_rec.get("position", "")
        voted_yes = position in ("Yea", "Yes")

        for sector in sectors:
            matched_assets = asset_sectors.get(sector, [])
            for asset in matched_assets:
                asset_class = (asset.get("asset_class") or asset.get("asset_type") or "unknown").lower()
                if asset.get("is_diversified") or asset_class in {
                    "diversified_fund",
                    "cash_or_deposit",
                    "money_market",
                    "treasury",
                    "municipal_bond",
                    "corporate_bond",
                    "private_business",
                    "trust",
                }:
                    continue

                exact_ticker_match = bool(
                    asset.get("ticker") and asset.get("ticker", "").lower() in bill_text
                )
                exact_company_match = _asset_name_matches_bill(asset.get("asset_name"), bill_text)
                narrow_industry_match = not (exact_ticker_match or exact_company_match) and _is_narrow_industry_match(
                    bill_text,
                    policy_area,
                    subjects,
                    sector,
                )
                committee_match = sector in committee_regulated

                # Sector-only signals with small holdings are too weak to be meaningful
                sector_only = not (exact_ticker_match or exact_company_match or narrow_industry_match or committee_match)
                if sector_only and float(asset.get("value_max") or 0) < 50_000:
                    continue

                # Compute raw score (0–100)
                raw_score = _compute_score(
                    voted_yes=voted_yes,
                    asset=asset,
                    bill=bill,
                    sector=sector,
                    committee_sector_match=committee_match,
                )

                results.append(ConflictCandidate(
                    conflict_type="vote_holding",
                    raw_score=raw_score,
                    vote_id=vote_rec.get("vote_id"),
                    bill_id=bill_id,
                    asset_id=asset.get("id"),
                    evidence={
                        "position": position,
                        "sector": sector,
                        "policy_area": policy_area,
                        "asset_name": asset.get("asset_name"),
                        "ticker": asset.get("ticker"),
                        "value_max": asset.get("value_max"),
                        "owner": asset.get("owner"),
                        "bill_title": bill.get("short_title") or bill.get("title"),
                        "exact_ticker_match": exact_ticker_match,
                        "exact_company_match": exact_company_match,
                        "narrow_industry_match": narrow_industry_match,
                        "committee_sector_match": committee_match,
                        "sector_match": True,
                        "source_quality": "public_disclosure_with_bill_and_vote_records",
                        "bill_source_url": bill.get("govinfo_url"),
                        "vote_source_url": vote_rec.get("vote_source_url"),
                        "asset_source_url": asset.get("disclosure_source_url"),
                        "asset_source_file": asset.get("disclosure_raw_file_path"),
                        "asset_parser_source": asset.get("disclosure_source"),
                        "disclosure_id": asset.get("disclosure_id"),
                    },
                ))

    return results


def _compute_score(voted_yes: bool, asset: dict, bill: dict, sector: str, committee_sector_match: bool = False) -> float:
    val = float(asset.get("value_max") or 0)

    # Voting in favor raises signal strength, but broad overlaps should stay conservative.
    vote_score = 26.0 if voted_yes else 10.0

    # Holding size on a continuous scale.
    if val >= 5_000_000:
        size_score = 30.0
    elif val >= 1_000_000:
        size_score = 24.0
    elif val >= 500_000:
        size_score = 18.0
    elif val >= 250_000:
        size_score = 13.0
    elif val >= 100_000:
        size_score = 9.0
    elif val >= 50_000:
        size_score = 5.0
    elif val >= 15_000:
        size_score = 3.0
    else:
        size_score = 1.0

    score = vote_score + size_score

    # Boost when the vote falls within the member's own committee jurisdiction.
    if committee_sector_match:
        score += 15.0

    # Company/ticker matches justify stronger scores than sector overlap alone.
    ticker = asset.get("ticker")
    title = (bill.get("title") or "").lower()
    short = (bill.get("short_title") or "").lower()
    if ticker and (ticker.lower() in title or ticker.lower() in short):
        score += 28.0
    elif _asset_name_matches_bill(asset.get("asset_name"), f"{title} {short}"):
        score += 24.0
    elif _is_narrow_industry_match(f"{title} {short}", bill.get("policy_area") or "", json.loads(bill.get("subjects_json") or "[]"), sector):
        score += 12.0

    # Discount for family holdings
    owner = asset.get("owner", "self")
    if owner == "spouse":
        score *= 0.55
    elif owner == "dependent":
        score *= 0.4
    elif owner == "joint":
        score *= 0.75

    return min(score, 100.0)


def _asset_name_matches_bill(asset_name: Optional[str], bill_text: str) -> bool:
    if not asset_name:
        return False
    cleaned = " ".join(part for part in asset_name.lower().replace(",", " ").split() if len(part) > 2)
    if not cleaned:
        return False
    variants = [cleaned]
    if " inc" in cleaned:
        variants.append(cleaned.replace(" inc", ""))
    if " corp" in cleaned:
        variants.append(cleaned.replace(" corp", ""))
    return any(variant and variant in bill_text for variant in variants)


def _is_narrow_industry_match(
    bill_text: str,
    policy_area: str,
    subjects: list[str],
    sector: str,
) -> bool:
    narrow_keywords = {
        "energy": ["pipeline", "drilling", "refinery", "petroleum", "liquefied natural gas"],
        "financials": ["hedge fund", "credit union", "mortgage servicer", "private equity", "securities"],
        "health_care": ["pharmaceutical", "biotech", "drug", "hospital", "medical device"],
        "information_technology": ["semiconductor", "cybersecurity", "software", "artificial intelligence"],
        "communication_services": ["telecommunications", "broadband", "broadcast", "social media"],
        "defense": ["defense contractor", "weapon", "aerospace", "military"],
    }
    text = " ".join([bill_text, policy_area.lower(), " ".join(s.lower() for s in subjects)])
    return any(keyword in text for keyword in narrow_keywords.get(sector, []))
