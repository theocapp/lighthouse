"""
Rule: Member sponsored or cosponsored a bill while holding stock
in the directly affected company or sector.
"""
import json

from ..rules.vote_holding import (
    ConflictCandidate,
    _asset_name_matches_bill,
    _is_narrow_industry_match,
)
from ...detection.industry_map import asset_name_is_diversified, bill_sectors, ticker_to_sector


def detect(
    sponsored_bills: list[dict],
    cosponsored_bills: list[dict],
    assets: list[dict],
    bills: dict,
) -> list[ConflictCandidate]:
    """
    sponsored_bills: list of bill records where member is sponsor
    cosponsored_bills: list of {bill_id, cosponsor_date} for member
    assets: list of asset records for the member
    bills: dict of bill_id → full bill record
    """
    results = []

    # Build sector → assets map
    asset_by_sector: dict[str, list[dict]] = {}
    for asset in assets:
        if float(asset.get("value_max") or 0) < 1000:
            continue
        if asset_name_is_diversified(asset.get("asset_name") or ""):
            continue
        sector = asset.get("sector") or ticker_to_sector(asset.get("ticker") or "")
        if not sector or sector in ("unknown", "diversified"):
            continue
        asset_by_sector.setdefault(sector, []).append(asset)

    def check_bill(bill: dict, is_sponsor: bool):
        bill_id = bill.get("bill_id")
        if not bill_id:
            return

        policy_area = bill.get("policy_area") or ""
        subjects = json.loads(bill.get("subjects_json") or "[]")
        sectors = bill_sectors(policy_area, subjects)
        bill_text = " ".join(
            part for part in [bill.get("title"), bill.get("short_title")] if part
        ).lower()

        for sector in sectors:
            for asset in asset_by_sector.get(sector, []):
                raw_score = _compute_score(bill, asset, is_sponsor)
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
                results.append(ConflictCandidate(
                    conflict_type="sponsorship_holding",
                    raw_score=raw_score,
                    bill_id=bill_id,
                    asset_id=asset.get("id"),
                    evidence={
                        "role": "sponsor" if is_sponsor else "cosponsor",
                        "policy_area": policy_area,
                        "sector": sector,
                        "asset_name": asset.get("asset_name"),
                        "ticker": asset.get("ticker"),
                        "value_max": asset.get("value_max"),
                        "owner": asset.get("owner"),
                        "bill_title": bill.get("short_title") or bill.get("title"),
                        "exact_ticker_match": exact_ticker_match,
                        "exact_company_match": exact_company_match,
                        "narrow_industry_match": narrow_industry_match,
                        "sector_match": True,
                        "source_quality": "public_disclosure_with_bill_metadata",
                        "bill_source_url": bill.get("govinfo_url"),
                        "asset_source_url": asset.get("disclosure_source_url"),
                        "asset_parser_source": asset.get("disclosure_source"),
                        "disclosure_id": asset.get("disclosure_id"),
                    },
                ))

    for bill in sponsored_bills:
        check_bill(bill, is_sponsor=True)

    for cosponsor_rec in cosponsored_bills:
        bill = bills.get(cosponsor_rec.get("bill_id", ""))
        if bill:
            check_bill(bill, is_sponsor=False)

    return results


def _compute_score(bill: dict, asset: dict, is_sponsor: bool) -> float:
    score = 30.0 if is_sponsor else 20.0

    val = float(asset.get("value_max") or 0)
    if val >= 1_000_000:
        score += 18.0
    elif val >= 100_000:
        score += 10.0
    elif val >= 15_000:
        score += 5.0

    title_text = " ".join(
        part for part in [bill.get("title"), bill.get("short_title")] if part
    ).lower()
    if asset.get("ticker") and asset.get("ticker", "").lower() in title_text:
        score += 24.0

    owner = asset.get("owner", "self")
    if owner == "spouse":
        score *= 0.55
    elif owner == "dependent":
        score *= 0.4
    elif owner == "joint":
        score *= 0.75

    return min(score, 100.0)
