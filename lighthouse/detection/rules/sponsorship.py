"""
Rule: Member sponsored or cosponsored a bill while holding stock
in the directly affected company or sector.
"""
import json
from typing import Optional

from ..rules.vote_holding import ConflictCandidate
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

        for sector in sectors:
            for asset in asset_by_sector.get(sector, []):
                raw_score = _compute_score(bill, asset, is_sponsor)
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
    score = 55.0 if is_sponsor else 40.0

    val = float(asset.get("value_max") or 0)
    if val >= 1_000_000:
        score += 20.0
    elif val >= 100_000:
        score += 10.0
    elif val >= 15_000:
        score += 5.0

    owner = asset.get("owner", "self")
    if owner == "spouse":
        score *= 0.6
    elif owner == "dependent":
        score *= 0.4

    return min(score, 100.0)
