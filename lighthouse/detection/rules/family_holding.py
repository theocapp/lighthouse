"""
Rule: Spouse or dependent asset creates an indirect signal on a vote.
Logic mirrors vote_holding.py but applies a score discount for indirect ownership.
"""
import json

from ..rules.vote_holding import ConflictCandidate, _compute_score as _base_score
from ...detection.industry_map import (
    asset_name_is_diversified,
    bill_sectors,
    ticker_to_sector,
)

FAMILY_OWNERS = {"spouse", "joint", "dependent"}


def detect(
    member_votes: list[dict],
    assets: list[dict],
    bills: dict,
    family_discount: float = 0.6,
) -> list[ConflictCandidate]:
    """
    Same as vote_holding but only considers assets owned by family members.
    family_discount is applied to the final raw_score.
    """
    family_assets = [a for a in assets if (a.get("owner") or "self") in FAMILY_OWNERS]
    if not family_assets:
        return []

    # Build sector index for family assets
    asset_sectors: dict[str, list[dict]] = {}
    for asset in family_assets:
        if float(asset.get("value_max") or 0) < 1000:
            continue
        if asset_name_is_diversified(asset.get("asset_name") or ""):
            continue
        sector = asset.get("sector") or ticker_to_sector(asset.get("ticker") or "")
        if not sector or sector in ("unknown", "diversified"):
            continue
        asset_sectors.setdefault(sector, []).append(asset)

    results = []
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

        if not sectors:
            continue

        voted_yes = vote_rec.get("position", "") in ("Yea", "Yes")

        for sector in sectors:
            for asset in asset_sectors.get(sector, []):
                raw_score = _base_score(
                    voted_yes=voted_yes, asset=asset, bill=bill, sector=sector
                ) * family_discount

                results.append(ConflictCandidate(
                    conflict_type="family_holding",
                    raw_score=raw_score,
                    vote_id=vote_rec.get("vote_id"),
                    bill_id=bill_id,
                    asset_id=asset.get("id"),
                    evidence={
                        "position": vote_rec.get("position"),
                        "sector": sector,
                        "policy_area": policy_area,
                        "asset_name": asset.get("asset_name"),
                        "ticker": asset.get("ticker"),
                        "value_max": asset.get("value_max"),
                        "owner": asset.get("owner"),
                        "family_discount": family_discount,
                        "sector_match": True,
                        "source_quality": "public_disclosure_with_bill_and_vote_records",
                        "bill_source_url": bill.get("govinfo_url"),
                        "vote_source_url": vote_rec.get("vote_source_url"),
                        "asset_source_url": asset.get("disclosure_source_url"),
                        "asset_parser_source": asset.get("disclosure_source"),
                        "disclosure_id": asset.get("disclosure_id"),
                    },
                ))

    return results
