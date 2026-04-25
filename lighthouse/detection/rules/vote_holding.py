"""
Rule: Member voted on a bill whose industry sector overlaps with their held stock.
This is the most direct and legally significant conflict pattern.
"""
import json
from dataclasses import dataclass, field
from typing import Optional

from ...detection.industry_map import (
    asset_name_is_diversified,
    bill_sectors,
    ticker_to_sector,
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


def detect(member_votes: list[dict], assets: list[dict], bills: dict) -> list[ConflictCandidate]:
    """
    member_votes: list of {vote_id, bill_id, position, vote_date, policy_area, subjects_json}
    assets: list of {id, ticker, asset_name, asset_type, value_max, sector, year, owner}
    bills: dict of bill_id → bill record (pre-fetched for efficiency)
    """
    results = []

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

        if not sectors:
            continue

        position = vote_rec.get("position", "")
        voted_yes = position in ("Yea", "Yes")

        for sector in sectors:
            matched_assets = asset_sectors.get(sector, [])
            for asset in matched_assets:

                # Skip diversified funds
                if asset_name_is_diversified(asset.get("asset_name") or ""):
                    continue

                # Compute raw score (0–100)
                raw_score = _compute_score(
                    voted_yes=voted_yes,
                    asset=asset,
                    bill=bill,
                    sector=sector,
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
                    },
                ))

    return results


def _compute_score(voted_yes: bool, asset: dict, bill: dict, sector: str) -> float:
    val = float(asset.get("value_max") or 0)

    # Vote direction: voted in favor of a bill = stronger conflict signal
    vote_score = 50.0 if voted_yes else 15.0

    # Holding size on a continuous scale (0–50 points)
    if val >= 5_000_000:
        size_score = 50.0
    elif val >= 1_000_000:
        size_score = 40.0
    elif val >= 500_000:
        size_score = 30.0
    elif val >= 250_000:
        size_score = 22.0
    elif val >= 100_000:
        size_score = 15.0
    elif val >= 50_000:
        size_score = 8.0
    elif val >= 15_000:
        size_score = 4.0
    else:
        size_score = 1.0

    score = vote_score + size_score

    # Direct ticker match (bill explicitly affects a specific company)
    ticker = asset.get("ticker")
    title = (bill.get("title") or "").lower()
    short = (bill.get("short_title") or "").lower()
    if ticker and (ticker.lower() in title or ticker.lower() in short):
        score += 10.0

    # Discount for family holdings
    owner = asset.get("owner", "self")
    if owner == "spouse":
        score *= 0.6
    elif owner == "dependent":
        score *= 0.4

    return min(score, 100.0)
