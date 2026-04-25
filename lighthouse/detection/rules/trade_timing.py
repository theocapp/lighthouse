"""
Rule: Stock traded within N days of related bill/vote activity.
Pre-vote trading (trade_timing_pre) is the most legally significant pattern.
Post-vote trading (trade_timing_post) is suspicious but less actionable.
"""
import json
from datetime import date, timedelta
from typing import Optional

from ..rules.vote_holding import ConflictCandidate
from ...detection.industry_map import asset_name_is_diversified, bill_sectors, ticker_to_sector


def detect(
    transactions: list[dict],
    votes: list[dict],
    bills: dict,
    window_days: int = 30,
) -> list[ConflictCandidate]:
    """
    transactions: list of {id, ticker, transaction_date, transaction_type, amount_max, sector, owner}
    votes: list of {vote_id, bill_id, vote_date}
    bills: dict of bill_id → bill record
    window_days: how many days around a vote to check for trades
    """
    results = []

    # Pre-index votes by date
    vote_index: list[tuple[date, dict]] = []
    for v in votes:
        raw = v.get("vote_date")
        if not raw:
            continue
        try:
            vdate = date.fromisoformat(str(raw)[:10])
            vote_index.append((vdate, v))
        except ValueError:
            continue

    for txn in transactions:
        raw_date = txn.get("transaction_date")
        if not raw_date:
            continue
        try:
            txn_date = date.fromisoformat(str(raw_date)[:10])
        except ValueError:
            continue

        ticker = (txn.get("ticker") or "").upper()
        txn_sector = txn.get("sector") or (ticker_to_sector(ticker) if ticker else None)

        if not txn_sector or txn_sector in ("unknown", "diversified"):
            continue
        if asset_name_is_diversified(txn.get("asset_name") or ""):
            continue

        for vdate, vote in vote_index:
            bill_id = vote.get("bill_id")
            if not bill_id:
                continue

            bill = bills.get(bill_id)
            if not bill:
                continue

            policy_area = bill.get("policy_area") or ""
            subjects = json.loads(bill.get("subjects_json") or "[]")
            vote_sectors = bill_sectors(policy_area, subjects)

            if txn_sector not in vote_sectors:
                continue

            gap_days = (vdate - txn_date).days

            if 0 < gap_days <= window_days:
                # Trade happened BEFORE the vote
                conflict_type = "trade_timing_pre"
                proximity = 1.0 - (gap_days / window_days)
            elif -window_days <= gap_days <= 0:
                # Trade happened AFTER the vote
                conflict_type = "trade_timing_post"
                proximity = 1.0 - (abs(gap_days) / window_days)
            else:
                continue

            raw_score = _compute_score(txn, proximity, conflict_type)

            results.append(ConflictCandidate(
                conflict_type=conflict_type,
                raw_score=raw_score,
                vote_id=vote.get("vote_id"),
                bill_id=bill_id,
                transaction_id=txn.get("id"),
                evidence={
                    "ticker": ticker,
                    "transaction_date": str(txn_date),
                    "vote_date": str(vdate),
                    "gap_days": gap_days,
                    "transaction_type": txn.get("transaction_type"),
                    "amount_max": txn.get("amount_max"),
                    "sector": txn_sector,
                    "owner": txn.get("owner"),
                },
            ))

    return results


def _compute_score(txn: dict, proximity: float, conflict_type: str) -> float:
    score = 40.0 + 40.0 * proximity  # base 40–80 based on timing

    # Larger trades are more suspicious
    amount = float(txn.get("amount_max") or 0)
    if amount >= 1_000_000:
        score += 15.0
    elif amount >= 100_000:
        score += 8.0
    elif amount >= 15_000:
        score += 3.0

    # Sales before votes are more suspicious than purchases
    txn_type = txn.get("transaction_type", "")
    if conflict_type == "trade_timing_pre" and "sale" in txn_type:
        score += 5.0

    # Family discount
    owner = txn.get("owner", "self")
    if owner == "spouse":
        score *= 0.6
    elif owner == "dependent":
        score *= 0.4

    return min(score, 100.0)
