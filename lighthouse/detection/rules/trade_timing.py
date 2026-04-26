"""
Rule: Stock traded within N days of related bill/vote activity.
Pre-vote trading is the strongest timing signal; post-vote trading is weaker.
"""
import json
from datetime import date

from ..rules.vote_holding import (
    ConflictCandidate,
    _asset_name_matches_bill,
    _is_narrow_industry_match,
)
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
            bill_text = " ".join(
                part for part in [bill.get("title"), bill.get("short_title")] if part
            ).lower()

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
            exact_ticker_match = bool(ticker and ticker.lower() in bill_text)
            exact_company_match = _asset_name_matches_bill(txn.get("asset_name"), bill_text)
            narrow_industry_match = not (exact_ticker_match or exact_company_match) and _is_narrow_industry_match(
                bill_text,
                policy_area,
                subjects,
                txn_sector,
            )

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
                    "bill_title": bill.get("short_title") or bill.get("title"),
                    "exact_ticker_match": exact_ticker_match,
                    "exact_company_match": exact_company_match,
                    "narrow_industry_match": narrow_industry_match,
                    "sector_match": True,
                    "source_quality": "public_disclosure_with_transaction_and_vote_records",
                    "bill_source_url": bill.get("govinfo_url"),
                    "vote_source_url": vote.get("vote_source_url"),
                    "transaction_source": txn.get("source"),
                },
            ))

    return results


def _compute_score(txn: dict, proximity: float, conflict_type: str) -> float:
    score = 24.0 + 28.0 * proximity

    # Larger disclosed trades raise signal strength.
    amount = float(txn.get("amount_max") or 0)
    if amount >= 1_000_000:
        score += 18.0
    elif amount >= 100_000:
        score += 10.0
    elif amount >= 15_000:
        score += 4.0

    # Sales before votes get a modest bump.
    txn_type = txn.get("transaction_type", "")
    if conflict_type == "trade_timing_pre" and "sale" in txn_type:
        score += 6.0

    owner = txn.get("owner", "self")
    if owner == "spouse":
        score *= 0.55
    elif owner == "dependent":
        score *= 0.4
    elif owner == "joint":
        score *= 0.75

    return min(score, 100.0)
