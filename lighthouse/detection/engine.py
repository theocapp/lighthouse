"""
Detection engine orchestrator.
Runs all rules for one or all members and persists scored public-data signals to the DB.
"""
import json
import logging
from datetime import datetime
from typing import Optional

from sqlalchemy.orm import Session

from ..db.models import Conflict, Member
from ..db import queries as q
from .rules import vote_holding, trade_timing, sponsorship, committee_donor, family_holding
from .rules.vote_holding import ConflictCandidate
from .scorer import score_candidates

log = logging.getLogger(__name__)


def run(
    session: Session,
    bioguide_id: Optional[str] = None,
    congress: int = 119,
    rule_weights: Optional[dict] = None,
    trade_window_days: int = 30,
    min_holding_value: float = 1000.0,
    family_discount: float = 0.6,
) -> dict[str, int]:
    """
    Run all signal detection rules.

    Args:
        session: Active SQLAlchemy session
        bioguide_id: If given, run only for this member. Otherwise runs for all.
        congress: Congressional session to analyze
        rule_weights: Override default rule weights
        trade_window_days: Days around vote to check for trades
        min_holding_value: Minimum asset value to consider
        family_discount: Score multiplier for family-held assets

    Returns:
        dict with 'members_processed' and 'conflicts_found' counts
    """
    members = q.get_members(session, bioguide_id=bioguide_id)
    stats = {"members_processed": 0, "conflicts_found": 0}

    for member in members:
        bid = member.bioguide_id
        log.info("Running detection for %s (%s)", member.full_name, bid)

        try:
            conflicts = _detect_for_member(
                session=session,
                bioguide_id=bid,
                congress=congress,
                rule_weights=rule_weights,
                trade_window_days=trade_window_days,
                min_holding_value=min_holding_value,
                family_discount=family_discount,
            )

            _persist_conflicts(session, bid, conflicts)
            stats["members_processed"] += 1
            stats["conflicts_found"] += len(conflicts)

        except Exception as exc:
            log.error("Detection failed for %s: %s", bid, exc, exc_info=True)

    session.commit()
    return stats


def _detect_for_member(
    session: Session,
    bioguide_id: str,
    congress: int,
    rule_weights: Optional[dict],
    trade_window_days: int,
    min_holding_value: float,
    family_discount: float,
) -> list[dict]:
    # Fetch data for this member
    member_votes = q.get_member_votes_with_bills(session, bioguide_id)
    assets = q.get_member_assets(session, bioguide_id, min_value=min_holding_value)
    transactions = q.get_member_transactions(session, bioguide_id)
    committee_memberships = q.get_committee_memberships(session, bioguide_id, congress)
    contributions = q.get_contributions(session, bioguide_id)
    sponsored = q.get_sponsored_bills(session, bioguide_id)
    cosponsored = q.get_cosponsored_bills(session, bioguide_id)

    # Build bills lookup dict (avoid N+1)
    bill_ids = set()
    for v in member_votes:
        if v.get("bill_id"):
            bill_ids.add(v["bill_id"])
    for b in cosponsored:
        bill_ids.add(b.get("bill_id", ""))

    bills = q.get_bills_by_ids(session, list(bill_ids))
    all_votes_for_timing = q.get_all_votes_with_bills(session, congress)
    all_bills = q.get_bills_by_ids(session, [v.get("bill_id") for v in all_votes_for_timing if v.get("bill_id")])

    candidates: list[ConflictCandidate] = []

    candidates += vote_holding.detect(member_votes, assets, bills)
    candidates += trade_timing.detect(transactions, all_votes_for_timing, all_bills, trade_window_days)
    candidates += sponsorship.detect(sponsored, cosponsored, assets, bills)
    candidates += committee_donor.detect(committee_memberships, contributions)
    candidates += family_holding.detect(member_votes, assets, bills, family_discount)

    return score_candidates(candidates, rule_weights)


def _persist_conflicts(session: Session, bioguide_id: str, conflicts: list[dict]):
    # Clear existing (re-detection is idempotent)
    session.query(Conflict).filter(Conflict.bioguide_id == bioguide_id).delete()

    for c in conflicts:
        obj = Conflict(
            bioguide_id=bioguide_id,
            conflict_type=c["conflict_type"],
            score=c["score"],
            confidence=c["confidence"],
            vote_id=c.get("vote_id"),
            bill_id=c.get("bill_id"),
            asset_id=c.get("asset_id"),
            transaction_id=c.get("transaction_id"),
            contribution_id=c.get("contribution_id"),
            evidence_summary=c.get("evidence_summary"),
            detail_json=c.get("detail_json"),
            detected_at=datetime.utcnow(),
        )
        session.add(obj)
