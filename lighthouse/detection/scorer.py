"""
Aggregates raw conflict candidates into scored, deduplicated Conflict records.
"""
import json
from typing import Optional

from .evidence import (
    build_evidence_context,
    classify_evidence_tier,
    confidence_from_evidence_tier,
    signal_strength_from_score,
)
from .rules.vote_holding import ConflictCandidate

RULE_WEIGHTS: dict[str, float] = {
    "vote_holding": 0.85,
    "trade_timing_pre": 1.00,
    "trade_timing_post": 0.70,
    "sponsorship_holding": 0.90,
    "committee_donor": 0.65,
    "family_holding": 0.75,
}


def _dedup_key(c: ConflictCandidate) -> tuple:
    """Two conflicts are duplicates if they share the same type + primary evidence pair."""
    return (
        c.conflict_type,
        c.vote_id or "",
        c.bill_id or "",
        c.asset_id or 0,
        c.transaction_id or 0,
        c.contribution_id or 0,
    )


def score_candidates(
    candidates: list[ConflictCandidate],
    rule_weights: Optional[dict[str, float]] = None,
) -> list[dict]:
    """
    Apply rule weights, deduplicate overlapping candidates,
    and return a list of Conflict dicts ready for DB insertion.
    """
    weights = rule_weights or RULE_WEIGHTS

    # Deduplicate: keep highest raw_score per unique key
    seen: dict[tuple, ConflictCandidate] = {}
    for c in candidates:
        key = _dedup_key(c)
        if key not in seen or c.raw_score > seen[key].raw_score:
            seen[key] = c

    results = []
    for c in seen.values():
        weight = weights.get(c.conflict_type, 1.0)
        final_score = min(c.raw_score * weight, 100.0)
        evidence_tier = classify_evidence_tier(c.evidence, conflict_type=c.conflict_type)
        confidence = confidence_from_evidence_tier(
            evidence_tier,
            c.evidence.get("source_quality"),
            has_exact_match=bool(
                c.evidence.get("exact_company_match") or c.evidence.get("exact_ticker_match")
            ),
        )
        detail = build_evidence_context(
            c.evidence,
            conflict_type=c.conflict_type,
            score=final_score,
            confidence=confidence,
        )
        detail.update(
            {
                "bill_id": c.bill_id,
                "vote_id": c.vote_id,
                "asset_id": c.asset_id,
                "transaction_id": c.transaction_id,
                "contribution_id": c.contribution_id,
            }
        )

        results.append({
            "conflict_type": c.conflict_type,
            "score": round(final_score, 2),
            "confidence": confidence,
            "vote_id": c.vote_id,
            "bill_id": c.bill_id,
            "asset_id": c.asset_id,
            "transaction_id": c.transaction_id,
            "contribution_id": c.contribution_id,
            "evidence_summary": _summarize(c),
            "signal_strength": signal_strength_from_score(final_score),
            "detail_json": json.dumps(detail),
        })

    results.sort(key=lambda x: x["score"], reverse=True)
    return results


def _summarize(c: ConflictCandidate) -> str:
    ev = c.evidence
    if c.conflict_type == "vote_holding":
        return (
            f"Potential vote-holding signal: voted {ev.get('position')} on a {ev.get('sector')} policy matter "
            f"while holding {ev.get('asset_name')} (max ${ev.get('value_max'):,.0f})"
            if ev.get("value_max") else
            f"Potential vote-holding signal: voted {ev.get('position')} on a {ev.get('sector')} policy matter "
            f"while holding {ev.get('asset_name')}"
        )
    if c.conflict_type in ("trade_timing_pre", "trade_timing_post"):
        direction = "before" if "pre" in c.conflict_type else "after"
        return (
            f"Potential trade-timing signal: traded {ev.get('ticker')} {ev.get('transaction_type')} "
            f"{abs(ev.get('gap_days', 0))} days {direction} a related vote"
        )
    if c.conflict_type == "sponsorship_holding":
        return (
            f"Potential sponsorship-holding signal: "
            f"{'sponsored' if ev.get('role') == 'sponsor' else 'cosponsored'} a {ev.get('sector')} bill "
            f"while holding {ev.get('asset_name')}"
        )
    if c.conflict_type == "committee_donor":
        return (
            f"Potential committee-donor signal: received {ev.get('contribution_type', 'contribution')} of "
            f"${ev.get('amount', 0):,.2f} from {ev.get('contributor_industry')} "
            f"while serving on {', '.join(ev.get('committees', []))}"
        )
    if c.conflict_type == "family_holding":
        return (
            f"Potential family-holding signal: {ev.get('owner', 'family').title()} holds {ev.get('asset_name')} "
            f"in a sector touched by the vote"
        )
    return c.conflict_type
