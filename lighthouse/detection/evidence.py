"""
Shared helpers for conservative evidence framing.
"""
from __future__ import annotations

from typing import Optional


EVIDENCE_TIER_ORDER = {
    "tier_1_exact_company": 1,
    "tier_2_narrow_industry": 2,
    "tier_3_committee_jurisdiction": 3,
    "tier_4_broad_sector": 4,
    "tier_5_weak_overlap": 5,
}


def classify_evidence_tier(evidence: dict, conflict_type: Optional[str] = None) -> str:
    if evidence.get("exact_company_match") or evidence.get("exact_ticker_match"):
        return "tier_1_exact_company"
    if evidence.get("narrow_industry_match") or evidence.get("direct_bill_company_mention"):
        return "tier_2_narrow_industry"
    if evidence.get("committee_jurisdiction_match") or conflict_type == "committee_donor":
        return "tier_3_committee_jurisdiction"
    if evidence.get("sector_match"):
        return "tier_4_broad_sector"
    return "tier_5_weak_overlap"


def confidence_from_evidence_tier(
    evidence_tier: str,
    source_quality: Optional[str] = None,
    *,
    has_exact_match: bool = False,
) -> str:
    high_quality = source_quality in {
        "public_disclosure_with_bill_and_vote_records",
        "public_disclosure_with_transaction_and_vote_records",
        "public_disclosure_with_bill_metadata",
        "public_fec_records_with_committee_metadata",
    }

    if has_exact_match or evidence_tier == "tier_1_exact_company":
        return "high"
    if evidence_tier == "tier_2_narrow_industry" and high_quality:
        return "high"
    if evidence_tier in {"tier_2_narrow_industry", "tier_3_committee_jurisdiction"}:
        return "medium"
    return "low"


def signal_strength_from_score(score: float) -> str:
    if score >= 65:
        return "strong"
    if score >= 35:
        return "moderate"
    return "weak"


def default_limitations(conflict_type: str, evidence: Optional[dict] = None) -> list[str]:
    evidence = evidence or {}
    limitations = [
        "Public data only; no evidence of intent.",
        "No legal conclusion is implied.",
    ]

    if evidence.get("sector_match") or conflict_type in {
        "vote_holding",
        "trade_timing_pre",
        "trade_timing_post",
        "sponsorship_holding",
        "family_holding",
    }:
        limitations.append("Sector match is approximate.")

    if conflict_type in {"vote_holding", "sponsorship_holding", "family_holding"}:
        limitations.append("Disclosure value ranges are broad.")
        limitations.append("A vote on a broad sector bill does not prove personal financial benefit.")

    if conflict_type in {"trade_timing_pre", "trade_timing_post"}:
        limitations.append("Trade timing alone does not establish motive or access to nonpublic information.")

    if conflict_type == "committee_donor":
        limitations.append("Campaign contribution timing and motive are not established.")

    owner = (evidence.get("owner") or "").lower()
    if owner in {"spouse", "dependent", "joint"} or conflict_type == "family_holding":
        limitations.append("The financial interest may be indirect or family-held.")

    return list(dict.fromkeys(limitations))


def build_evidence_context(
    evidence: dict,
    *,
    conflict_type: str,
    score: Optional[float] = None,
    confidence: Optional[str] = None,
) -> dict:
    detail = dict(evidence)
    evidence_tier = classify_evidence_tier(detail, conflict_type=conflict_type)
    source_quality = detail.get("source_quality") or "derived_from_public_disclosure_and_bill_metadata"
    detail["evidence_tier"] = evidence_tier
    detail["source_quality"] = source_quality
    detail["match_reason"] = detail.get("match_reason") or _default_match_reason(conflict_type, detail, evidence_tier)
    detail["limitations"] = detail.get("limitations") or default_limitations(conflict_type, detail)

    if score is not None:
        detail["signal_score"] = round(score, 2)
        detail["signal_strength"] = signal_strength_from_score(score)
    if confidence is not None:
        detail["confidence"] = confidence
    return detail


def _default_match_reason(conflict_type: str, evidence: dict, evidence_tier: str) -> str:
    if evidence_tier == "tier_1_exact_company":
        return "Exact company or ticker match between the public policy record and the financial interest."
    if evidence_tier == "tier_2_narrow_industry":
        return "A narrow industry match links the policy record to the disclosed financial interest."
    if evidence_tier == "tier_3_committee_jurisdiction":
        return "Committee jurisdiction overlaps with the contributor or industry connected to the signal."
    if evidence_tier == "tier_4_broad_sector":
        return "Bill policy area or vote context maps to the same broad sector as the asset or trade."
    if conflict_type == "committee_donor":
        return "Keyword-level overlap connects the contributor industry to committee jurisdiction."
    return "Weak topical overlap or inferred sector relationship from public records."
