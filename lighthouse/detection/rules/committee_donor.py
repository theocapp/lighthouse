"""
Rule: Member received campaign donations from industries
that their committee assignments regulate.
This is a structural ethics signal, not proof of quid pro quo or improper influence.
"""
from ..rules.vote_holding import ConflictCandidate
from ...detection.industry_map import committee_sectors


def detect(
    committee_memberships: list[dict],
    contributions: list[dict],
) -> list[ConflictCandidate]:
    """
    committee_memberships: list of {committee_code, committee_name, role}
    contributions: list of {id, contributor_industry, amount, contribution_type, election_cycle}
    """
    results = []

    # Build set of regulated sectors across all committee assignments
    regulated_sectors: dict[str, list[dict]] = {}  # sector → [committee records]
    for membership in committee_memberships:
        code = membership.get("committee_code", "")
        sectors = committee_sectors(code)
        for sector in sectors:
            regulated_sectors.setdefault(sector, []).append(membership)

    if not regulated_sectors:
        return results

    for contrib in contributions:
        industry = (contrib.get("contributor_industry") or "").lower()
        if not industry:
            continue

        # Match contributor industry to regulated sectors (keyword-based)
        for sector, committees in regulated_sectors.items():
            if _industry_matches_sector(industry, sector):
                raw_score = _compute_score(contrib, committees)
                results.append(ConflictCandidate(
                    conflict_type="committee_donor",
                    raw_score=raw_score,
                    contribution_id=contrib.get("id"),
                    evidence={
                        "contributor_industry": contrib.get("contributor_industry"),
                        "amount": contrib.get("amount"),
                        "contribution_type": contrib.get("contribution_type"),
                        "election_cycle": contrib.get("election_cycle"),
                        "sector": sector,
                        "committees": [c.get("committee_name") for c in committees],
                        "committee_jurisdiction_match": True,
                        "sector_match": True,
                        "source_quality": "public_fec_records_with_committee_metadata",
                        "fec_committee_id": contrib.get("fec_committee_id"),
                    },
                ))
                break  # one conflict per contribution

    return results


_SECTOR_KEYWORDS: dict[str, list[str]] = {
    "financials": ["bank", "finance", "insurance", "investment", "securities", "hedge", "equity"],
    "energy": ["oil", "gas", "coal", "petroleum", "energy", "drilling", "pipeline"],
    "health_care": ["pharma", "health", "hospital", "biotech", "medical", "drug"],
    "information_technology": ["tech", "software", "computer", "cyber", "semiconductor"],
    "communication_services": ["telecom", "media", "broadcast", "cable", "internet"],
    "defense": ["defense", "military", "weapon", "aerospace", "contractor"],
    "real_estate": ["real estate", "housing", "mortgage", "reit"],
    "industrials": ["manufacturing", "airline", "aviation", "railroad", "shipping"],
    "consumer_staples": ["agriculture", "food", "farm", "tobacco", "beverage"],
    "consumer_discretionary": ["retail", "auto", "automotive", "gaming", "entertainment"],
    "materials": ["mining", "chemical", "steel", "timber", "material"],
    "utilities": ["utility", "electric", "water", "power grid"],
}


def _industry_matches_sector(industry: str, sector: str) -> bool:
    keywords = _SECTOR_KEYWORDS.get(sector, [])
    return any(kw in industry for kw in keywords)


def _compute_score(contrib: dict, committees: list[dict]) -> float:
    score = 18.0

    amount = float(contrib.get("amount") or 0)
    if amount >= 10000:
        score += 16.0
    elif amount >= 5000:
        score += 10.0
    elif amount >= 2000:
        score += 6.0

    if contrib.get("contribution_type") == "pac":
        score += 8.0

    roles = {c.get("role", "") for c in committees}
    if "chair" in roles or "ranking" in roles:
        score += 10.0

    return min(score, 100.0)
