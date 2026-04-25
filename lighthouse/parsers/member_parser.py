from typing import Optional


# Maps Congress.gov party codes to short codes
_PARTY_MAP = {
    "Democrat": "D",
    "Republican": "R",
    "Independent": "I",
    "Libertarian": "L",
    "Democratic": "D",
}


def parse_member(raw: dict) -> dict:
    """
    Normalize a Congress.gov API member record into a flat dict
    matching the members table schema.
    """
    terms = raw.get("terms", {}).get("item", [])
    if isinstance(terms, dict):
        terms = [terms]

    # Determine current chamber from most recent term
    chamber = "house"
    if terms:
        last = terms[-1]
        chamber_raw = last.get("chamber", "").lower()
        if "senate" in chamber_raw:
            chamber = "senate"
        elif "house" in chamber_raw:
            chamber = "house"

    party_raw = raw.get("partyName") or raw.get("party", "")
    party = _PARTY_MAP.get(party_raw, party_raw[:1] if party_raw else "")

    # District is only relevant for House members
    district: Optional[int] = None
    if chamber == "house":
        district_raw = raw.get("district")
        if district_raw is not None:
            try:
                district = int(district_raw)
            except (ValueError, TypeError):
                pass

    name = raw.get("name") or f"{raw.get('firstName', '')} {raw.get('lastName', '')}".strip()
    first = raw.get("firstName") or (name.split()[0] if name else "")
    last = raw.get("lastName") or (name.split()[-1] if name else "")

    # FEC candidate ID lives in identifiers section
    identifiers = raw.get("identifiers") or {}
    fec_id = identifiers.get("fecCandidateId") or raw.get("fecCandidateId")

    return {
        "bioguide_id": raw["bioguideId"],
        "full_name": name,
        "first_name": first,
        "last_name": last,
        "party": party,
        "state": raw.get("state") or raw.get("stateCode"),
        "district": district,
        "chamber": chamber,
        "is_active": True,
        "fec_candidate_id": fec_id,
    }


def parse_committee_membership(bioguide_id: str, committee: dict, congress: int) -> dict:
    return {
        "bioguide_id": bioguide_id,
        "committee_code": committee.get("systemCode") or committee.get("code", ""),
        "committee_name": committee.get("name", ""),
        "role": (committee.get("rank") or "member").lower(),
        "congress": congress,
        "start_date": None,
        "end_date": None,
    }
