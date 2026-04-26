from typing import Optional


# Maps Congress.gov party codes to short codes
_PARTY_MAP = {
    "Democrat": "D",
    "Republican": "R",
    "Independent": "I",
    "Libertarian": "L",
    "Democratic": "D",
}

_STATE_NAME_TO_CODE = {
    "Alabama": "AL",
    "Alaska": "AK",
    "Arizona": "AZ",
    "Arkansas": "AR",
    "California": "CA",
    "Colorado": "CO",
    "Connecticut": "CT",
    "Delaware": "DE",
    "District of Columbia": "DC",
    "American Samoa": "AS",
    "Florida": "FL",
    "Georgia": "GA",
    "Guam": "GU",
    "Hawaii": "HI",
    "Idaho": "ID",
    "Illinois": "IL",
    "Indiana": "IN",
    "Iowa": "IA",
    "Kansas": "KS",
    "Kentucky": "KY",
    "Louisiana": "LA",
    "Maine": "ME",
    "Maryland": "MD",
    "Massachusetts": "MA",
    "Michigan": "MI",
    "Minnesota": "MN",
    "Mississippi": "MS",
    "Missouri": "MO",
    "Montana": "MT",
    "Nebraska": "NE",
    "Nevada": "NV",
    "New Hampshire": "NH",
    "New Jersey": "NJ",
    "New Mexico": "NM",
    "New York": "NY",
    "North Carolina": "NC",
    "North Dakota": "ND",
    "Northern Mariana Islands": "MP",
    "Ohio": "OH",
    "Oklahoma": "OK",
    "Oregon": "OR",
    "Pennsylvania": "PA",
    "Puerto Rico": "PR",
    "Rhode Island": "RI",
    "South Carolina": "SC",
    "South Dakota": "SD",
    "Tennessee": "TN",
    "Texas": "TX",
    "U.S. Virgin Islands": "VI",
    "Utah": "UT",
    "Vermont": "VT",
    "Virgin Islands": "VI",
    "Virginia": "VA",
    "Washington": "WA",
    "West Virginia": "WV",
    "Wisconsin": "WI",
    "Wyoming": "WY",
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
    first = raw.get("firstName")
    last = raw.get("lastName")
    if not first or not last:
        first, last = _split_member_name(name)

    # FEC candidate ID lives in identifiers section
    identifiers = raw.get("identifiers") or {}
    fec_id = identifiers.get("fecCandidateId") or raw.get("fecCandidateId")

    return {
        "bioguide_id": raw["bioguideId"],
        "full_name": name,
        "first_name": first,
        "last_name": last,
        "party": party,
        "state": _state_code(raw.get("stateCode") or raw.get("state")),
        "district": district,
        "chamber": chamber,
        "is_active": True,
        "fec_candidate_id": fec_id,
    }


def _split_member_name(name: str) -> tuple[str, str]:
    if not name:
        return "", ""
    if "," in name:
        last, first = name.split(",", 1)
        return first.strip(), last.strip()
    parts = name.split()
    if len(parts) == 1:
        return parts[0], ""
    return parts[0], parts[-1]


def _state_code(value: str | None) -> str | None:
    if not value:
        return value
    value = value.strip()
    if len(value) == 2:
        return value.upper()
    return _STATE_NAME_TO_CODE.get(value, value)


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
