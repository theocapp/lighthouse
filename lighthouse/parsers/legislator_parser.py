import csv
from pathlib import Path


_PARTY_MAP = {
    "Democrat": "D",
    "Democratic": "D",
    "Republican": "R",
    "Independent": "I",
    "Libertarian": "L",
}


def iter_legislator_rows(path: Path):
    with path.open(newline="", encoding="utf-8") as handle:
        yield from csv.DictReader(handle)


def parse_legislator_row(row: dict) -> dict:
    chamber = "senate" if row.get("type") == "sen" else "house"
    district = None
    if chamber == "house" and row.get("district"):
        try:
            district = int(row["district"])
        except (TypeError, ValueError):
            district = None

    party_raw = row.get("party") or ""
    party = _PARTY_MAP.get(party_raw, party_raw[:1].upper() if party_raw else "")

    fec_ids = _split_csv_ids(row.get("fec_ids"))

    return {
        "bioguide_id": row["bioguide_id"],
        "full_name": row.get("full_name") or "",
        "first_name": row.get("first_name") or "",
        "last_name": row.get("last_name") or "",
        "party": party,
        "state": row.get("state") or None,
        "district": district,
        "chamber": chamber,
        "is_active": True,
        "fec_candidate_id": pick_primary_fec_id(fec_ids, chamber),
    }


def parse_legislator_identifiers(row: dict, chamber: str) -> list[dict]:
    identifiers: list[dict] = []
    fec_ids = _split_csv_ids(row.get("fec_ids"))
    primary_fec = pick_primary_fec_id(fec_ids, chamber)

    for fec_id in fec_ids:
        identifiers.append({
            "identifier_type": "fec_candidate_id",
            "identifier_value": fec_id,
            "is_primary": fec_id == primary_fec,
            "source": "legislators_csv",
        })

    for identifier_type in (
        "lis_id",
        "thomas_id",
        "opensecrets_id",
        "govtrack_id",
        "votesmart_id",
        "cspan_id",
        "ballotpedia_id",
        "washington_post_id",
        "icpsr_id",
        "wikipedia_id",
    ):
        value = (row.get(identifier_type) or "").strip()
        if value:
            identifiers.append({
                "identifier_type": identifier_type,
                "identifier_value": value,
                "is_primary": True,
                "source": "legislators_csv",
            })

    return identifiers


def pick_primary_fec_id(fec_ids: list[str], chamber: str) -> str | None:
    if not fec_ids:
        return None

    preferred_prefix = "S" if chamber == "senate" else "H"
    for fec_id in fec_ids:
        if fec_id.upper().startswith(preferred_prefix):
            return fec_id
    return fec_ids[0]


def _split_csv_ids(raw: str | None) -> list[str]:
    if not raw:
        return []
    return [part.strip() for part in raw.split(",") if part.strip()]
