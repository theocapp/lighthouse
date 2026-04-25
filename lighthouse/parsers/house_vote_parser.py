from datetime import datetime
from typing import Optional

import xmltodict


_POSITION_MAP = {
    "aye": "Yea",
    "yea": "Yea",
    "yes": "Yea",
    "no": "Nay",
    "nay": "Nay",
    "present": "Present",
    "not voting": "Not Voting",
}


def parse_house_vote_content(xml_content: str) -> Optional[dict]:
    try:
        raw = xmltodict.parse(xml_content)
    except Exception:
        return None

    root = raw.get("rollcall-vote") or {}
    meta = root.get("vote-metadata") or {}

    congress = _to_int(meta.get("congress"))
    session = _parse_session(meta.get("session"))
    vote_number = _to_int(meta.get("rollcall-num"))
    if congress is None or session is None or vote_number is None:
        return None

    vote_date = _parse_house_datetime(meta.get("action-date"), meta.get("action-time"))
    bill_id = _parse_house_bill_id(meta.get("legis-num"), congress)

    return {
        "vote_id": f"h{congress}s{session}-{str(vote_number).zfill(4)}",
        "chamber": "house",
        "congress": congress,
        "session": session,
        "vote_number": vote_number,
        "vote_date": vote_date,
        "question": meta.get("vote-question"),
        "result": meta.get("vote-result"),
        "bill_id": bill_id,
        "category": meta.get("vote-type"),
        "requires": None,
        "source_url": None,
    }


def parse_house_member_votes(xml_content: str, vote_id: str) -> list[dict]:
    try:
        raw = xmltodict.parse(xml_content)
    except Exception:
        return []

    root = raw.get("rollcall-vote") or {}
    vote_data = root.get("vote-data") or {}
    rows = vote_data.get("recorded-vote") or []
    if isinstance(rows, dict):
        rows = [rows]

    results = []
    for row in rows:
        legislator = row.get("legislator") or {}
        bioguide_id = legislator.get("@name-id")
        position_raw = (row.get("vote") or "").strip()
        if not bioguide_id or not position_raw:
            continue
        position = _POSITION_MAP.get(position_raw.lower(), position_raw)
        results.append({
            "vote_id": vote_id,
            "bioguide_id": bioguide_id,
            "position": position,
        })
    return results


def extract_house_vote_identity(xml_content: str) -> Optional[dict]:
    parsed = parse_house_vote_content(xml_content)
    if not parsed:
        return None
    return {
        "vote_id": parsed["vote_id"],
        "chamber": parsed["chamber"],
        "congress": parsed["congress"],
        "session": parsed["session"],
        "vote_number": parsed["vote_number"],
    }


def _parse_session(raw: str | None) -> Optional[int]:
    if not raw:
        return None
    raw = raw.strip().lower()
    if raw.startswith("1"):
        return 1
    if raw.startswith("2"):
        return 2
    return None


def _parse_house_datetime(date_raw: str | None, time_raw: str | None) -> Optional[str]:
    if not date_raw:
        return None
    if isinstance(time_raw, dict):
        time_raw = time_raw.get("#text") or ""
    combined = f"{date_raw} {time_raw or ''}".strip()
    for fmt in ("%d-%b-%Y %I:%M %p", "%d-%b-%Y"):
        try:
            return datetime.strptime(combined, fmt).strftime("%Y-%m-%d %H:%M:%S")
        except ValueError:
            continue
    return None


def _parse_house_bill_id(raw: str | None, congress: int) -> Optional[str]:
    if not raw:
        return None
    token = raw.strip().upper().replace(".", "").replace(" ", "")
    for prefix in ("HR", "HRES", "HJRES", "HCONRES", "S", "SRES", "SJRES", "SCONRES"):
        if token.startswith(prefix):
            number = token[len(prefix):]
            if number.isdigit():
                return f"{prefix.lower()}{number}-{congress}"
    return None


def _to_int(value) -> Optional[int]:
    try:
        return int(str(value))
    except (TypeError, ValueError):
        return None
