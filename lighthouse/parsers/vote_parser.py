"""
Parses Congress.gov API v3 vote records into normalized Vote + MemberVote dicts.
"""
from typing import Optional


_POSITION_MAP = {
    "Yea": "Yea",
    "Nay": "Nay",
    "Yes": "Yea",
    "No": "Nay",
    "Not Voting": "Not Voting",
    "Present": "Present",
    "Abstain": "Not Voting",
    # Senate-specific
    "YEA": "Yea",
    "NAY": "Nay",
}


def _safe_date(raw: Optional[str]) -> Optional[str]:
    if not raw:
        return None
    return raw[:19].replace("T", " ") if "T" in raw else raw[:10]


def parse_vote(raw: dict, chamber: str) -> dict:
    """Normalize a Congress.gov API vote summary into a Vote dict."""
    vote_num = raw.get("rollNumber") or raw.get("voteNumber") or 0
    congress = raw.get("congress")
    session = raw.get("sessionNumber") or raw.get("session") or 1

    vote_id = f"{chamber[0]}{congress}s{session}-{str(vote_num).zfill(4)}"

    # Link to bill if present
    bill_ref = raw.get("bill") or {}
    bill_id = None
    if bill_ref:
        bt = (bill_ref.get("type") or "").lower()
        bn = bill_ref.get("number")
        bc = bill_ref.get("congress") or congress
        if bt and bn:
            bill_id = f"{bt}{bn}-{bc}"

    return {
        "vote_id": vote_id,
        "chamber": chamber,
        "congress": congress,
        "session": session,
        "vote_number": vote_num,
        "vote_date": _safe_date(raw.get("date")),
        "question": raw.get("question"),
        "result": raw.get("result"),
        "bill_id": bill_id,
        "category": raw.get("type") or raw.get("category"),
        "requires": raw.get("requires"),
        "source_url": raw.get("url"),
    }


def parse_member_votes(vote_detail: dict, vote_id: str) -> list[dict]:
    """
    Extract per-member vote positions from a detailed vote record.
    Returns list of MemberVote dicts.
    """
    results = []

    # Congress.gov vote detail nests positions under vote_positions or voteCounts
    positions = vote_detail.get("votePositions") or vote_detail.get("positions") or {}

    # Different API shapes: sometimes a dict of lists keyed by position label
    if isinstance(positions, dict):
        for position_label, members in positions.items():
            normalized = _POSITION_MAP.get(position_label, position_label)
            if not isinstance(members, list):
                members = [members] if members else []
            for m in members:
                bid = m.get("bioguideId") if isinstance(m, dict) else None
                if bid:
                    results.append({
                        "vote_id": vote_id,
                        "bioguide_id": bid,
                        "position": normalized,
                    })
    elif isinstance(positions, list):
        for entry in positions:
            bid = entry.get("bioguideId")
            pos = _POSITION_MAP.get(entry.get("votePosition", ""), entry.get("votePosition", ""))
            if bid:
                results.append({"vote_id": vote_id, "bioguide_id": bid, "position": pos})

    return results
