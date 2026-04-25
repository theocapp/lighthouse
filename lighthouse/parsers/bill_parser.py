"""
Parses GovInfo BILLSTATUS XML into normalized Bill dicts.
Schema reference: https://github.com/usgpo/bill-status/blob/main/BILLSTATUS-XML_User_User-Guide.md
"""
import json
from pathlib import Path
from typing import Optional

import xmltodict


def _safe_date(raw: Optional[str]) -> Optional[str]:
    if not raw:
        return None
    return raw[:10]  # ISO date prefix


def parse_billstatus_xml(path: Path) -> Optional[dict]:
    """Parse a BILLSTATUS XML file into a normalized bill dict."""
    try:
        with open(path, "rb") as f:
            raw = xmltodict.parse(f)
    except Exception:
        return None

    return _parse_billstatus_record(raw)


def parse_billstatus_content(xml_content: str) -> Optional[dict]:
    try:
        raw = xmltodict.parse(xml_content)
    except Exception:
        return None
    return _parse_billstatus_record(raw)


def parse_cosponsors_from_xml(path: Path) -> list[dict]:
    """Extract cosponsor bioguide IDs and dates from a BILLSTATUS XML file."""
    try:
        with open(path, "rb") as f:
            raw = xmltodict.parse(f)
    except Exception:
        return []

    return _parse_cosponsors(raw)


def parse_cosponsors_from_content(xml_content: str) -> list[dict]:
    try:
        raw = xmltodict.parse(xml_content)
    except Exception:
        return []
    return _parse_cosponsors(raw)


def extract_billstatus_identity(xml_content: str) -> Optional[dict]:
    try:
        raw = xmltodict.parse(xml_content)
    except Exception:
        return None

    bill = raw.get("billStatus", {}).get("bill", {})
    bill_type = (bill.get("type") or "").lower()
    bill_number = bill.get("number")
    congress = bill.get("congress")
    if not (bill_type and bill_number and congress):
        return None

    return {
        "bill_id": f"{bill_type}{bill_number}-{congress}",
        "bill_type": bill_type,
        "bill_number": int(bill_number) if str(bill_number).isdigit() else None,
        "congress": int(congress) if str(congress).isdigit() else None,
    }


def parse_congress_bill_summary(raw: dict, sponsor_bioguide: Optional[str] = None) -> Optional[dict]:
    bill_type = (raw.get("type") or "").lower()
    bill_number = raw.get("number")
    congress = raw.get("congress")
    if not (bill_type and bill_number and congress):
        return None

    bill_id = f"{bill_type}{bill_number}-{congress}"
    latest_action = raw.get("latestAction") or {}
    policy_area = (raw.get("policyArea") or {}).get("name")

    return {
        "bill_id": bill_id,
        "bill_number": int(bill_number) if str(bill_number).isdigit() else None,
        "bill_type": bill_type,
        "congress": int(congress) if str(congress).isdigit() else None,
        "title": raw.get("title") or "",
        "short_title": None,
        "introduced_date": _safe_date(raw.get("introducedDate")),
        "status": latest_action.get("text", ""),
        "policy_area": policy_area,
        "sponsor_bioguide": sponsor_bioguide,
        "subjects_json": json.dumps([]),
        "industries_json": json.dumps([]),
        "govinfo_url": raw.get("url"),
    }


def _parse_billstatus_record(raw: dict) -> Optional[dict]:
    bill = raw.get("billStatus", {}).get("bill", {})
    if not bill:
        return None

    bill_type = (bill.get("type") or "").lower()
    bill_number = bill.get("number")
    congress = bill.get("congress")

    if not (bill_type and bill_number and congress):
        return None

    bill_id = f"{bill_type}{bill_number}-{congress}"

    sponsor_raw = bill.get("sponsors", {}).get("item")
    if isinstance(sponsor_raw, list):
        sponsor_raw = sponsor_raw[0]
    sponsor_bioguide = (sponsor_raw or {}).get("bioguideId")

    subjects_container = bill.get("subjects", {}) or {}
    leg_subjects = subjects_container.get("legislativeSubjects", {}) or {}
    subject_items = leg_subjects.get("item", [])
    if isinstance(subject_items, dict):
        subject_items = [subject_items]
    subjects = [s.get("name", "") for s in subject_items if s.get("name")]

    policy_area_raw = (bill.get("policyArea") or {}).get("name")

    titles = bill.get("titles", {}).get("item", [])
    if isinstance(titles, dict):
        titles = [titles]
    title = ""
    short_title = ""
    for t in titles:
        t_type = t.get("titleType", "")
        if "Official" in t_type and not title:
            title = t.get("title", "")
        if "Short" in t_type and not short_title:
            short_title = t.get("title", "")
    if not title:
        title = bill.get("title") or bill.get("titles", {}).get("item", [{}])[0].get("title", "")

    latest_action = bill.get("latestAction", {}) or {}
    status = latest_action.get("text", "")

    return {
        "bill_id": bill_id,
        "bill_number": int(bill_number) if str(bill_number).isdigit() else None,
        "bill_type": bill_type,
        "congress": int(congress) if str(congress).isdigit() else None,
        "title": title,
        "short_title": short_title or None,
        "introduced_date": _safe_date(bill.get("introducedDate")),
        "status": status,
        "policy_area": policy_area_raw,
        "sponsor_bioguide": sponsor_bioguide,
        "subjects_json": json.dumps(subjects),
        "industries_json": json.dumps([]),
        "govinfo_url": None,
    }


def _parse_cosponsors(raw: dict) -> list[dict]:
    bill = raw.get("billStatus", {}).get("bill", {})
    cosponsors_raw = (bill.get("cosponsors") or {}).get("item", [])
    if isinstance(cosponsors_raw, dict):
        cosponsors_raw = [cosponsors_raw]

    bill_type = (bill.get("type") or "").lower()
    bill_number = bill.get("number")
    congress = bill.get("congress")
    bill_id = f"{bill_type}{bill_number}-{congress}"

    results = []
    for c in cosponsors_raw:
        bid = c.get("bioguideId")
        if bid:
            results.append({
                "bill_id": bill_id,
                "bioguide_id": bid,
                "cosponsor_date": _safe_date(c.get("sponsorshipDate")),
            })
    return results
