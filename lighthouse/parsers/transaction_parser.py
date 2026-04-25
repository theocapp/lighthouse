"""
Parsers for periodic transaction reports and related transaction records.
"""
import re
from pathlib import Path
from typing import Optional

from .disclosure_parser import _extract_ticker, parse_value_range

_PTR_ROW_RE = re.compile(
    r"^(?P<owner>[A-Z]{1,3})\s+"
    r"(?P<asset>.+?)\s+"
    r"(?P<txn_code>[PSXE])\s+"
    r"(?P<txn_date>\d{2}/\d{2}/\d{4})\s+"
    r"(?P<disc_date>\d{2}/\d{2}/\d{4})\s+"
    r"(?P<amount>.+)$"
)


def parse_house_ptr_pdf(path: Path, bioguide_id: str) -> list[dict]:
    """Parse a House periodic transaction report PDF into StockTransaction dicts."""
    try:
        import pdfplumber
    except ImportError:
        return []

    try:
        with pdfplumber.open(path) as pdf:
            text = "\n".join((page.extract_text() or "") for page in pdf.pages)
    except Exception:
        return []

    lines = [_clean_line(line) for line in text.splitlines()]
    lines = [line for line in lines if line]

    transactions: list[dict] = []
    idx = 0
    while idx < len(lines):
        line = lines[idx]
        match = _PTR_ROW_RE.match(line)
        if not match:
            idx += 1
            continue

        row_text = line
        if idx + 1 < len(lines) and (
            lines[idx + 1].startswith("[") or lines[idx + 1].startswith("$")
        ):
            row_text = f"{row_text} {lines[idx + 1]}"
            idx += 1
            match = _PTR_ROW_RE.match(row_text)
            if not match:
                idx += 1
                continue

        record = match.groupdict()
        asset_name = record["asset"].strip()
        amount_min, amount_max = parse_value_range(record["amount"])

        description = None
        if idx + 1 < len(lines) and "Description:" in lines[idx + 1]:
            description = lines[idx + 1].split("Description:", 1)[1].strip() or None
            idx += 1
        elif idx + 2 < len(lines) and "Description:" in lines[idx + 2]:
            description = lines[idx + 2].split("Description:", 1)[1].strip() or None
            idx += 2

        transactions.append({
            "bioguide_id": bioguide_id,
            "transaction_date": _mmddyyyy_to_iso(record["txn_date"]),
            "disclosure_date": _mmddyyyy_to_iso(record["disc_date"]),
            "ticker": _extract_ticker(asset_name),
            "asset_name": asset_name,
            "transaction_type": _normalize_transaction_type(record["txn_code"]),
            "amount_min": amount_min,
            "amount_max": amount_max,
            "owner": _normalize_owner(record["owner"]),
            "source": "disclosure",
            "comment": description,
            "sector": None,
            "industry_code": None,
        })
        idx += 1

    return transactions


def _clean_line(line: str) -> str:
    cleaned = "".join(ch for ch in line.replace("\x00", "") if ch == "\n" or ch == "\t" or ord(ch) >= 32)
    return re.sub(r"\s+", " ", cleaned).strip()


def _mmddyyyy_to_iso(raw: str) -> Optional[str]:
    match = re.match(r"(\d{2})/(\d{2})/(\d{4})", raw or "")
    if not match:
        return None
    month, day, year = match.groups()
    return f"{year}-{month}-{day}"


def _normalize_transaction_type(code: str) -> str:
    code = (code or "").upper()
    if code == "P":
        return "purchase"
    if code == "S":
        return "sale"
    if code == "E":
        return "exchange"
    if code == "X":
        return "sale_partial"
    return "unknown"


def _normalize_owner(code: str) -> str:
    code = (code or "").upper()
    if code.startswith("SP"):
        return "spouse"
    if code.startswith("JT"):
        return "joint"
    if code.startswith("DC"):
        return "dependent"
    return "self"
