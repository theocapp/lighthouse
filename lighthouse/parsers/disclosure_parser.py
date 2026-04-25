"""
Financial disclosure parser.
Handles both structured HTML tables (Senate online viewer)
and PDF text extraction (older House filings via pdfplumber).
"""
import re
from pathlib import Path
from typing import Optional

from bs4 import BeautifulSoup


_ASSET_TYPE_MAP = {
    "stock": "stock",
    "common stock": "stock",
    "preferred stock": "stock",
    "bond": "bond",
    "treasury": "bond",
    "note": "bond",
    "mutual fund": "fund",
    "etf": "fund",
    "exchange traded": "fund",
    "partnership": "other",
    "real estate": "real_estate",
    "property": "real_estate",
    "trust": "other",
    "ira": "other",
    "retirement": "other",
    "cash": "other",
}

# STOCK Act dollar range bands → (min, max)
_VALUE_BANDS = [
    (r"\$1,001\s*[-–]\s*\$15,000", 1001, 15000),
    (r"\$15,001\s*[-–]\s*\$50,000", 15001, 50000),
    (r"\$50,001\s*[-–]\s*\$100,000", 50001, 100000),
    (r"\$100,001\s*[-–]\s*\$250,000", 100001, 250000),
    (r"\$250,001\s*[-–]\s*\$500,000", 250001, 500000),
    (r"\$500,001\s*[-–]\s*\$1,000,000", 500001, 1000000),
    (r"\$1,000,001\s*[-–]\s*\$5,000,000", 1000001, 5000000),
    (r"\$5,000,001\s*[-–]\s*\$25,000,000", 5000001, 25000000),
    (r"\$25,000,001\s*[-–]\s*\$50,000,000", 25000001, 50000000),
    (r"over\s+\$50,000,000", 50000001, None),
    (r"none", 0, 0),
]


def parse_value_range(text: str) -> tuple[Optional[float], Optional[float]]:
    clean = _clean_text(text).strip().lower()
    for pattern, lo, hi in _VALUE_BANDS:
        if re.search(pattern, clean, re.IGNORECASE):
            return float(lo), float(hi) if hi is not None else None
    # Fallback: extract any dollar amounts
    nums = re.findall(r"\$?([\d,]+)", clean)
    nums_int = []
    for n in nums:
        cleaned = n.replace(",", "")
        if cleaned.isdigit():
            nums_int.append(int(cleaned))
    if len(nums_int) >= 2:
        return float(nums_int[0]), float(nums_int[1])
    if len(nums_int) == 1:
        return float(nums_int[0]), None
    return None, None


def _classify_asset_type(name: str) -> str:
    lower = _clean_text(name).lower()
    for keyword, asset_type in _ASSET_TYPE_MAP.items():
        if keyword in lower:
            return asset_type
    return "other"


def _extract_ticker(name: str) -> Optional[str]:
    """Try to extract a stock ticker from an asset name string."""
    name = _clean_text(name)
    # Pattern: text (TICKER) or TICKER: text
    m = re.search(r"\(([A-Z]{1,5})\)", name)
    if m:
        return m.group(1)
    m = re.search(r"\b([A-Z]{2,5})\b(?:\s*:|\s*-|\s*Common)", name)
    if m:
        candidate = m.group(1)
        # Filter out common false positives
        if candidate not in {"LLC", "INC", "LTD", "NA", "US", "USA", "USD", "IRA", "ETF"}:
            return candidate
    return None


def _normalize_owner(text: str) -> str:
    lower = _clean_text(text).lower()
    if "spouse" in lower or "husband" in lower or "wife" in lower:
        return "spouse"
    if "joint" in lower:
        return "joint"
    if "dependent" in lower or "child" in lower:
        return "dependent"
    return "self"


def parse_html_disclosure(path: Path, bioguide_id: str, disclosure_id: int) -> list[dict]:
    """Parse an HTML financial disclosure page into a list of Asset dicts."""
    html = path.read_text(errors="replace")
    soup = BeautifulSoup(html, "lxml")

    assets = []
    table = _find_assets_table(soup)
    if not table:
        return assets

    headers = [th.get_text(strip=True).lower() for th in table.find_all("th")]

    def col(row, name_fragment: str) -> str:
        for i, h in enumerate(headers):
            if name_fragment in h:
                cells = row.find_all("td")
                return cells[i].get_text(strip=True) if i < len(cells) else ""
        return ""

    year = _extract_year_from_html(soup)

    for row in table.find_all("tr")[1:]:
        cells = row.find_all("td")
        if not cells:
            continue

        name = _clean_text(col(row, "asset") or col(row, "description") or cells[0].get_text(strip=True))
        if not name:
            continue

        value_text = _clean_text(col(row, "value") or col(row, "amount"))
        income_text = _clean_text(col(row, "income"))
        owner_text = _clean_text(col(row, "owner") or col(row, "sp/dc"))

        value_min, value_max = parse_value_range(value_text)
        income_min, income_max = parse_value_range(income_text)

        ticker = _extract_ticker(name)
        asset_type = _classify_asset_type(name)
        owner = _normalize_owner(owner_text)

        assets.append({
            "disclosure_id": disclosure_id,
            "bioguide_id": bioguide_id,
            "asset_name": name,
            "asset_type": asset_type,
            "ticker": ticker,
            "value_min": value_min,
            "value_max": value_max,
            "income_min": income_min,
            "income_max": income_max,
            "owner": owner,
            "year": year,
            "industry_code": None,
            "sector": None,
        })

    return assets


def parse_pdf_disclosure(path: Path, bioguide_id: str, disclosure_id: int) -> list[dict]:
    """Parse a PDF financial disclosure using pdfplumber (for older filings)."""
    try:
        import pdfplumber
    except ImportError:
        return []

    assets = []
    try:
        with pdfplumber.open(path) as pdf:
            full_text = "\n".join(
                page.extract_text() or "" for page in pdf.pages
            )
    except Exception:
        return []

    full_text = _clean_text(full_text)
    year = _extract_year_from_text(full_text)
    lines = full_text.split("\n")

    for line in lines:
        # Look for lines that contain dollar range patterns (a heuristic for asset rows)
        if not re.search(r"\$[\d,]+", line):
            continue

        name = _clean_text(re.sub(r"\$[\d,\s\-–]+.*$", "", line)).strip()
        if not name or len(name) < 3:
            continue

        value_text = _clean_text(line[len(name):])
        value_min, value_max = parse_value_range(value_text)

        if value_min is None:
            continue

        ticker = _extract_ticker(name)
        asset_type = _classify_asset_type(name)

        assets.append({
            "disclosure_id": disclosure_id,
            "bioguide_id": bioguide_id,
            "asset_name": name,
            "asset_type": asset_type,
            "ticker": ticker,
            "value_min": value_min,
            "value_max": value_max,
            "income_min": None,
            "income_max": None,
            "owner": "self",
            "year": year,
            "industry_code": None,
            "sector": None,
        })

    return assets


def _find_assets_table(soup: BeautifulSoup):
    for table in soup.find_all("table"):
        headers = [th.get_text(strip=True).lower() for th in table.find_all("th")]
        text = " ".join(headers)
        if any(kw in text for kw in ["asset", "description", "value", "income"]):
            return table
    return None


def _extract_year_from_html(soup: BeautifulSoup) -> Optional[int]:
    text = soup.get_text()
    m = re.search(r"\b(20\d{2})\b", text)
    return int(m.group(1)) if m else None


def _extract_year_from_text(text: str) -> Optional[int]:
    m = re.search(r"\b(20\d{2})\b", text)
    return int(m.group(1)) if m else None


def _clean_text(text: str) -> str:
    if not text:
        return ""
    text = text.replace("\x00", "")
    text = re.sub(r"[^\S\n]+", " ", text)
    return text.strip()
