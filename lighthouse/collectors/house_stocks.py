"""
House Stock Watcher collector.
API: https://housestockwatcher.com/api
Returns all congressional stock transactions for House members.
"""
import hashlib
import json
from datetime import date
from pathlib import Path
from typing import Optional

from .base import BaseCollector

API_URL = "https://housestockwatcher.com/api"


class HouseStocksCollector(BaseCollector):

    def __init__(self, cache_dir: Path, rate: float = 1.0):
        super().__init__(rate=rate, cache_dir=cache_dir / "house_stocks", cache_ttl_days=1)

    def get_all_transactions(self) -> list[dict]:
        data = self.fetch_json(API_URL, cache_key="house_stocks:all")
        if isinstance(data, list):
            return data
        return data.get("data", [])

    def get_transactions_since(self, since: date) -> list[dict]:
        all_txns = self.get_all_transactions()
        results = []
        for t in all_txns:
            raw_date = t.get("transaction_date") or t.get("disclosure_date", "")
            if not raw_date:
                continue
            try:
                txn_date = date.fromisoformat(raw_date[:10])
                if txn_date >= since:
                    results.append(t)
            except ValueError:
                continue
        return results


def normalize_house_transaction(raw: dict, bioguide_lookup: dict) -> Optional[dict]:
    """
    Convert a House Stock Watcher record into a StockTransaction dict.
    bioguide_lookup: {normalized_name: bioguide_id}
    """
    rep_name = (raw.get("representative") or "").strip()
    bioguide_id = _resolve_bioguide(rep_name, bioguide_lookup)
    if not bioguide_id:
        return None

    txn_type_raw = (raw.get("type") or "").lower()
    txn_type = _normalize_type(txn_type_raw)

    amount_min, amount_max = _parse_amount_range(raw.get("amount") or "")

    return {
        "bioguide_id": bioguide_id,
        "transaction_date": raw.get("transaction_date", "")[:10] or None,
        "disclosure_date": raw.get("disclosure_date", "")[:10] or None,
        "ticker": (raw.get("ticker") or "").upper() or None,
        "asset_name": raw.get("asset_description") or raw.get("ticker"),
        "transaction_type": txn_type,
        "amount_min": amount_min,
        "amount_max": amount_max,
        "owner": _normalize_owner(raw.get("owner", "")),
        "source": "house_watcher",
        "comment": raw.get("comment"),
        "source_url": API_URL,
        "source_file": None,
        "source_key": str(raw.get("id") or raw.get("ptr_link") or _row_hash(raw)),
        "source_hash": _row_hash(raw),
        "sector": None,
        "industry_code": None,
    }


def _resolve_bioguide(name: str, lookup: dict) -> Optional[str]:
    key = name.lower().strip()
    return lookup.get(key)


def _normalize_type(raw: str) -> str:
    if "purchase" in raw or "buy" in raw:
        return "purchase"
    if "sale (partial)" in raw or "partial" in raw:
        return "sale_partial"
    if "sale" in raw or "sell" in raw:
        return "sale"
    return raw or "unknown"


def _normalize_owner(raw: str) -> str:
    lower = raw.lower()
    if "spouse" in lower:
        return "spouse"
    if "joint" in lower:
        return "joint"
    if "dependent" in lower or "child" in lower:
        return "dependent"
    return "self"


# Dollar range bands from STOCK Act disclosures
_AMOUNT_RANGES = [
    ("$1,001 - $15,000", 1001, 15000),
    ("$15,001 - $50,000", 15001, 50000),
    ("$50,001 - $100,000", 50001, 100000),
    ("$100,001 - $250,000", 100001, 250000),
    ("$250,001 - $500,000", 250001, 500000),
    ("$500,001 - $1,000,000", 500001, 1000000),
    ("$1,000,001 - $5,000,000", 1000001, 5000000),
    ("$5,000,001 - $25,000,000", 5000001, 25000000),
    ("$25,000,001 - $50,000,000", 25000001, 50000000),
    ("over $50,000,000", 50000001, None),
]


def _parse_amount_range(raw: str) -> tuple[Optional[float], Optional[float]]:
    clean = raw.strip().lower()
    for label, lo, hi in _AMOUNT_RANGES:
        if label.lower() in clean:
            return float(lo), float(hi) if hi else None
    # Try numeric extraction
    import re
    nums = re.findall(r"[\d,]+", clean.replace("$", ""))
    nums = [int(n.replace(",", "")) for n in nums if n.replace(",", "").isdigit()]
    if len(nums) == 2:
        return float(nums[0]), float(nums[1])
    if len(nums) == 1:
        return float(nums[0]), None
    return None, None


def _row_hash(raw: dict) -> str:
    return hashlib.sha256(json.dumps(raw, sort_keys=True, default=str).encode("utf-8")).hexdigest()
