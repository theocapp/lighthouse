"""
Senate Stock Watcher collector.
API: https://senatestockwatcher.com/api (returns JSON array)
"""
from datetime import date
from pathlib import Path
from typing import Optional

from .base import BaseCollector
from .house_stocks import (
    _normalize_owner,
    _normalize_type,
    _parse_amount_range,
    _row_hash,
    _resolve_bioguide,
)

API_URL = "https://senatestockwatcher.com/api"


class SenateStocksCollector(BaseCollector):

    def __init__(self, cache_dir: Path, rate: float = 1.0):
        super().__init__(rate=rate, cache_dir=cache_dir / "senate_stocks", cache_ttl_days=1)

    def get_all_transactions(self) -> list[dict]:
        data = self.fetch_json(API_URL, cache_key="senate_stocks:all")
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


def normalize_senate_transaction(raw: dict, bioguide_lookup: dict) -> Optional[dict]:
    senator_name = (raw.get("senator") or raw.get("name") or "").strip()
    bioguide_id = _resolve_bioguide(senator_name, bioguide_lookup)
    if not bioguide_id:
        return None

    txn_type_raw = (raw.get("type") or raw.get("transaction_type") or "").lower()
    amount_min, amount_max = _parse_amount_range(raw.get("amount") or "")

    return {
        "bioguide_id": bioguide_id,
        "transaction_date": (raw.get("transaction_date") or "")[:10] or None,
        "disclosure_date": (raw.get("disclosure_date") or "")[:10] or None,
        "ticker": (raw.get("ticker") or "").upper() or None,
        "asset_name": raw.get("asset_description") or raw.get("asset") or raw.get("ticker"),
        "transaction_type": _normalize_type(txn_type_raw),
        "amount_min": amount_min,
        "amount_max": amount_max,
        "owner": _normalize_owner(raw.get("owner", "")),
        "source": "senate_watcher",
        "comment": raw.get("comment"),
        "source_url": API_URL,
        "source_file": None,
        "source_key": str(raw.get("id") or raw.get("ptr_link") or _row_hash(raw)),
        "source_hash": _row_hash(raw),
        "sector": None,
        "industry_code": None,
    }
