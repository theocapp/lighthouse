"""
OpenFEC API collector for campaign finance data.
Docs: https://api.open.fec.gov/developers/
Rate limit: ~250 req/day on free tier — we track daily usage in a counter file.
"""
import json
from datetime import date
from pathlib import Path
from typing import Generator, Optional

from .base import BaseCollector

BASE_URL = "https://api.open.fec.gov/v1"


class FecCollector(BaseCollector):

    def __init__(self, api_key: str, cache_dir: Path, rate: float = 0.003):
        super().__init__(rate=rate, cache_dir=cache_dir / "fec", cache_ttl_days=7)
        self.api_key = api_key
        self._counter_path = Path(cache_dir) / "fec" / "daily_counter.json"

    def _check_daily_limit(self, limit: int = 240):
        """Raise if we've exceeded the daily request budget."""
        today = date.today().isoformat()
        counter = {"date": today, "count": 0}
        if self._counter_path.exists():
            try:
                counter = json.loads(self._counter_path.read_text())
            except Exception:
                pass

        if counter.get("date") != today:
            counter = {"date": today, "count": 0}

        if counter["count"] >= limit:
            raise RuntimeError(
                f"FEC daily request limit ({limit}) reached for {today}. "
                "Try again tomorrow or upgrade your api.data.gov plan."
            )

        counter["count"] += 1
        self._counter_path.parent.mkdir(parents=True, exist_ok=True)
        self._counter_path.write_text(json.dumps(counter))

    def _get(self, endpoint: str, params: Optional[dict] = None) -> dict:
        self._check_daily_limit()
        p = {"api_key": self.api_key, "per_page": 100, **(params or {})}
        return self.fetch_json(
            f"{BASE_URL}/{endpoint.lstrip('/')}",
            params=p,
            bypass_cache=False,
        )

    def _paginate(self, endpoint: str, params: Optional[dict] = None) -> Generator[dict, None, None]:
        p = dict(params or {})
        p["page"] = 1
        while True:
            data = self._get(endpoint, params=p)
            results = data.get("results", [])
            if not results:
                break
            yield from results
            pagination = data.get("pagination", {})
            if p["page"] >= pagination.get("pages", 1):
                break
            p["page"] += 1

    def find_candidate(self, name: str, office: Optional[str] = None) -> list[dict]:
        """Search for a candidate by name. office: H (House), S (Senate), P (President)."""
        params = {"name": name}
        if office:
            params["office"] = office
        data = self._get("candidates/search", params)
        return data.get("results", [])

    def get_candidate_committees(self, candidate_id: str, cycle: int) -> list[dict]:
        """Get principal campaign committees for a candidate in an election cycle."""
        data = self._get(f"candidate/{candidate_id}/committees", params={"cycle": cycle})
        return data.get("results", [])

    def get_contributions_to_committee(
        self, committee_id: str, cycle: int
    ) -> Generator[dict, None, None]:
        """Individual contributions received by a committee in an election cycle."""
        yield from self._paginate(
            "schedules/schedule_a",
            params={"committee_id": committee_id, "two_year_transaction_period": cycle},
        )

    def get_pac_donations_to_committee(
        self, committee_id: str, cycle: int
    ) -> Generator[dict, None, None]:
        """PAC-to-candidate (Schedule B) donations to a committee."""
        yield from self._paginate(
            "schedules/schedule_b",
            params={"committee_id": committee_id, "two_year_transaction_period": cycle},
        )


def normalize_contribution(raw: dict, bioguide_id: str) -> dict:
    return {
        "bioguide_id": bioguide_id,
        "fec_committee_id": raw.get("committee_id"),
        "contributor_name": raw.get("contributor_name"),
        "contributor_employer": raw.get("contributor_employer"),
        "contributor_industry": raw.get("contributor_industry"),
        "amount": float(raw["contribution_receipt_amount"]) if raw.get("contribution_receipt_amount") else None,
        "contribution_date": (raw.get("contribution_receipt_date") or "")[:10] or None,
        "election_cycle": raw.get("two_year_transaction_period"),
        "contribution_type": "pac" if raw.get("entity_type") == "PAC" else "individual",
    }
