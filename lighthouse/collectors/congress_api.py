"""
Congress.gov API v3 collector.
Docs: https://api.congress.gov/
Rate limit: 5,000 req/hr → 1.38 req/sec max; we use 1.3 req/sec to be safe.
"""
from pathlib import Path
from typing import Generator, Optional

from .base import BaseCollector

BASE_URL = "https://api.congress.gov/v3"


class CongressApiCollector(BaseCollector):

    def __init__(self, api_key: str, cache_dir: Path, rate: float = 1.3):
        super().__init__(rate=rate, cache_dir=cache_dir / "congress_api")
        self.api_key = api_key

    def _get(self, endpoint: str, params: Optional[dict] = None, **kwargs) -> dict:
        p = {"api_key": self.api_key, "format": "json", **(params or {})}
        return self.fetch_json(f"{BASE_URL}/{endpoint.lstrip('/')}", params=p, **kwargs)

    def _paginate(self, endpoint: str, params: Optional[dict] = None) -> Generator[dict, None, None]:
        """Yield every item across all pages of a paginated endpoint."""
        offset = 0
        limit = 250
        p = {"limit": limit, "offset": offset, **(params or {})}

        while True:
            p["offset"] = offset
            data = self._get(endpoint, params=p, cache_key=f"{endpoint}:offset={offset}")
            pagination = data.get("pagination", {})

            # The response wraps items under a key that varies by endpoint
            items_key = next(
                (k for k in data if k not in {"pagination", "request"}), None
            )
            if not items_key:
                break

            items = data[items_key]
            if not isinstance(items, list):
                items = [items]

            yield from items

            total = pagination.get("count", 0)
            offset += limit
            if offset >= total or not items:
                break

    # --- Members ---

    def get_members(self, congress: int) -> Generator[dict, None, None]:
        yield from self._paginate(f"member/congress/{congress}")

    def get_member_detail(self, bioguide_id: str) -> dict:
        data = self._get(f"member/{bioguide_id}")
        return data.get("member", data)

    def get_member_committees(self, bioguide_id: str) -> list[dict]:
        try:
            data = self._get(f"member/{bioguide_id}/committee-assignments")
            items = data.get("committees", [])
            return items if isinstance(items, list) else [items]
        except Exception:
            return []

    def get_member_sponsored_legislation(self, bioguide_id: str) -> Generator[dict, None, None]:
        yield from self._paginate(f"member/{bioguide_id}/sponsored-legislation")

    def get_member_cosponsored_legislation(self, bioguide_id: str) -> Generator[dict, None, None]:
        yield from self._paginate(f"member/{bioguide_id}/cosponsored-legislation")

    # --- Bills ---

    def get_bills(
        self,
        congress: int,
        from_date: Optional[str] = None,
        bill_type: Optional[str] = None,
    ) -> Generator[dict, None, None]:
        endpoint = f"bill/{congress}"
        if bill_type:
            endpoint += f"/{bill_type}"
        params = {}
        if from_date:
            params["fromDateTime"] = f"{from_date}T00:00:00Z"
        yield from self._paginate(endpoint, params)

    def get_bill_detail(self, congress: int, bill_type: str, bill_number: int) -> dict:
        data = self._get(f"bill/{congress}/{bill_type}/{bill_number}")
        return data.get("bill", data)

    def get_bill_subjects(self, congress: int, bill_type: str, bill_number: int) -> list[str]:
        try:
            data = self._get(f"bill/{congress}/{bill_type}/{bill_number}/subjects")
            subjects = data.get("subjects", {})
            items = subjects.get("legislativeSubjects", [])
            return [s.get("name", "") for s in items if s.get("name")]
        except Exception:
            return []

    def get_bill_cosponsors(self, congress: int, bill_type: str, bill_number: int) -> list[dict]:
        try:
            data = self._get(f"bill/{congress}/{bill_type}/{bill_number}/cosponsors")
            items = data.get("cosponsors", [])
            return items if isinstance(items, list) else []
        except Exception:
            return []

    # --- Votes ---

    def get_house_votes(
        self, congress: int, session: int, from_date: Optional[str] = None
    ) -> Generator[dict, None, None]:
        params = {}
        if from_date:
            params["fromDateTime"] = f"{from_date}T00:00:00Z"
        yield from self._paginate(f"vote/house/{congress}/{session}", params)

    def get_senate_votes(
        self, congress: int, session: int, from_date: Optional[str] = None
    ) -> Generator[dict, None, None]:
        params = {}
        if from_date:
            params["fromDateTime"] = f"{from_date}T00:00:00Z"
        yield from self._paginate(f"vote/senate/{congress}/{session}", params)

    def get_vote_detail(self, chamber: str, congress: int, session: int, roll_call: int) -> dict:
        data = self._get(f"vote/{chamber}/{congress}/{session}/{roll_call}")
        return data.get("vote", data)

    # --- Committees ---

    def get_committees(self, congress: int, chamber: Optional[str] = None) -> Generator[dict, None, None]:
        endpoint = "committee"
        if congress:
            endpoint += f"/{congress}"
        if chamber:
            endpoint += f"/{chamber}"
        yield from self._paginate(endpoint)
