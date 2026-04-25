from pathlib import Path
from typing import Generator, Optional

import requests

from .base import BaseCollector


class HouseVoteCollector(BaseCollector):
    def __init__(self, cache_dir: Path, rate: float = 1.5):
        super().__init__(rate=rate, cache_dir=cache_dir / "house_votes", cache_ttl_days=1)
        self.votes_dir = Path(cache_dir) / "house_votes"

    def download_votes(self, year: int, max_consecutive_missing: int = 25) -> Generator[Path, None, None]:
        missing = 0
        vote_number = 1
        found_any = False

        while True:
            url = f"https://clerk.house.gov/evs/{year}/roll{vote_number:03d}.xml"
            dest = self.votes_dir / str(year) / f"roll{vote_number:03d}.xml"

            status = self._download_if_exists(url, dest)
            if status == "ok":
                found_any = True
                missing = 0
                yield dest
            else:
                missing += 1
                if found_any and missing >= max_consecutive_missing:
                    break
                if not found_any and missing >= 5:
                    break

            vote_number += 1

    def _download_if_exists(self, url: str, dest: Path) -> str:
        if dest.exists():
            return "ok"

        self.rate_limiter.wait()
        try:
            resp = self._session.get(url, timeout=30)
        except requests.RequestException:
            return "error"

        if resp.status_code == 404:
            return "missing"
        resp.raise_for_status()

        dest.parent.mkdir(parents=True, exist_ok=True)
        dest.write_bytes(resp.content)
        return "ok"
