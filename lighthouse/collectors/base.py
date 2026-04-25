import hashlib
import json
import time
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Optional
from urllib.parse import urlencode

import requests
from requests.adapters import HTTPAdapter
from tenacity import retry, stop_after_attempt, wait_exponential


class RateLimiter:
    """Token-bucket rate limiter (thread-safe for single-threaded use)."""

    def __init__(self, rate: float):
        self.rate = rate          # requests per second
        self._last_call = 0.0

    def wait(self):
        if self.rate <= 0:
            return
        min_gap = 1.0 / self.rate
        now = time.monotonic()
        elapsed = now - self._last_call
        if elapsed < min_gap:
            time.sleep(min_gap - elapsed)
        self._last_call = time.monotonic()


class BaseCollector:
    """
    Shared infrastructure for all data collectors:
    - HTTP session with retry logic
    - Token-bucket rate limiting
    - Disk-based response caching keyed by URL + params
    """

    USER_AGENT = "Lighthouse/0.1 (Congressional COI Tracker; contact@lighthouse.dev)"

    def __init__(self, rate: float, cache_dir: Path, cache_ttl_days: int = 1):
        self.rate_limiter = RateLimiter(rate)
        self.cache_dir = Path(cache_dir)
        self.cache_ttl = timedelta(days=cache_ttl_days)
        self._session = self._build_session()

    def _build_session(self) -> requests.Session:
        session = requests.Session()
        session.headers["User-Agent"] = self.USER_AGENT
        adapter = HTTPAdapter(max_retries=3)
        session.mount("https://", adapter)
        session.mount("http://", adapter)
        return session

    def _cache_path(self, cache_key: str) -> Path:
        digest = hashlib.sha256(cache_key.encode()).hexdigest()
        subdir = self.cache_dir / digest[:2]
        subdir.mkdir(parents=True, exist_ok=True)
        return subdir / f"{digest}.json"

    def _cache_is_fresh(self, path: Path) -> bool:
        if not path.exists():
            return False
        age = datetime.utcnow() - datetime.utcfromtimestamp(path.stat().st_mtime)
        return age < self.cache_ttl

    @retry(stop=stop_after_attempt(4), wait=wait_exponential(multiplier=1, min=2, max=30))
    def _fetch(self, url: str, params: Optional[dict] = None, headers: Optional[dict] = None) -> Any:
        self.rate_limiter.wait()
        resp = self._session.get(url, params=params, headers=headers, timeout=30)
        resp.raise_for_status()
        return resp

    def fetch_json(
        self,
        url: str,
        params: Optional[dict] = None,
        headers: Optional[dict] = None,
        cache_key: Optional[str] = None,
        bypass_cache: bool = False,
    ) -> Any:
        key = cache_key or (url + ("?" + urlencode(sorted((params or {}).items())) if params else ""))
        path = self._cache_path(key)

        if not bypass_cache and self._cache_is_fresh(path):
            with open(path) as f:
                return json.load(f)

        resp = self._fetch(url, params=params, headers=headers)
        data = resp.json()
        with open(path, "w") as f:
            json.dump(data, f)
        return data

    def fetch_raw(
        self,
        url: str,
        params: Optional[dict] = None,
        dest_path: Optional[Path] = None,
        skip_if_exists: bool = True,
    ) -> bytes:
        """Download raw bytes; optionally saves to dest_path (skips if already present)."""
        if dest_path and skip_if_exists and dest_path.exists():
            return dest_path.read_bytes()

        self.rate_limiter.wait()
        resp = self._fetch(url, params=params)
        content = resp.content

        if dest_path:
            dest_path.parent.mkdir(parents=True, exist_ok=True)
            dest_path.write_bytes(content)

        return content

    def fetch_text(
        self,
        url: str,
        params: Optional[dict] = None,
        headers: Optional[dict] = None,
        dest_path: Optional[Path] = None,
        skip_if_exists: bool = True,
        cache_key: Optional[str] = None,
        bypass_cache: bool = False,
    ) -> str:
        if dest_path:
            raw = self.fetch_raw(
                url,
                params=params,
                dest_path=dest_path,
                skip_if_exists=skip_if_exists,
            )
            return raw.decode("utf-8", errors="replace")
        key = cache_key or (url + ("?" + urlencode(sorted((params or {}).items())) if params else ""))
        path = self._cache_path(key)

        if not bypass_cache and self._cache_is_fresh(path):
            return path.read_text(errors="replace")

        resp = self._fetch(url, params=params, headers=headers)
        text = resp.text
        path.write_text(text)
        return text
