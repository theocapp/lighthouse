"""
Senate Financial Disclosure scraper.
Source: https://efdsearch.senate.gov/search/
Scrapes annual and periodic financial disclosure reports for senators.
"""
import re
from pathlib import Path
from typing import Generator

from bs4 import BeautifulSoup

from .base import BaseCollector

SEARCH_URL = "https://efdsearch.senate.gov/search/"
AGREE_URL = "https://efdsearch.senate.gov/search/home/"
REPORT_BASE = "https://efdsearch.senate.gov"
PROJECT_ROOT = Path(__file__).resolve().parents[2]


class SenateDisclosuresCollector(BaseCollector):

    def __init__(self, cache_dir: Path, rate: float = 0.5):
        super().__init__(rate=rate, cache_dir=cache_dir / "senate_disclosures", cache_ttl_days=7)
        self.disc_dir = Path(cache_dir) / "disclosures" / "senate"
        self._agreed = False

    def _accept_terms(self):
        """POST to accept the ethics terms of service (required for search access)."""
        if self._agreed:
            return
        self._session.post(
            AGREE_URL,
            data={"prohibition_agreement": "1"},
            headers={"Referer": AGREE_URL},
        )
        self._agreed = True

    def search_member(self, first: str, last: str, filing_type: str = "Annual") -> list[dict]:
        """Search for annual disclosure reports for a senator."""
        self._accept_terms()
        params = {
            "last_name": last,
            "first_name": first,
            "filing_type": filing_type,
            "submitted_start_date": "01/01/2012",
            "submitted_end_date": "",
            "candidate_status": "senator",
        }
        html = self.fetch_text(
            SEARCH_URL,
            params=params,
            cache_key=f"senate_disc:{first}_{last}:{filing_type}",
        )
        return _parse_senate_results(html)

    def download_report(self, report_url: str, report_id: str) -> Path:
        """Download an HTML or PDF report."""
        dest = self.disc_dir / f"{report_id}.html"
        full_url = report_url if report_url.startswith("http") else REPORT_BASE + report_url

        shared_cache = PROJECT_ROOT / "cache" / "disclosures" / "senate" / f"{report_id}.html"
        if shared_cache.exists():
            dest.parent.mkdir(parents=True, exist_ok=True)
            if not dest.exists():
                dest.write_bytes(shared_cache.read_bytes())
            return dest

        self.fetch_raw(full_url, dest_path=dest, skip_if_exists=True)
        return dest


def _parse_senate_results(html: str) -> list[dict]:
    soup = BeautifulSoup(html, "lxml")
    results = []

    table = soup.find("table", {"id": "searchResultTable"}) or soup.find("table")
    if not table:
        return results

    rows = table.find_all("tr")[1:]
    for row in rows:
        cells = row.find_all("td")
        if len(cells) < 4:
            continue

        link = cells[3].find("a") or cells[0].find("a")
        href = link["href"] if link and link.get("href") else ""
        report_id = re.sub(r"[^a-zA-Z0-9_-]", "_", href.split("/")[-1] or "unknown")

        results.append({
            "report_id": report_id,
            "first_name": cells[0].get_text(strip=True),
            "last_name": cells[1].get_text(strip=True),
            "office": cells[2].get_text(strip=True),
            "report_type": cells[3].get_text(strip=True),
            "filed_date": cells[4].get_text(strip=True) if len(cells) > 4 else None,
            "report_url": href,
        })

    return results
