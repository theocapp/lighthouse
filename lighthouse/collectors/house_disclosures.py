"""
House Financial Disclosure scraper.
Source: https://disclosures-clerk.house.gov/FinancialDisclosure
Uses the current tokenized member-search flow and returns both annual
financial disclosure filings and periodic transaction reports.
"""
import re
from pathlib import Path
from typing import Generator, Optional

from bs4 import BeautifulSoup

from .base import BaseCollector

BASE_URL = "https://disclosures-clerk.house.gov"
SEARCH_URL = f"{BASE_URL}/FinancialDisclosure/ViewSearch"
MEMBER_RESULT_URL = f"{BASE_URL}/FinancialDisclosure/ViewMemberSearchResult"
LISTING_URL = f"{BASE_URL}/public_disc/financial-pdfs/{{year}}/{{year}}FD.zip"
PROJECT_ROOT = Path(__file__).resolve().parents[2]


class HouseDisclosuresCollector(BaseCollector):

    def __init__(self, cache_dir: Path, rate: float = 0.5):
        super().__init__(rate=rate, cache_dir=cache_dir / "house_disclosures", cache_ttl_days=7)
        self.disc_dir = Path(cache_dir) / "disclosures" / "house"

    def search_member(
        self,
        last_name: str,
        year: int,
        state: str = "",
        district: Optional[int | str] = None,
    ) -> list[dict]:
        """Search for House filings for a member by last name and office."""
        token = self._get_verification_token()
        payload = {
            "LastName": last_name,
            "FilingYear": str(year),
            "State": state or "",
            "District": "" if district in (None, "", 0) else str(district),
            "__RequestVerificationToken": token,
        }
        html = self._post_search_results(
            payload,
            cache_key=f"house_disc:{last_name}:{year}:{payload['State']}:{payload['District']}",
        )
        return _parse_search_results(html)

    def download_filing(self, filing: dict) -> Path:
        """Download a House annual filing or PTR PDF."""
        source_url = filing.get("source_url") or ""
        if not source_url:
            raise ValueError("House filing is missing source_url")

        year = filing.get("year") or "unknown"
        doc_id = filing.get("doc_id") or "unknown"
        document_type = filing.get("document_type") or "unknown"
        dest = self.disc_dir / str(year) / document_type / f"{doc_id}.pdf"

        shared_cache = PROJECT_ROOT / "cache" / "disclosures" / "house" / str(year) / document_type / f"{doc_id}.pdf"
        if shared_cache.exists():
            dest.parent.mkdir(parents=True, exist_ok=True)
            if not dest.exists():
                dest.write_bytes(shared_cache.read_bytes())
            return dest

        self.fetch_raw(source_url, dest_path=dest, skip_if_exists=True)
        return dest

    def get_all_filings_for_year(self, year: int) -> Generator[dict, None, None]:
        """
        Enumerate all House filings for a given year by posting an empty
        member-search form for that filing year.
        """
        token = self._get_verification_token()
        html = self._post_search_results(
            {
                "LastName": "",
                "FilingYear": str(year),
                "State": "",
                "District": "",
                "__RequestVerificationToken": token,
            },
            cache_key=f"house_disc:all:{year}",
        )
        yield from _parse_search_results(html)

    def get_cached_filings_for_year(self, year: int) -> list[dict]:
        """Load House filing rows from cached search-result pages on disk."""
        results: list[dict] = []
        seen_doc_ids: set[str] = set()

        for path in self.cache_dir.glob("*/*.json"):
            try:
                html = path.read_text(errors="replace")
                rows = _parse_search_results(html)
            except Exception:
                continue

            for row in rows:
                row_year = row.get("year")
                if row_year != year:
                    continue
                doc_id = row.get("doc_id") or ""
                if doc_id and doc_id in seen_doc_ids:
                    continue
                if doc_id:
                    seen_doc_ids.add(doc_id)
                results.append(row)

        return results

    def _get_verification_token(self) -> str:
        html = self.fetch_text(SEARCH_URL, cache_key="house_disc:view_search")
        soup = BeautifulSoup(html, "lxml")
        token = soup.find("input", {"name": "__RequestVerificationToken"})
        if not token or not token.get("value"):
            raise ValueError("House disclosures token not found")
        return token["value"]

    def _post_search_results(self, payload: dict, cache_key: str) -> str:
        path = self._cache_path(cache_key)
        if self._cache_is_fresh(path):
            return path.read_text(errors="replace")

        self.rate_limiter.wait()
        resp = self._session.post(
            MEMBER_RESULT_URL,
            data=payload,
            headers={"Referer": SEARCH_URL},
            timeout=30,
        )
        resp.raise_for_status()
        path.write_text(resp.text)
        return resp.text


def _parse_search_results(html: str) -> list[dict]:
    soup = BeautifulSoup(html, "lxml")
    results = []

    table = soup.find("table")
    if not table:
        return results

    rows = table.find_all("tr")[1:]
    for row in rows:
        cells = row.find_all("td")
        if len(cells) < 4:
            continue

        link = cells[0].find("a")
        href = link.get("href", "") if link else ""
        match = re.search(r"/(\d+)\.pdf", href)
        doc_id = match.group(1) if match else ""
        document_type = "ptr" if "/ptr-pdfs/" in href else "financial"
        year_text = cells[2].get_text(strip=True)

        results.append({
            "doc_id": doc_id,
            "source_url": href if href.startswith("http") else f"{BASE_URL}/{href.lstrip('/')}",
            "document_type": document_type,
            "name": cells[0].get_text(strip=True),
            "office": cells[1].get_text(strip=True),
            "year": int(year_text) if year_text.isdigit() else year_text,
            "filing_type": cells[3].get_text(strip=True),
            "filed_date": cells[4].get_text(strip=True) if len(cells) > 4 else None,
        })

    return results
