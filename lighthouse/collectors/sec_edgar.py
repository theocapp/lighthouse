"""
SEC EDGAR Form 4 collector for insider trading data.
No authentication required. Rate limit: 10 req/sec.
Docs: https://www.sec.gov/search-filings/edgar-application-programming-interfaces
"""
from pathlib import Path
from typing import Generator, Optional

from .base import BaseCollector

BASE = "https://data.sec.gov"
SEARCH_URL = "https://efts.sec.gov/LATEST/search-index"


class SecEdgarCollector(BaseCollector):

    def __init__(self, cache_dir: Path, rate: float = 8.0):
        super().__init__(rate=rate, cache_dir=cache_dir / "sec_edgar", cache_ttl_days=7)
        self._session.headers["User-Agent"] = (
            "Lighthouse Congressional Ethics Signal Explorer contact@lighthouse.dev"
        )

    def search_company_cik(self, name: str) -> Optional[str]:
        """Find the SEC CIK for a company name. Returns first match."""
        url = f"{BASE}/submissions/?"
        data = self.fetch_json(
            "https://efts.sec.gov/LATEST/search-index?q=%22{}%22&dateRange=custom&startdt=2020-01-01&forms=4".format(
                name.replace(" ", "+")
            ),
            cache_key=f"sec:cik:{name}",
        )
        hits = data.get("hits", {}).get("hits", [])
        if hits:
            return hits[0].get("_source", {}).get("file_num")
        return None

    def get_submissions(self, cik: str) -> dict:
        """Fetch all filing submissions for a CIK."""
        padded = cik.zfill(10)
        return self.fetch_json(
            f"{BASE}/submissions/CIK{padded}.json",
            cache_key=f"sec:submissions:{cik}",
        )

    def get_form4_filings(self, cik: str) -> list[dict]:
        """Return a list of Form 4 filing metadata for a given CIK."""
        submissions = self.get_submissions(cik)
        recent = submissions.get("filings", {}).get("recent", {})
        forms = recent.get("form", [])
        dates = recent.get("filingDate", [])
        accessions = recent.get("accessionNumber", [])
        primary_docs = recent.get("primaryDocument", [])

        results = []
        for i, form in enumerate(forms):
            if form in ("4", "4/A"):
                results.append({
                    "form": form,
                    "filing_date": dates[i] if i < len(dates) else None,
                    "accession": accessions[i] if i < len(accessions) else None,
                    "primary_doc": primary_docs[i] if i < len(primary_docs) else None,
                    "cik": cik,
                })
        return results

    def get_insider_transactions(self, cik: str, accession: str) -> list[dict]:
        """Parse a Form 4 filing and return normalized insider transaction records."""
        accession_fmt = accession.replace("-", "")
        padded_cik = cik.zfill(10)
        url = f"{BASE}/Archives/edgar/data/{padded_cik}/{accession_fmt}/{accession}.txt"
        try:
            text = self.fetch_text(url, cache_key=f"sec:form4:{accession}")
            return _parse_form4_text(text, cik, accession)
        except Exception:
            return []

    def search_member_form4(self, first: str, last: str) -> Generator[dict, None, None]:
        """
        Search EDGAR full-text search for Form 4 filings mentioning a member's name.
        Used to find cases where a member is the reporting person (rare but valid).
        """
        query = f"{first}+{last}"
        url = f"https://efts.sec.gov/LATEST/search-index?q=%22{query}%22&forms=4&dateRange=custom&startdt=2012-01-01"
        try:
            data = self.fetch_json(url, cache_key=f"sec:member_form4:{first}_{last}")
            hits = data.get("hits", {}).get("hits", [])
            for h in hits:
                yield h.get("_source", {})
        except Exception:
            return


def _parse_form4_text(text: str, cik: str, accession: str) -> list[dict]:
    """Minimal XML parser for Form 4 insider transaction records."""
    import xml.etree.ElementTree as ET

    try:
        # Form 4 filings embed XML; find the XML block
        start = text.find("<?xml")
        if start == -1:
            start = text.find("<ownershipDocument")
        if start == -1:
            return []
        xml_text = text[start:]
        end = xml_text.find("</ownershipDocument>")
        if end != -1:
            xml_text = xml_text[: end + len("</ownershipDocument>")]

        root = ET.fromstring(xml_text)
    except ET.ParseError:
        return []

    results = []
    for txn in root.findall(".//nonDerivativeTransaction"):
        sec_title = _xml_text(txn, "securityTitle/value")
        txn_date = _xml_text(txn, "transactionDate/value")
        code = _xml_text(txn, "transactionCoding/transactionCode")
        shares = _xml_text(txn, "transactionAmounts/transactionShares/value")
        price = _xml_text(txn, "transactionAmounts/transactionPricePerShare/value")

        results.append({
            "cik": cik,
            "accession": accession,
            "security_title": sec_title,
            "transaction_date": txn_date,
            "transaction_code": code,  # P=purchase, S=sale, etc.
            "shares": float(shares) if shares else None,
            "price_per_share": float(price) if price else None,
        })

    return results


def _xml_text(element, path: str) -> Optional[str]:
    node = element.find(path)
    return node.text.strip() if node is not None and node.text else None
