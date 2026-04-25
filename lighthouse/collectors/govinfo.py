"""
GovInfo.gov bulk BILLSTATUS XML collector.
Reuses URL patterns from /Users/theo/congress/congress/bills_119/download_bills.py.
"""
from pathlib import Path
from typing import Generator

from .base import BaseCollector

BILL_TYPES = ["hr", "s", "hres", "sres", "hjres", "sjres", "hconres", "sconres"]
BASE_JSON = "https://www.govinfo.gov/bulkdata/json/BILLSTATUS/{congress}/{bill_type}"
BASE_DOWNLOAD = "https://www.govinfo.gov/bulkdata/BILLSTATUS/{congress}/{bill_type}"


class GovInfoCollector(BaseCollector):

    def __init__(self, cache_dir: Path, rate: float = 2.0):
        super().__init__(rate=rate, cache_dir=cache_dir / "govinfo", cache_ttl_days=1)
        self.bills_dir = Path(cache_dir) / "bills"

    def list_available(self, congress: int, bill_type: str, session: int = 1) -> list[dict]:
        """Return listing of available BILLSTATUS files for a given congress/session/type."""
        url = BASE_JSON.format(congress=congress, session=session, bill_type=bill_type)
        data = self.fetch_json(url, cache_key=f"govinfo:list:{congress}:{bill_type}")
        return data.get("files", [])

    def download_billstatus(
        self, congress: int, bill_type: str, session: int = 1
    ) -> Generator[Path, None, None]:
        """
        Download BILLSTATUS XML files for all bills of a type/congress/session.
        Skips files already on disk (os.path.exists guard from congress/ pattern).
        Yields the local Path of each downloaded file.
        """
        files = self.list_available(congress, bill_type, session)
        base_url = BASE_DOWNLOAD.format(congress=congress, session=session, bill_type=bill_type)

        for entry in files:
            filename = entry.get("fileName") or entry.get("name", "")
            if not filename.endswith(".xml"):
                continue

            dest = self.bills_dir / str(congress) / str(session) / bill_type / filename
            self.fetch_raw(f"{base_url}/{filename}", dest_path=dest, skip_if_exists=True)
            yield dest

    def download_all(self, congress: int, session: int = 1) -> Generator[Path, None, None]:
        """Download BILLSTATUS XML for all bill types."""
        for bill_type in BILL_TYPES:
            yield from self.download_billstatus(congress, bill_type, session)
