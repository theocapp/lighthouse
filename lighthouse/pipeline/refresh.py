"""
Incremental update pipeline — re-runs ingestion only for data that has changed
since the last successful run, using timestamps stored in ingestion_log.
"""
import logging
from datetime import datetime, timedelta
from typing import Optional

from sqlalchemy.orm import Session

from ..config import Config
from ..db.models import IngestionLog
from .ingest import IngestPipeline

log = logging.getLogger(__name__)


def run_refresh(session: Session, config: Config, sources: Optional[list[str]] = None):
    """
    Run incremental update. Checks ingestion_log for last successful run per source
    and passes from_date to collectors that support it.
    """
    pipeline = IngestPipeline(session, config)

    all_sources = ["members", "committees", "bills", "votes", "stocks", "disclosures", "fec"]
    selected = sources or all_sources

    for source in all_sources:
        if source not in selected:
            continue

        last_run = _get_last_run(session, source)
        if last_run:
            age = datetime.utcnow() - last_run
            log.info("Source '%s' last run %s ago", source, age)

            # Skip sources that are fresh (configurable thresholds)
            thresholds = {
                "members": timedelta(days=1),
                "committees": timedelta(days=7),
                "bills": timedelta(hours=6),
                "votes": timedelta(hours=1),
                "stocks": timedelta(hours=1),
                "disclosures": timedelta(days=7),
                "fec": timedelta(days=7),
            }
            if age < thresholds.get(source, timedelta(hours=1)):
                log.info("Skipping '%s' — data is fresh", source)
                continue

        pipeline.run(sources=[source])


def _get_last_run(session: Session, source: str) -> Optional[datetime]:
    obj = session.get(IngestionLog, source)
    if obj and obj.status == "ok" and obj.last_run:
        return obj.last_run
    return None
