#!/usr/bin/env python3
"""
CLI: Run the data ingestion pipeline.

Usage:
    python scripts/ingest.py
    python scripts/ingest.py --source members
    python scripts/ingest.py --source bills --source votes
    python scripts/ingest.py --refresh      # incremental update only
"""
import logging
import sys
from pathlib import Path

import click

sys.path.insert(0, str(Path(__file__).parent.parent))

from lighthouse.config import load_config
from lighthouse.db.models import get_session, init_db
from lighthouse.pipeline.ingest import IngestPipeline
from lighthouse.pipeline.refresh import run_refresh

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")


@click.command()
@click.option("--source", "-s", multiple=True, help="Source(s) to ingest. Default: all.")
@click.option("--refresh", is_flag=True, help="Incremental update (skip fresh sources).")
@click.option("--config", "config_path", default=None, help="Path to config.yml.")
def cli(source, refresh, config_path):
    cfg = load_config(config_path)
    engine = init_db(cfg.database.url)
    session = get_session(cfg.database.url)

    sources = list(source) if source else None

    try:
        if refresh:
            run_refresh(session, cfg, sources)
        else:
            pipeline = IngestPipeline(session, cfg)
            pipeline.run(sources)
        click.echo("Ingestion complete.")
    except Exception as exc:
        click.echo(f"Error: {exc}", err=True)
        sys.exit(1)
    finally:
        session.close()


if __name__ == "__main__":
    cli()
