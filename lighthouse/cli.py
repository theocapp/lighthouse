"""
Minimal Click CLI for ingestion, detection, and coverage inspection.
"""
from __future__ import annotations

import json

import click

from .config import load_config
from .db.models import get_session, init_db
from .detection import engine
from .db import queries as q
from .pipeline.ingest import IngestPipeline
from .pipeline.refresh import run_refresh


@click.group()
def main():
    """Lighthouse command-line interface."""


@main.command()
@click.option("--source", "-s", multiple=True, help="Source(s) to ingest. Default: all.")
@click.option("--refresh", is_flag=True, help="Incremental update (skip fresh sources).")
@click.option("--config", "config_path", default=None, help="Path to config.yml.")
def ingest(source, refresh, config_path):
    """Run ingestion."""
    cfg = load_config(config_path)
    init_db(cfg.database.url)
    session = get_session(cfg.database.url)
    try:
        sources = list(source) if source else None
        if refresh:
            run_refresh(session, cfg, sources)
        else:
            IngestPipeline(session, cfg).run(sources)
        click.echo("Ingestion complete.")
    finally:
        session.close()


@main.command()
@click.option("--member", "-m", default=None, help="Bioguide ID of a specific member.")
@click.option("--congress", default=None, type=int, help="Congress number (default: from config).")
@click.option("--config", "config_path", default=None, help="Path to config.yml.")
def detect(member, congress, config_path):
    """Run signal detection."""
    cfg = load_config(config_path)
    session = get_session(cfg.database.url)
    congress_num = congress or cfg.congress.current
    try:
        stats = engine.run(
            session=session,
            bioguide_id=member,
            congress=congress_num,
            rule_weights=dict(vars(cfg.detection.rule_weights)),
            trade_window_days=cfg.detection.trade_window_days,
            min_holding_value=cfg.detection.min_holding_value,
            family_discount=cfg.detection.family_holding_discount,
        )
        click.echo(
            f"Detection complete: {stats['members_processed']} members processed, "
            f"{stats['conflicts_found']} signals detected."
        )
    finally:
        session.close()


@main.command()
@click.option("--config", "config_path", default=None, help="Path to config.yml.")
def coverage(config_path):
    """Show data coverage as JSON."""
    cfg = load_config(config_path)
    session = get_session(cfg.database.url)
    try:
        click.echo(json.dumps(q.get_data_coverage(session), indent=2))
    finally:
        session.close()


if __name__ == "__main__":
    main()
