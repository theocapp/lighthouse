#!/usr/bin/env python3
"""
CLI: Run signal detection.

Usage:
    python scripts/detect.py                         # all members
    python scripts/detect.py --member A000370        # single member by bioguide ID
    python scripts/detect.py --congress 119
"""
import logging
import sys
from pathlib import Path

import click

sys.path.insert(0, str(Path(__file__).parent.parent))

from lighthouse.config import load_config
from lighthouse.db.models import get_session
from lighthouse.detection import engine

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")


@click.command()
@click.option("--member", "-m", default=None, help="Bioguide ID of a specific member.")
@click.option("--congress", default=None, type=int, help="Congress number (default: from config).")
@click.option("--config", "config_path", default=None, help="Path to config.yml.")
def cli(member, congress, config_path):
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
    except Exception as exc:
        click.echo(f"Error: {exc}", err=True)
        sys.exit(1)
    finally:
        session.close()


if __name__ == "__main__":
    cli()
