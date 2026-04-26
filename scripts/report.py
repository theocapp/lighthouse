#!/usr/bin/env python3
"""
CLI: Generate signal reports.

Usage:
    python scripts/report.py --member A000370
    python scripts/report.py --member A000370 --format html --output ./output/
    python scripts/report.py --member A000370 --format csv
    python scripts/report.py --all --format json --output ./output/
"""
import sys
from pathlib import Path

import click

sys.path.insert(0, str(Path(__file__).parent.parent))

from lighthouse.config import load_config
from lighthouse.db.models import Member, get_session
from lighthouse.reporting import member_report
from lighthouse.reporting.formatters import csv_formatter, html_formatter, json_formatter

TEMPLATES_DIR = Path(__file__).parent.parent / "lighthouse" / "reporting" / "templates"


@click.command()
@click.option("--member", "-m", default=None, help="Bioguide ID of a specific member.")
@click.option("--all", "all_members", is_flag=True, help="Generate reports for all members.")
@click.option(
    "--format", "-f", "fmt",
    type=click.Choice(["json", "csv", "html"]),
    default="json",
    help="Output format (default: json).",
)
@click.option("--output", "-o", default="./output", help="Output directory.")
@click.option("--config", "config_path", default=None, help="Path to config.yml.")
def cli(member, all_members, fmt, output, config_path):
    cfg = load_config(config_path)
    session = get_session(cfg.database.url)
    out_dir = Path(output)

    try:
        if all_members:
            members = session.query(Member).filter(Member.is_active == True).all()
            bioguide_ids = [m.bioguide_id for m in members]
        elif member:
            bioguide_ids = [member]
        else:
            click.echo("Specify --member <bioguide_id> or --all.", err=True)
            sys.exit(1)

        for bid in bioguide_ids:
            report = member_report.build_report(session, bid)
            if not report:
                click.echo(f"No data found for {bid}", err=True)
                continue

            name_slug = report["member"]["full_name"].replace(" ", "_").lower()
            stem = f"{bid}_{name_slug}"

            if fmt == "json":
                path = json_formatter.write(report, out_dir / f"{stem}.json")
            elif fmt == "csv":
                path = csv_formatter.write(report, out_dir / f"{stem}.csv")
            elif fmt == "html":
                path = html_formatter.write(report, out_dir / f"{stem}.html", TEMPLATES_DIR)

            click.echo(f"Report written: {path}")

    except Exception as exc:
        click.echo(f"Error: {exc}", err=True)
        sys.exit(1)
    finally:
        session.close()


if __name__ == "__main__":
    cli()
