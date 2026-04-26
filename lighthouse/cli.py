"""
Minimal Click CLI for ingestion, detection, and coverage inspection.
"""
from __future__ import annotations

import json
from collections import Counter

import click

from .config import load_config
from .db.models import Asset, get_engine, get_session, init_db, upgrade_db
from .detection.asset_classifier import classify_asset_record
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


@main.command(name="asset-audit")
@click.option("--member", "bioguide_id", default=None, help="Optional Bioguide ID to scope the audit.")
@click.option("--min-value", default=1000.0, type=float, show_default=True, help="Minimum asset value to include.")
@click.option("--config", "config_path", default=None, help="Path to config.yml.")
def asset_audit(bioguide_id, min_value, config_path):
    """Show asset classification distribution as JSON."""
    cfg = load_config(config_path)
    session = get_session(cfg.database.url)
    try:
        click.echo(json.dumps(q.get_asset_classification_distribution(session, bioguide_id, min_value), indent=2))
    finally:
        session.close()


@main.command(name="ingest-elections")
@click.option("--house",   "house_path",    default=None, help="Path to MIT house CSV (1976-2022-house.csv).")
@click.option("--senate",  "senate_path",   default=None, help="Path to MIT senate CSV (1976-2022-senate.csv).")
@click.option("--governor","governor_path", default=None, help="Path to MIT governor CSV (1976-2022-governor.csv).")
@click.option("--president","president_path",default=None,help="Path to MIT president CSV (1976-2020-president.csv).")
@click.option("--config",  "config_path",   default=None, help="Path to config.yml.")
def ingest_elections(house_path, senate_path, governor_path, president_path, config_path):
    """Load election results from MIT Election Data and Science Lab CSVs.

    Download CSVs from https://dataverse.harvard.edu/dataverse/medsl then run:

        lighthouse ingest-elections --house 1976-2022-house.csv --senate 1976-2022-senate.csv
    """
    from .collectors.mit_elections import ingest_house_csv, ingest_senate_csv, ingest_governor_csv, ingest_president_csv

    cfg = load_config(config_path)
    session = get_session(cfg.database.url)
    try:
        if not any([house_path, senate_path, governor_path, president_path]):
            raise click.UsageError(
                "Provide at least one CSV path. Example:\n"
                "  lighthouse ingest-elections --house 1976-2022-house.csv"
            )
        for path, fn, label in [
            (house_path,    ingest_house_csv,    "House"),
            (senate_path,   ingest_senate_csv,   "Senate"),
            (governor_path, ingest_governor_csv, "Governor"),
            (president_path,ingest_president_csv,"President"),
        ]:
            if path:
                click.echo(f"Ingesting {label} elections from {path}…")
                result = fn(session, path)
                click.echo(f"  → {result['candidates_added']} candidates, {result['races_added']} races.")
    finally:
        session.close()


@main.command(name="classify-assets")
@click.option("--member", "bioguide_id", default=None, help="Optional Bioguide ID to scope the backfill.")
@click.option("--dry-run", is_flag=True, help="Preview changes without writing to the database.")
@click.option("--config", "config_path", default=None, help="Path to config.yml.")
def classify_assets(bioguide_id, dry_run, config_path):
    """Backfill asset classification fields for existing Asset rows."""
    cfg = load_config(config_path)
    session = get_session(cfg.database.url)
    try:
        assets = _load_assets(session, bioguide_id)
        before_rows = []
        changes = []
        simulated_assets = []

        for asset in assets:
            before_rows.append(_asset_snapshot(asset))
            update, simulated = _classify_existing_asset(asset)
            if update:
                changes.append({"id": asset.id, "bioguide_id": asset.bioguide_id, "changes": update})
                if not dry_run:
                    for field, value in update.items():
                        setattr(asset, field, value)
            simulated_assets.append(simulated)

        before = _summarize_raw_asset_rows(before_rows)
        after = _summarize_raw_asset_rows(simulated_assets)

        if not dry_run:
            session.commit()

        click.echo(json.dumps({
            "dry_run": dry_run,
            "scoped_member": bioguide_id,
            "total_rows": len(assets),
            "changed_rows": len(changes),
            "before": before,
            "after": after,
            "changes": changes[:50],
        }, indent=2, default=str))
    finally:
        session.close()


@main.command(name="migrate")
@click.option("--config", "config_path", default=None, help="Path to config.yml.")
def migrate_cmd(config_path):
    """Apply lightweight schema upgrades for existing databases."""
    cfg = load_config(config_path)
    engine = get_engine(cfg.database.url)
    upgrade_db(engine)
    click.echo("Schema upgrade complete.")


def _load_assets(session, bioguide_id: str | None):
    query = session.query(Asset)
    if bioguide_id:
        query = query.filter(Asset.bioguide_id == bioguide_id)
    return query.order_by(Asset.id.asc()).all()


def _asset_snapshot(asset: Asset) -> dict:
    return {
        "asset_name": asset.asset_name,
        "ticker": asset.ticker,
        "asset_type": asset.asset_type,
        "sector": asset.sector,
        "owner": asset.owner,
        "value_max": float(asset.value_max) if asset.value_max is not None else None,
        "value_min": float(asset.value_min) if asset.value_min is not None else None,
    }


def _classify_existing_asset(asset: Asset) -> tuple[dict, dict]:
    classification = classify_asset_record({
        "asset_name": asset.asset_name,
        "ticker": asset.ticker,
        "asset_type": asset.asset_type,
        "sector": asset.sector,
    })

    current_sector = _normalize_bucket(asset.sector)
    current_asset_class = _normalize_bucket(asset.asset_type)
    current_ticker = (asset.ticker or "").strip().upper() or None

    update: dict[str, object] = {}
    if _should_update_sector(current_sector, classification["sector"]):
        update["sector"] = classification["sector"]

    proposed_asset_class = classification["asset_class"]
    if current_asset_class == "stock":
        update["asset_type"] = proposed_asset_class if proposed_asset_class != "stock" else "public_equity"
    elif current_asset_class == "bond":
        if proposed_asset_class in {"bond", "unknown"}:
            update["asset_type"] = "fixed_income"
        elif proposed_asset_class != "bond":
            update["asset_type"] = proposed_asset_class
    elif _should_update_asset_class(current_asset_class, proposed_asset_class):
        update["asset_type"] = proposed_asset_class
    if _should_update_ticker(current_ticker, classification):
        update["ticker"] = classification["matched_ticker"]

    simulated = {
        "asset_name": asset.asset_name,
        "ticker": update.get("ticker", asset.ticker),
        "asset_type": update.get("asset_type", asset.asset_type),
        "sector": update.get("sector", asset.sector),
    }
    simulated.update({
        "owner": asset.owner,
        "value_max": float(asset.value_max) if asset.value_max is not None else None,
        "value_min": float(asset.value_min) if asset.value_min is not None else None,
    })
    classified_simulated = classify_asset_record(simulated)
    simulated.update({
        "asset_class": classified_simulated["asset_class"],
        "classification_confidence": classified_simulated["classification_confidence"],
        "classification_reason": classified_simulated["classification_reason"],
        "is_diversified": classified_simulated["is_diversified"],
        "matched_ticker": classified_simulated["matched_ticker"],
    })
    return update, simulated


def _should_update_sector(current_sector: str, proposed_sector: str) -> bool:
    if proposed_sector in {None, "", "unknown", "other"}:
        return False
    return current_sector in {"", "unknown", "other"}


def _should_update_asset_class(current_asset_class: str, proposed_asset_class: str) -> bool:
    if proposed_asset_class in {None, "", "unknown", "other"}:
        return False
    generic_current = {"", "unknown", "other", "stock", "bond", "fund"}
    if current_asset_class in generic_current:
        return True
    if current_asset_class == "public_equity" and proposed_asset_class == "public_equity":
        return False
    return False


def _should_update_ticker(current_ticker: str | None, classification: dict) -> bool:
    if current_ticker:
        return False
    return bool(classification.get("matched_ticker")) and classification.get("classification_confidence") == "high" and classification.get("asset_class") == "public_equity"


def _summarize_raw_asset_rows(rows: list[dict]) -> dict:
    sector_counts: Counter[str] = Counter()
    asset_class_counts: Counter[str] = Counter()
    confidence_counts: Counter[str] = Counter()
    unknown_count = 0
    other_count = 0
    diversified_count = 0

    for row in rows:
        classified = classify_asset_record(row)
        sector = _normalize_bucket(row.get("sector"))
        asset_class = _normalize_bucket(row.get("asset_type"))
        confidence = classified["classification_confidence"] or "low"

        sector_counts[sector] += 1
        asset_class_counts[asset_class] += 1
        confidence_counts[confidence] += 1

        if sector in {"unknown", "other"} or asset_class in {"unknown", "other"}:
            unknown_count += 1
        if sector == "other" or asset_class == "other":
            other_count += 1
        if sector == "diversified" or asset_class == "diversified_fund" or classified.get("is_diversified"):
            diversified_count += 1

    return {
        "sector_counts": dict(sector_counts),
        "asset_class_counts": dict(asset_class_counts),
        "confidence_counts": dict(confidence_counts),
        "unknown_count": unknown_count,
        "other_count": other_count,
        "diversified_count": diversified_count,
        "total": len(rows),
    }


def _normalize_bucket(value: object) -> str:
    text = str(value or "").strip().lower()
    if not text:
        return "unknown"
    if text in {"stock", "bond", "fund"}:
        return text
    if text in {"other", "unknown", "none", "null"}:
        return "unknown"
    return text


if __name__ == "__main__":
    main()
