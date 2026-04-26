import json
from types import SimpleNamespace

from click.testing import CliRunner

from lighthouse.cli import main as cli_main
from lighthouse.config import Config
from lighthouse.db.models import Asset, Base, FinancialDisclosure, Member, get_session, init_db
from lighthouse import cli as cli_mod


def _make_db(tmp_path):
    db_path = tmp_path / "assets.sqlite"
    db_url = f"sqlite:///{db_path}"
    Base.metadata.schema = None
    for table in Base.metadata.tables.values():
        table.schema = None
    init_db(db_url)
    session = get_session(db_url)
    member = Member(
        bioguide_id="A000001",
        full_name="Test Member",
        first_name="Test",
        last_name="Member",
        party="D",
        state="CA",
        district=12,
        chamber="house",
        is_active=True,
    )
    disclosure = FinancialDisclosure(
        bioguide_id="A000001",
        filer_name="Test Member",
        filer_type="member",
        filing_type="annual",
        year=2024,
        source="house",
        raw_file_path="/tmp/disclosure.html",
    )
    session.add_all([member, disclosure])
    session.flush()

    assets = [
        Asset(
            disclosure_id=disclosure.id,
            bioguide_id="A000001",
            asset_name="Apple Inc.",
            asset_type="other",
            ticker=None,
            owner="self",
            year=2024,
            sector="other",
        ),
        Asset(
            disclosure_id=disclosure.id,
            bioguide_id="A000001",
            asset_name="Vanguard Total Stock Market Index Fund",
            asset_type="other",
            ticker=None,
            owner="self",
            year=2024,
            sector="other",
        ),
        Asset(
            disclosure_id=disclosure.id,
            bioguide_id="A000001",
            asset_name="City of Austin Municipal Bond",
            asset_type="other",
            ticker=None,
            owner="self",
            year=2024,
            sector="other",
        ),
        Asset(
            disclosure_id=disclosure.id,
            bioguide_id="A000001",
            asset_name="Mystery Holdings LLC",
            asset_type="public_equity",
            ticker="ZZZ",
            owner="self",
            year=2024,
            sector="financials",
        ),
        Asset(
            disclosure_id=disclosure.id,
            bioguide_id="A000001",
            asset_name="Miscellaneous Asset",
            asset_type="unknown",
            ticker=None,
            owner="self",
            year=2024,
            sector=None,
        ),
    ]
    session.add_all(assets)
    session.commit()
    session.close()

    cfg = Config()
    cfg.database.url = db_url
    return cfg, db_url


def _invoke_classify_assets(monkeypatch, cfg, args):
    monkeypatch.setattr(cli_mod, "load_config", lambda _: cfg)
    runner = CliRunner()
    return runner.invoke(cli_main, ["classify-assets", *args])


def test_classify_assets_backfill_updates_missing_classifications_and_is_idempotent(tmp_path, monkeypatch):
    cfg, db_url = _make_db(tmp_path)

    result = _invoke_classify_assets(monkeypatch, cfg, [])
    assert result.exit_code == 0, result.output
    payload = json.loads(result.output)
    assert payload["changed_rows"] >= 3
    assert payload["before"]["unknown_count"] >= 1
    assert payload["after"]["diversified_count"] >= 1

    session = get_session(db_url)
    rows = {asset.asset_name: asset for asset in session.query(Asset).all()}

    assert rows["Apple Inc."].ticker == "AAPL"
    assert rows["Apple Inc."].asset_type == "public_equity"
    assert rows["Apple Inc."].sector == "information_technology"

    assert rows["Vanguard Total Stock Market Index Fund"].asset_type == "diversified_fund"
    assert rows["Vanguard Total Stock Market Index Fund"].sector == "diversified"

    assert rows["City of Austin Municipal Bond"].asset_type == "municipal_bond"
    assert rows["City of Austin Municipal Bond"].sector == "fixed_income"

    assert rows["Mystery Holdings LLC"].ticker == "ZZZ"
    assert rows["Mystery Holdings LLC"].sector == "financials"

    assert rows["Miscellaneous Asset"].asset_type == "unknown"
    assert rows["Miscellaneous Asset"].sector is None

    second = _invoke_classify_assets(monkeypatch, cfg, [])
    assert second.exit_code == 0, second.output
    second_payload = json.loads(second.output)
    assert second_payload["changed_rows"] == 0


def test_classify_assets_dry_run_does_not_write_changes(tmp_path, monkeypatch):
    cfg, db_url = _make_db(tmp_path)

    result = _invoke_classify_assets(monkeypatch, cfg, ["--dry-run"])
    assert result.exit_code == 0, result.output
    payload = json.loads(result.output)
    assert payload["dry_run"] is True
    assert payload["changed_rows"] >= 1

    session = get_session(db_url)
    apple = session.query(Asset).filter(Asset.asset_name == "Apple Inc.").one()
    assert apple.ticker is None
    assert apple.asset_type == "other"
    assert apple.sector == "other"


def test_classify_assets_preserves_existing_known_sector_and_ticker_when_classifier_is_weaker(tmp_path, monkeypatch):
    cfg, db_url = _make_db(tmp_path)

    result = _invoke_classify_assets(monkeypatch, cfg, [])
    assert result.exit_code == 0, result.output

    session = get_session(db_url)
    preserved = session.query(Asset).filter(Asset.asset_name == "Mystery Holdings LLC").one()
    assert preserved.ticker == "ZZZ"
    assert preserved.sector == "financials"
    assert preserved.asset_type == "public_equity"
