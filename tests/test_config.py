from lighthouse.config import load_config
from sqlalchemy import create_engine, inspect, text

from lighthouse.db.models import upgrade_db


def test_data_year_defaults_load_correctly(tmp_path):
    config_path = tmp_path / "config.yml"
    config_path.write_text("{}", encoding="utf-8")

    cfg = load_config(str(config_path))

    assert cfg.data.disclosure_year == 2024
    assert cfg.data.ptr_year == 2024
    assert cfg.data.fec_cycle == 2024


def test_upgrade_db_is_importable_and_idempotent_for_existing_tables(tmp_path):
    db_path = tmp_path / "upgrade.sqlite"
    engine = create_engine(f"sqlite:///{db_path}")

    with engine.begin() as conn:
        conn.execute(text("""
            CREATE TABLE campaign_contributions (
                id INTEGER PRIMARY KEY,
                bioguide_id VARCHAR(10),
                fec_committee_id VARCHAR(20),
                contributor_name TEXT,
                contributor_employer TEXT,
                contributor_industry VARCHAR(100),
                amount NUMERIC,
                contribution_date DATE,
                election_cycle INTEGER,
                contribution_type VARCHAR(20)
            )
        """))
        conn.execute(text("""
            CREATE TABLE stock_transactions (
                id INTEGER PRIMARY KEY,
                bioguide_id VARCHAR(10),
                transaction_date DATE NOT NULL,
                disclosure_date DATE,
                ticker VARCHAR(20),
                asset_name TEXT,
                transaction_type VARCHAR(20),
                amount_min NUMERIC,
                amount_max NUMERIC,
                owner VARCHAR(20),
                source VARCHAR(30),
                comment TEXT,
                sector VARCHAR(50),
                industry_code VARCHAR(20)
            )
        """))
        conn.execute(text("""
            CREATE TABLE fec_individual_contributions (
                source_sub_id VARCHAR(100) NOT NULL,
                file_year INTEGER NOT NULL,
                member_bioguide_id VARCHAR(10) NOT NULL,
                candidate_id VARCHAR(9),
                committee_id VARCHAR(9) NOT NULL,
                committee_name VARCHAR(200),
                contributor_name TEXT,
                contributor_employer TEXT,
                contributor_occupation TEXT,
                contributor_state VARCHAR(2),
                entity_type VARCHAR(3),
                transaction_type VARCHAR(3),
                transaction_dt_raw VARCHAR(100),
                transaction_date DATE,
                amount NUMERIC,
                other_id VARCHAR(9),
                image_num VARCHAR(20),
                memo_text TEXT,
                derived_sector VARCHAR(50),
                source_record_hash VARCHAR(64),
                PRIMARY KEY (source_sub_id, file_year)
            )
        """))

    upgrade_db(engine)
    upgrade_db(engine)

    inspector = inspect(engine)
    contrib_cols = {col["name"] for col in inspector.get_columns("campaign_contributions")}
    stock_cols = {col["name"] for col in inspector.get_columns("stock_transactions")}
    raw_cols = {col["name"] for col in inspector.get_columns("fec_individual_contributions")}

    assert {"source_table", "source_key", "source_url", "source_file", "source_hash", "source_sub_id", "source_image_num", "source_transaction_id"} <= contrib_cols
    assert {"source_url", "source_file", "source_key", "source_hash"} <= stock_cols
    assert {"memo_code", "source_transaction_id"} <= raw_cols

    contrib_indexes = {idx["name"] for idx in inspector.get_indexes("campaign_contributions")}
    stock_indexes = {idx["name"] for idx in inspector.get_indexes("stock_transactions")}
    raw_indexes = {idx["name"] for idx in inspector.get_indexes("fec_individual_contributions")}

    assert "idx_campaign_contributions_source_key" in contrib_indexes
    assert "idx_campaign_contributions_source_sub_id" in contrib_indexes
    assert "idx_campaign_contributions_source_hash" in contrib_indexes
    assert "idx_stock_transactions_source_key" in stock_indexes
    assert "idx_stock_transactions_source_hash" in stock_indexes
    assert "idx_raw_fec_contrib_source_txn_id" in raw_indexes
