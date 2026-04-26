# Lighthouse

Lighthouse is a Python project for ingesting congressional, financial-disclosure, stock-trade, campaign-finance, bill, vote, and committee data to surface public-data ethics signals for review.

Lighthouse does not prove corruption, illegality, intent, or misconduct. It surfaces public-data signals that may deserve review. All findings should be independently verified.

## What Lighthouse Does

- Ingests public congressional, disclosure, STOCK Act, and FEC-derived data.
- Normalizes members, bills, votes, assets, trades, committees, and contributions into a relational model.
- Runs rule-based detection to find potential vote-holding, trade-timing, sponsorship-holding, committee-donor, and family-holding signals.
- Scores signal strength separately from confidence in the underlying evidence.
- Produces reports and a small web UI for browsing members, trades, and detected signals.

## What Lighthouse Does Not Claim

- It does not prove insider trading, corruption, bribery, intent, or legal wrongdoing.
- It does not establish motive.
- It does not infer access to nonpublic information.
- It does not conclude that a broad sector vote created a personal financial benefit.

## Data Sources

- Congress.gov / Congress API
- GovInfo BILLSTATUS files
- House Clerk vote feeds
- House and Senate financial disclosures
- House and Senate STOCK Act trade disclosures
- OpenFEC or a local FEC warehouse
- SEC EDGAR where supported by the current ingestion flow

## Architecture

- [`lighthouse/collectors`](/Users/theo/lighthouse/lighthouse/collectors): external source collection
- [`lighthouse/parsers`](/Users/theo/lighthouse/lighthouse/parsers): normalization/parsing
- [`lighthouse/db/models.py`](/Users/theo/lighthouse/lighthouse/db/models.py): SQLAlchemy models
- [`lighthouse/db/queries.py`](/Users/theo/lighthouse/lighthouse/db/queries.py): query helpers, dashboard stats, coverage
- [`lighthouse/pipeline/ingest.py`](/Users/theo/lighthouse/lighthouse/pipeline/ingest.py): ingestion pipeline
- [`lighthouse/detection`](/Users/theo/lighthouse/lighthouse/detection): rule engine, scoring, evidence helpers
- [`lighthouse/reporting`](/Users/theo/lighthouse/lighthouse/reporting): member reports and formatters
- [`app`](/Users/theo/lighthouse/app): FastAPI app and templates

## Setup

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
cp config.yml.example config.yml
python -m lighthouse.cli migrate
```

## Configuration

Main configuration lives in [`config.yml.example`](/Users/theo/lighthouse/config.yml.example).

Important fields:

- `database.url`
- `api_keys.congress_gov`
- `api_keys.fec`
- `data.disclosure_year`
- `data.ptr_year`
- `data.fec_cycle`
- `fec_warehouse.cycles`
- `detection.trade_window_days`
- `detection.min_holding_value`

Defaults remain aligned with the current 2024 disclosure/PTR/FEC ingestion behavior.

## Database Upgrades

New installs are covered by table creation from the current ORM metadata.

For existing local databases, run:

```bash
python -m lighthouse.cli migrate
```

This upgrade path is idempotent and only adds missing provenance columns and indexes.

## Running Ingestion

```bash
python scripts/ingest.py
python scripts/ingest.py --source members --source bills --source votes
python -m lighthouse.cli ingest --source disclosures
```

## Running Detection And Scoring

```bash
python scripts/detect.py
python scripts/detect.py --member A000370
python -m lighthouse.cli detect --congress 119
```

The detection layer stores:

- `score`: signal strength
- `confidence`: confidence in the evidence and match quality
- `detail_json`: evidence tier, limitations, match reason, provenance, and source quality

## Running Reports And The Web App

```bash
python scripts/report.py --member A000370 --format html
uvicorn app.main:app --reload
```

The grouped CLI also supports:

```bash
python -m lighthouse.cli coverage
```

## Data Coverage

Use the coverage helper to inspect current data completeness:

- House votes loaded
- Senate votes loaded or partial/unavailable
- House disclosures loaded
- Senate disclosures loaded
- FEC contributions loaded
- Latest ingestion logs by source

Coverage is available through:

- [`lighthouse.db.queries.get_data_coverage`](/Users/theo/lighthouse/lighthouse/db/queries.py)
- `/api/coverage`
- `python -m lighthouse.cli coverage`

## Traceability

Lighthouse aims to keep every normalized signal anchored to concrete source evidence.

- `FinancialDisclosure` stores `source`, `source_url`, and `raw_file_path`.
- `Asset` traces back through `disclosure_id` to its originating disclosure file and URL.
- `StockTransaction` stores source provenance including source name plus available URL, cache/file path, stable source key, and source hash.
- `CampaignContribution` stores provenance for both local FEC bulk imports and OpenFEC API imports, including source table/endpoint, source key, hash, and available FEC identifiers such as `sub_id`, `image_num`, and transaction id.
- Signal `detail_json` carries bill/vote/disclosure/transaction/contribution provenance where those fields are available.

Current limitation:

- `CampaignContribution` rows are still not linked to raw FEC rows by a formal foreign key. Traceability is direct through stored source identifiers rather than an enforced relational constraint.

## Known Limitations

- Public data only.
- Senate vote ingestion may be partial or unavailable depending on source access.
- Sector and industry matching can be approximate.
- Disclosure value ranges are broad.
- Diversified funds are skipped or heavily discounted because they are weak ownership signals.
- Family-held assets are discounted and should be interpreted more cautiously.
- Campaign contribution patterns do not establish motive or influence.

## Ethics / Legal Disclaimer

Lighthouse does not prove corruption, illegality, intent, or misconduct. It identifies evidence-backed signals that may deserve review. No legal conclusion is implied. Reports should be treated as starting points for further verification, not accusations.
