# Local FEC Warehouse

Lighthouse should keep campaign-finance data local, but scoped.

## Recommended scope

- Keep recent federal cycles only: `2022`, `2024`, `2026`
- Import only committees linked to current members of Congress
- Import only contribution rows that flow into those committees
- Normalize the subset into `core.campaign_contributions`

This avoids mirroring the full national FEC warehouse unless you explicitly want it.

## Storage model

Use one local Postgres database for Lighthouse with separate schemas:

- `raw`
  - `raw.fec_committees`
  - `raw.fec_candidate_committee_linkages`
  - `raw.fec_individual_contributions`
- `core`
  - `core.campaign_contributions`

The `raw` tables preserve source-level finance records for traceability.
The `core` table stays small and query-friendly for member profiles, dashboard summaries, and detection rules.

## What fits locally

This machine currently has roughly `162 GiB` free.

A scoped congressional subset should fit comfortably.
The disk risk comes from trying to mirror all bulk FEC years and tables indiscriminately.

## Import strategy

Preferred path:

1. Build or restore a separate local bulk FEC database such as `fec_complete`
2. Point Lighthouse at it using `fec_warehouse.source_db_url`
3. Run `python scripts/ingest.py --source fec`

Fallback path:

1. Use the live FEC API only for small spot imports
2. Replace API-derived data later with local bulk imports

## Funding analytics

The current local import path supports:

- donor size buckets
- PAC vs individual mix
- top donors
- top funding sectors

Important caveat:
bulk FEC individual contributions do not ship a clean `contributor_industry` field like the API does.
Lighthouse therefore derives a sector label from employer, occupation, contributor name, and committee name heuristics.

That means:

- donor size and PAC/individual mix are strong signals
- sector labels are useful but heuristic

## Config example

```yaml
fec_warehouse:
  source_db_url: "postgresql+psycopg:///fec_complete?host=/tmp"
  raw_dir: "./data/fec"
  cycles:
    - 2022
    - 2024
    - 2026
  prefer_local_db: true
```
