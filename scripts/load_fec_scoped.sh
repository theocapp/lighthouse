#!/usr/bin/env bash
set -euo pipefail

BASE_URL="https://cg-519a459a-0ea3-42c2-b7bc-fa1143481f74.s3-us-gov-west-1.amazonaws.com/bulk-downloads"
YEAR="${1:-2024}"
DB_NAME="${DB_NAME:-fec_complete}"
DB_HOST="${DB_HOST:-/tmp}"
DATA_ROOT="${DATA_ROOT:-/Users/theo/lighthouse/data/fec}"
YEAR_DIR="${DATA_ROOT}/${YEAR}"

DATASET_TUPLES=(
  "cn candidate_master"
  "cm committee_master"
  "ccl candidate_committee_linkages"
  "indiv individual_contributions"
)

usage() {
  cat <<EOF
Usage: $0 [year]

Downloads and loads the minimum bulk FEC tables Lighthouse needs for campaign funding:
  - candidate_master
  - committee_master
  - candidate_committee_linkages
  - individual_contributions

Defaults:
  year: 2024
  db:   fec_complete on host /tmp
  dir:  /Users/theo/lighthouse/data/fec/<year>
EOF
}

if [[ "${YEAR}" == "--help" || "${YEAR}" == "-h" ]]; then
  usage
  exit 0
fi

mkdir -p "${YEAR_DIR}"

download_dataset() {
  local abbreviation="$1"
  local table_name="$2"
  local url="${BASE_URL}/${YEAR}/${abbreviation}${YEAR: -2}.zip"
  local output_path="${YEAR_DIR}/${table_name}.txt"

  if [[ -s "${output_path}" ]]; then
    echo "Using existing file ${output_path}"
    return
  fi

  echo "Downloading ${table_name} from ${url}"
  curl -L --fail --silent --show-error "${url}" | \
    funzip | \
    iconv -c -t UTF-8 | \
    tr -d '\010' > "${output_path}"
}

load_table() {
  local table_name="$1"
  local temp_table="temp_${table_name}_${YEAR}"
  local file_path="${YEAR_DIR}/${table_name}.txt"

  if [[ ! -s "${file_path}" ]]; then
    echo "Missing input file ${file_path}" >&2
    exit 1
  fi

  echo "Loading ${table_name} for ${YEAR}"
  psql -h "${DB_HOST}" -d "${DB_NAME}" -v ON_ERROR_STOP=1 -c "DROP TABLE IF EXISTS ${temp_table};"
  psql -h "${DB_HOST}" -d "${DB_NAME}" -v ON_ERROR_STOP=1 -c "CREATE TABLE ${temp_table} AS SELECT * FROM ${table_name} WITH NO DATA;"
  psql -h "${DB_HOST}" -d "${DB_NAME}" -v ON_ERROR_STOP=1 -c "ALTER TABLE ${temp_table} DROP COLUMN file_year;"
  psql -h "${DB_HOST}" -d "${DB_NAME}" -v ON_ERROR_STOP=1 -c "\\copy ${temp_table} from '${file_path}' (FORMAT CSV, DELIMITER('|'), HEADER FALSE, QUOTE E'\\b');"
  psql -h "${DB_HOST}" -d "${DB_NAME}" -v ON_ERROR_STOP=1 -c "INSERT INTO ${table_name} (SELECT DISTINCT ON (${temp_table}.*) *, ${YEAR} AS file_year FROM ${temp_table}) ON CONFLICT DO NOTHING;"
  psql -h "${DB_HOST}" -d "${DB_NAME}" -v ON_ERROR_STOP=1 -c "DROP TABLE ${temp_table};"
}

for dataset in "${DATASET_TUPLES[@]}"; do
  set -- ${dataset}
  download_dataset "$1" "$2"
done

for dataset in "${DATASET_TUPLES[@]}"; do
  set -- ${dataset}
  load_table "$2"
done

echo "Scoped FEC load complete for ${YEAR}"
