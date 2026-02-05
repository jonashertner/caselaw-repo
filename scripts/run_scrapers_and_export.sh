#!/usr/bin/env bash
set -euo pipefail

# This script is intended to be run inside a repo that contains the swiss-caselaw scrapers
# (https://github.com/jonashertner/swiss-caselaw) OR inside a fork that merged this repo.
#
# Output required by this repo:
#   data/exports/decisions.json.gz

ROLLING_DAYS="${CASELAW_ROLLING_SINCE_DAYS:-10}"

if [[ ! -f "docker-compose.yml" ]]; then
  echo "ERROR: docker-compose.yml not found. Put this repo inside your scraper repo, or adapt this script." >&2
  exit 2
fi

SINCE="$(python - <<'PY'
import os, datetime as dt
days=int(os.environ.get("CASELAW_ROLLING_SINCE_DAYS","10"))
d=(dt.date.today()-dt.timedelta(days=days)).isoformat()
print(d)
PY
)"

echo "Rolling ingest since: ${SINCE}"

docker compose up -d db

# Optional: init/tune (idempotent)
docker compose run --rm backend python -m app.cli db init || true
docker compose run --rm backend python -m app.cli db tune || true

docker compose run --rm backend python -m app.cli ingest run --source all --since "${SINCE}"

# Assumed exporter command in the scraper repo.
# If your exporter differs, change this line.
docker compose run --rm backend python -m app.cli export decisions

if [[ ! -f "data/exports/decisions.json.gz" ]]; then
  echo "ERROR: export did not produce data/exports/decisions.json.gz" >&2
  exit 3
fi

ls -lh data/exports/decisions.json.gz
