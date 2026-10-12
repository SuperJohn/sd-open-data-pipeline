#!/bin/sh
# Container entrypoint. One image, three roles:
#   pipeline   run ingest -> pandera gate -> dbt build once, then exit (default)
#   dagster    Dagster UI + daemon on 0.0.0.0:${DAGSTER_PORT:-3000}
#   dashboard  Streamlit on 0.0.0.0:${DASHBOARD_PORT:-8501}
# Anything else is executed as-is (e.g. `docker compose run --rm dagster bash`).
set -eu

role="${1:-pipeline}"
source_mode="${SD311_SOURCE:-sample}"

case "$role" in
  pipeline)
    case "$source_mode" in
      sample | live) ;;
      *)
        echo "SD311_SOURCE must be 'sample' or 'live' (got '$source_mode')" >&2
        exit 2
        ;;
    esac
    mkdir -p "$(dirname "${SD311_DUCKDB_PATH:-/app/data/warehouse/sd311.duckdb}")"
    exec dagster asset materialize -m sd311.definitions --select '*' \
      --config-json "{\"ops\": {\"raw__get_it_done_requests\": {\"config\": {\"source\": \"$source_mode\"}}}}"
    ;;
  dagster)
    exec dagster dev -m sd311.definitions -h 0.0.0.0 -p "${DAGSTER_PORT:-3000}"
    ;;
  dashboard)
    exec streamlit run dashboard/app.py \
      --server.address=0.0.0.0 \
      --server.port="${DASHBOARD_PORT:-8501}" \
      --server.headless=true \
      --browser.gatherUsageStats=false
    ;;
  *)
    exec "$@"
    ;;
esac
