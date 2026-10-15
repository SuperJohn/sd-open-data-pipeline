# sd-open-data-pipeline

[![ci](https://github.com/SuperJohn/sd-open-data-pipeline/actions/workflows/ci.yml/badge.svg)](https://github.com/SuperJohn/sd-open-data-pipeline/actions/workflows/ci.yml)
[![docs](https://img.shields.io/badge/dbt_docs-GitHub_Pages-orange)](https://superjohn.github.io/sd-open-data-pipeline/)
![python](https://img.shields.io/badge/python-3.12-blue) ![dbt](https://img.shields.io/badge/dbt-duckdb-orange) ![dagster](https://img.shields.io/badge/orchestration-dagster-purple)

An end-to-end, production-shaped analytics pipeline over the City of San Diego's
**Get It Done (311)** service requests: potholes, streetlights, graffiti, illegal
dumping, and so on.

**Extract → DuckDB → dbt (staging → intermediate → marts) → data-quality gates → Dagster
schedule → Streamlit dashboard.** It runs on a laptop in under a minute against the full
~400k-row live extract, and fully offline against a checked-in sample (that's what CI uses).

![dashboard](docs/dashboard.png)

## Why this exists

Most "portfolio pipelines" stop at a notebook. This one is built the way I'd build a
small team's first production pipeline:

- **Idempotent, year-partitioned loads.** Raw data lands as untyped VARCHAR with file and
  close-year lineage (`_source_year`); reloading one year does not touch the others.
- **Incremental dbt models.** Staging and `fct_requests` upsert on `service_request_id`
  using `date_requested` (`requested_at`) lookback plus ingest `_loaded_at` (the city
  extract has no `update_date`). `make dbt-build` is incremental; `make dbt-full-refresh`
  (or `FULL_REFRESH=1`) rebuilds from scratch.
- **Layered dbt models** with tests at every layer, including custom generic tests and a
  reconciliation test between the fact table and the aggregates.
- **A blocking quality gate before transformation.** A pandera schema check runs as a
  Dagster asset check, so a broken extract never reaches the marts.
- **Orchestration as software-defined assets.** dbt models and tests show up in Dagster as
  assets and asset checks, with one lineage graph from the CSV to the dashboard.
- **Source freshness with alerting.** dbt watches `_loaded_at`; a Dagster sensor polls load
  and city-feed clocks and requests a live refresh when either goes stale.
- **Districts from official boundaries.** Each geocoded request is placed in a City Council
  district by point-in-polygon against the city's GIS shapes (DuckDB `spatial`), and the
  city-reported district is kept alongside so disagreements are measurable.
- **Laptop-to-CI parity.** The same `make` targets run locally and in GitHub Actions.

## Architecture

```mermaid
flowchart LR
    subgraph Source["data.sandiego.gov (public, no key)"]
        A1[get_it_done_requests_open.csv]
        A2[get_it_done_requests_closed_YYYY.csv]
        A3[council_districts_datasd.geojson]
    end
    subgraph Ingest["Dagster asset: raw/get_it_done_requests"]
        B[sd311.ingest<br/>download + load as VARCHAR<br/>+ _source_file, _source_year, _loaded_at]
    end
    B2[sd311.districts<br/>validate districts 1-9<br/>load GeoJSON text]
    G{{pandera gate<br/>blocking asset check}}
    subgraph DuckDB["DuckDB warehouse"]
        R[(raw.get_it_done_requests)]
        RD[(raw.council_districts)]
        SD[staging.stg_city_gis__council_districts<br/>GEOMETRY via spatial]
        S[staging.stg_get_it_done__requests<br/>typed · trimmed · deduped · incremental]
        I[intermediate.int_requests__enriched<br/>lifecycle · SLA · geo validity<br/>ST_Contains district]
        F[marts.fct_requests<br/>incremental upsert]
        M1[marts.mart_service_daily]
        M2[marts.mart_service_performance]
        M3[marts.mart_district_performance]
    end
    D[Streamlit dashboard]
    A1 & A2 --> B --> R --> G --> S --> I --> F
    A3 --> B2 --> RD --> SD --> I
    F --> M1 & M2 & M3 --> D
    SCH[[Dagster schedule<br/>daily 06:00 PT]] -.-> B
```

## Quickstart

Requires Python 3.11+ and [uv](https://docs.astral.sh/uv/).

```bash
make setup                # uv sync + dbt parse
make run                  # Dagster: ingest sample -> pandera gate -> dbt build (models + tests)
make dashboard            # http://localhost:8501

make run SOURCE=live      # open + closed years 2016→current (~GB-scale download)
make ingest START_YEAR=2020 SOURCE=live   # backfill closed years from 2020 only
make dbt-build            # incremental staging + fact (default)
make dbt-full-refresh     # rebuild incremental models from scratch
make dev                  # Dagster UI at http://localhost:3000 (lineage, checks, schedule)
make test                 # ruff + pytest (unit + dbt integration)
make freshness            # dbt source freshness against the current warehouse
make docs                 # dbt docs site -> ./site (also deployed on main)
```

Without Dagster, the same flow is `make ingest && make dbt-build`.

### With Docker

One command builds the image, runs the pipeline once on the sample data, and then
starts the Dagster UI and the dashboard (requires Docker with Compose v2):

```bash
docker compose up --build              # or: make up / make down
# Dagster UI  http://localhost:3000    Dashboard  http://localhost:8501

SD311_SOURCE=live docker compose up --build   # same stack against the live extract
```

All three services share one image (`docker/entrypoint.sh` picks the role: `pipeline`,
`dagster`, or `dashboard`) and one named `warehouse` volume holding the DuckDB file.
The dashboard and Dagster wait for the pipeline to finish before starting. CI builds the
image and smoke-tests the whole stack on every push.

## Data lineage

| Layer | Object | Grain | What happens |
|---|---|---|---|
| raw | `raw.get_it_done_requests` | one row per CSV row | Idempotent load of the open extract plus closed_YYYY files from `CLOSED_START_YEAR` (default 2016) through the current year. All VARCHAR, plus `_source_file`, `_source_year` (NULL for open), and `_loaded_at`. Reloading a year replaces that partition only. |
| staging | `stg_get_it_done__requests` | `service_request_id` | Casts with `try_cast`, trims, and turns empty strings into NULL. **Dedupes requests that appear in both extracts** while they transition, preferring the closed record. |
| raw | `raw.council_districts` | one row per district | City Council district polygons as GeoJSON text, validated to exactly districts 1–9. Sample mode uses a checked-in copy simplified to ~10 m; live mode downloads the full-resolution file. |
| staging | `stg_city_gis__council_districts` | `council_district` | Polygons parsed to DuckDB spatial `GEOMETRY` (table, parsed once). |
| intermediate | `int_requests__enriched` | `service_request_id` | `days_to_close`, `open_age_days`, `is_open`, channel (self-service, assisted, or internal), calendar fields, **geo validity against the city bounding box**, and **council district by `ST_Contains`** (`gis_council_district`, the resolved `council_district`, its `council_district_source`, and whether the city-reported district agrees). |
| marts | `fct_requests` | `service_request_id` | Analytics-ready fact with `closed_within_sla` (30 days by default, via `var('sla_days')`). Invalid geocodes are nulled. |
| marts | `mart_service_daily` | `requested_date × service_name` | Opened, closed, and still-open counts, plus median days to close. |
| marts | `mart_service_performance` | `service_name` | Volume, median and p90 days to close, SLA %, backlog, and self-service %. |
| marts | `mart_district_performance` | `council_district` | District scorecard on GIS-assigned districts, with `pct_assigned_by_gis` and `pct_reported_district_mismatch`. |

## Data-quality checks: what they catch

| Where | Check | Catches | Severity |
|---|---|---|---|
| Ingest (schema contract) | CSV headers match EXPECTED_COLUMNS | added or removed columns in the source extract | **block** |
| Pre-dbt (pandera, Dagster blocking check) | required columns present; no unexpected columns | upstream schema changes or renamed columns | **block** |
| | `service_request_id` unique and not null | a bad extract or double load | **block** |
| | `date_requested` parseable, `status` in the known set | format drift or new status codes | **block** |
| | ≥ 100 rows | an empty or truncated download | **block** |
| dbt source | `not_null` keys, `min_row_count` | the same issues, at the dbt layer | error |
| dbt source freshness | `_loaded_at` warn 24h / error 48h (`dbt source freshness`) | missed ingest or a stuck schedule | warn / error |
| Pre-dbt (freshness gate) | load age (`_loaded_at`) and city-feed age (`date_requested`) | warehouse not refreshed, or city extract stopped updating | warn / error |
| Dagster freshness | `FreshnessPolicy` on raw asset + `city_feed_freshness_sensor` | alerts and requests a live refresh when clocks exceed error thresholds | alert |
| staging | `unique` / `not_null` PK, `accepted_values(status)` | broken dedupe logic, new statuses | error |
| | `in_range(council_district, 1, 9)` | bad district codes | error |
| | `not_before(closed_at, requested_at)` | requests closed before they were opened | warn |
| | `max_null_fraction(latitude / council_district, 5%)` | a geocoding outage upstream | warn |
| | `assert_no_requests_in_future` | timezone or parse bugs | error |
| intermediate | `in_range(days_to_close, 0, 3650)` | decade-old tickets closed in bulk (data hygiene) | warn |
| Ingest (districts) | exactly districts 1–9, polygon geometry, no duplicates | a truncated or changed boundary file | **block** |
| intermediate | `assert_geocoded_requests_get_gis_district` (≤ 2% outside every polygon) | wrong or incomplete boundary file | warn |
| | `assert_reported_district_mostly_matches_gis` (≤ 2% disagree) | stale districts after redistricting, swapped lat/lng | warn |
| marts | PK tests, `unique_combination(date, service)` | fan-out in aggregates | error |
| | `assert_daily_mart_reconciles_to_fact` | aggregates that drop or double-count rows | error |
| | `in_range(pct_closed_within_sla, 0, 100)`, `latitude` within the bbox | metric bugs, leaked bad geocodes | error |

On the **live** extract (Oct 2026, 409,547 requests) the pipeline passes with **2
warnings**, both real findings in the source data: one request closed before its
open date, and one ticket that took more than 10 years to close. Warnings surface in
Dagster as failed (non-blocking) asset checks, so they're visible without paging anyone.

**Districts:** on the sample, 99.9% of valid geocodes land inside a district polygon, and
the city-reported district disagrees with the official boundary for 5 of 3,942 requests
(0.13%), all within 5 m of the reported district's edge, inside the ~10 m tolerance of the
simplified sample shapes. Requests without a usable geocode keep the reported
district (`council_district_source = 'reported'`).

**Coverage:** the city publishes *open* requests plus one closed extract per calendar
year. Live ingest backfills closed files from `SD311_CLOSED_START_YEAR` / `--start-year`
(default 2016) through the current year so multi-year trends are not under-counted.
Sample/CI mode uses small year-tagged fixtures (2024–2026) offline.

## Project layout

```
src/sd311/
  config.py         # env-driven settings (SD311_SOURCE, SD311_DUCKDB_PATH)
  contracts.py      # raw CSV schema contract (fail fast on column drift)
  ingest.py         # download + load to DuckDB (CLI: python -m sd311.ingest)
  quality.py        # pandera pre-transform gate
  districts.py      # council district polygons: validate + load GeoJSON (CLI: python -m sd311.districts)
  freshness.py      # load + city-feed freshness clocks (dbt + Dagster sensor)
  definitions.py    # Dagster assets, checks, dbt assets, job, schedule, freshness sensors
  docs.py           # package dbt docs artifacts for GitHub Pages
dbt/
  docs/overview.md  # dbt docs homepage
  models/{staging,intermediate,marts}/
  tests/generic/    # in_range, not_before, max_null_fraction, min_row_count, unique_combination
  tests/singular/   # reconciliation + future-dated requests
dashboard/app.py    # Streamlit over the marts
docker/entrypoint.sh # container roles: pipeline | dagster | dashboard
data/reference/     # council district polygons, simplified to ~10 m for offline use
data/sample/        # 4,000-row sample (1,500 open + 800/800/900 closed 2024–2026), free-text dropped
tests/              # pytest: ingest, quality gate, dbt integration, Dagster definitions
```

## Roadmap

- [x] Incremental dbt models (`is_incremental()` on staging + fact; `make dbt-full-refresh`)
- [ ] Dagster partitions on top of raw `_source_year`
- [x] Backfill prior years' closed extracts (raw `_source_year` partitions, idempotent loader)
- [x] Spatial join to official council district polygons (DuckDB `spatial`)
- [ ] Spatial join to community plan polygons
- [x] dbt source freshness, plus Dagster FreshnessPolicy and city-feed alert sensor
- [ ] Snapshot (SCD2) of request status to measure reopen rates
- [ ] Publish marts to MotherDuck or Parquet on S3 and deploy the dashboard
- [x] Publish dbt docs to GitHub Pages (CI builds `site/`, deploys on main)
- [ ] Data contracts on the marts (dbt model contracts + versioned schemas)

## Data & license

Data: [City of San Diego Open Data Portal, Get It Done](https://data.sandiego.gov/datasets/get-it-done-311/)
and [City Council Districts](https://data.sandiego.gov/datasets/gis-city-council-districts/),
published under the portal's open data terms. The in-repo sample drops the free-text
`public_description` column. Code: MIT.
