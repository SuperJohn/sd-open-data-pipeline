SHELL := /bin/bash
SOURCE ?= sample            # sample (offline, checked in) | live (download from data.sandiego.gov)
export SD311_SOURCE := $(strip $(SOURCE))
export DAGSTER_HOME ?= $(CURDIR)/.dagster
RUN := uv run
DBT := $(RUN) dbt --no-use-colors
DBT_ARGS := --project-dir dbt --profiles-dir dbt

.PHONY: help setup ingest dbt-build run dev test lint format dashboard clean

help:  ## Show targets
	@grep -E '^[a-z-]+:.*##' $(MAKEFILE_LIST) | awk -F':.*## ' '{printf "  %-12s %s\n", $$1, $$2}'

setup:  ## Install Python deps (uv) and parse the dbt project
	uv sync
	mkdir -p $(DAGSTER_HOME) data/warehouse
	$(DBT) parse $(DBT_ARGS) --quiet

ingest:  ## Load raw CSVs into DuckDB (SOURCE=sample|live)
	$(RUN) python -m sd311.ingest --source $(SD311_SOURCE)

dbt-build:  ## dbt build (models + tests) against the current warehouse
	$(DBT) build $(DBT_ARGS)

run: setup  ## Full pipeline through Dagster: ingest -> pandera gate -> dbt build
	$(RUN) dagster asset materialize -m sd311.definitions --select '*' \
	  --config-json '{"ops": {"raw__get_it_done_requests": {"config": {"source": "$(SD311_SOURCE)"}}}}'

dev: setup  ## Dagster UI on http://localhost:3000
	$(RUN) dagster dev -m sd311.definitions

test:  ## Lint + unit + dbt integration tests
	$(RUN) ruff check .
	$(RUN) ruff format --check .
	$(RUN) pytest -q

lint:  ## Ruff lint
	$(RUN) ruff check .

format:  ## Ruff autoformat
	$(RUN) ruff format . && $(RUN) ruff check --fix .

dashboard:  ## Streamlit dashboard on http://localhost:8501
	$(RUN) streamlit run dashboard/app.py

clean:  ## Remove warehouse, dbt artifacts, downloads
	rm -rf data/warehouse data/raw dbt/target dbt/logs .dagster .pytest_cache
