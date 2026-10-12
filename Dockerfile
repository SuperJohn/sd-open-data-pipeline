# One image for every service: the Dagster pipeline, the Dagster UI, and the dashboard.
# Build:  docker compose build      Run:  docker compose up
ARG PYTHON_VERSION=3.12
ARG UV_VERSION=0.12

FROM ghcr.io/astral-sh/uv:${UV_VERSION} AS uv

FROM python:${PYTHON_VERSION}-slim-bookworm

COPY --from=uv /uv /uvx /bin/

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    UV_COMPILE_BYTECODE=1 \
    UV_LINK_MODE=copy \
    UV_PYTHON_DOWNLOADS=never \
    UV_PROJECT_ENVIRONMENT=/app/.venv \
    PATH="/app/.venv/bin:${PATH}" \
    SD311_SOURCE=sample \
    SD311_DUCKDB_PATH=/app/data/warehouse/sd311.duckdb \
    DAGSTER_HOME=/opt/dagster/home

WORKDIR /app

# Dependencies first (cached layer), then the project itself.
COPY pyproject.toml uv.lock README.md ./
RUN uv sync --locked --no-dev --no-install-project

COPY src ./src
COPY dbt ./dbt
COPY dashboard ./dashboard
COPY data/sample ./data/sample
COPY docker/entrypoint.sh /usr/local/bin/sd311-entrypoint
RUN uv sync --locked --no-dev

# Non-root runtime user. Named volumes inherit these directories' ownership on first mount.
RUN useradd --create-home --uid 10001 app \
    && mkdir -p /app/data/warehouse /app/data/raw "${DAGSTER_HOME}" \
    && chown -R app:app /app/data /app/dbt "${DAGSTER_HOME}"
USER app

# Dagster loads the dbt manifest at import time, so parse the project into the image.
RUN dbt parse --project-dir dbt --profiles-dir dbt --quiet

EXPOSE 3000 8501

ENTRYPOINT ["sd311-entrypoint"]
# Default role: run the whole pipeline once (ingest -> pandera gate -> dbt build).
CMD ["pipeline"]
