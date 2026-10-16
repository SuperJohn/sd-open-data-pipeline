"""Query helpers for the Streamlit dashboard.

Pure DuckDB reads over the dbt marts so the map of open requests and
per-service SLA trends can be unit-tested without Streamlit.
"""

from __future__ import annotations

from collections.abc import Sequence
from pathlib import Path

import duckdb
import pandas as pd

DEFAULT_MAP_SAMPLE = 5000
DEFAULT_SLA_WEEKS = 26
DEFAULT_TOP_SERVICES = 8


def service_filter_sql(
    services: Sequence[str] | None,
    *,
    column: str = "service_name",
) -> str:
    """Return a SQL WHERE/AND fragment for an optional service multiselect.

    Empty or None means no filter. Values are single-quote escaped for DuckDB.
    """
    if not services:
        return ""
    escaped = ",".join("'" + s.replace("'", "''") + "'" for s in services)
    return f"{column} in ({escaped})"


def _combine_where(*clauses: str) -> str:
    parts = [c.strip() for c in clauses if c and c.strip()]
    if not parts:
        return ""
    return "where " + " and ".join(f"({p})" for p in parts)


def read_df(db_path: Path | str, sql: str) -> pd.DataFrame:
    """Run *sql* against the warehouse and return a DataFrame."""
    with duckdb.connect(str(db_path), read_only=True) as con:
        return con.execute(sql).df()


def open_requests_map(
    db_path: Path | str,
    *,
    services: Sequence[str] | None = None,
    sample: int = DEFAULT_MAP_SAMPLE,
) -> pd.DataFrame:
    """Lat/lon points for open requests with valid geocodes.

    Returns columns ``lat``, ``lon``, ``service_name``, ``open_age_days``,
    ``council_district``. Sampled when the open backlog exceeds *sample*.
    """
    if sample < 1:
        raise ValueError("sample must be >= 1")
    svc = service_filter_sql(services)
    where = _combine_where("is_open", "has_valid_geo", svc)
    sql = f"""
        select
            latitude as lat,
            longitude as lon,
            service_name,
            open_age_days,
            council_district
        from marts.fct_requests
        {where}
        using sample {int(sample)} rows
    """
    return read_df(db_path, sql)


def service_sla_weekly(
    db_path: Path | str,
    *,
    services: Sequence[str] | None = None,
    weeks: int = DEFAULT_SLA_WEEKS,
    top_n: int = DEFAULT_TOP_SERVICES,
) -> pd.DataFrame:
    """Weekly SLA attainment (%) by service for closed requests.

    SLA is ``closed_within_sla`` among requests whose ``closed_date`` falls in
    that week. When *services* is empty, the *top_n* services by closed volume
    in the window are shown (drilldown default).

    Returns columns ``week``, ``service_name``, ``sla_pct``, ``closed_count``.
    """
    if weeks < 1:
        raise ValueError("weeks must be >= 1")
    if top_n < 1:
        raise ValueError("top_n must be >= 1")

    svc = service_filter_sql(services)
    # Cap the window to complete weeks ending before the latest closed week.
    window = f"""
        closed_at is not null
        and closed_date >= (
            select date_trunc('week', max(closed_date))
                   - interval '{int(weeks)}' week
            from marts.fct_requests
            where closed_at is not null
        )
        and closed_date < date_trunc(
            'week',
            (select max(closed_date) from marts.fct_requests where closed_at is not null)
        ) + interval '1' week
    """

    if svc:
        scope = _combine_where(window, svc)
        sql = f"""
            select
                date_trunc('week', closed_date)::date as week,
                service_name,
                round(avg(closed_within_sla::int) * 100, 1) as sla_pct,
                count(*)::bigint as closed_count
            from marts.fct_requests
            {scope}
            group by 1, 2
            order by 1, 2
        """
    else:
        # Restrict to highest-volume services in the window for a readable chart.
        sql = f"""
            with closed as (
                select *
                from marts.fct_requests
                {_combine_where(window)}
            ),
            top as (
                select service_name
                from closed
                where service_name is not null and service_name != 'Unknown'
                group by 1
                order by count(*) desc
                limit {int(top_n)}
            )
            select
                date_trunc('week', c.closed_date)::date as week,
                c.service_name,
                round(avg(c.closed_within_sla::int) * 100, 1) as sla_pct,
                count(*)::bigint as closed_count
            from closed c
            inner join top t using (service_name)
            group by 1, 2
            order by 1, 2
        """
    return read_df(db_path, sql)


def list_services(db_path: Path | str) -> list[str]:
    """Service names ordered by total volume (for the sidebar multiselect)."""
    df = read_df(
        db_path,
        "select service_name from marts.mart_service_performance order by total_requests desc",
    )
    return df["service_name"].tolist()
