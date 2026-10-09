"""Streamlit dashboard over the dbt marts.  Run: `make dashboard`."""

from __future__ import annotations

import os
from pathlib import Path

import duckdb
import plotly.express as px
import streamlit as st

DB_PATH = Path(
    os.getenv(
        "SD311_DUCKDB_PATH",
        Path(__file__).resolve().parents[1] / "data" / "warehouse" / "sd311.duckdb",
    )
)

st.set_page_config(page_title="San Diego 311 — Get It Done", page_icon="🌴", layout="wide")


@st.cache_data(ttl=600)
def q(sql: str):
    with duckdb.connect(str(DB_PATH), read_only=True) as con:
        return con.execute(sql).df()


if not DB_PATH.exists():
    st.error(f"No warehouse at {DB_PATH}. Run `make run` first.")
    st.stop()

st.title("San Diego 311 — Get It Done requests")
st.caption(
    "Source: City of San Diego Open Data (data.sandiego.gov). "
    "Pipeline: DuckDB → dbt → Dagster. Marts refreshed daily."
)

services = q("select service_name from marts.mart_service_performance order by total_requests desc")
picked = st.sidebar.multiselect("Services", services["service_name"].tolist())
svc_filter = (
    "where service_name in (" + ",".join("'" + s.replace("'", "''") + "'" for s in picked) + ")"
    if picked
    else ""
)

kpi = q(
    f"""
    select count(*) as total,
           count(*) filter (where is_open) as open_backlog,
           median(days_to_close) as median_days,
           avg(case when closed_at is not null then closed_within_sla::int end) * 100 as sla_pct
    from marts.fct_requests {svc_filter}
    """
).iloc[0]
c1, c2, c3, c4 = st.columns(4)
c1.metric("Requests", f"{int(kpi.total):,}")
c2.metric("Open backlog", f"{int(kpi.open_backlog):,}")
c3.metric(
    "Median days to close", f"{kpi.median_days:.1f}" if kpi.median_days == kpi.median_days else "–"
)
c4.metric("Closed within 30 days", f"{kpi.sla_pct:.0f}%" if kpi.sla_pct == kpi.sla_pct else "–")

weekly = q(
    f"""
    select date_trunc('week', requested_date) as week, sum(requests_opened) as requests
    from marts.mart_service_daily {svc_filter}
    group by 1
    having week >= date_trunc('year', (select max(requested_date) from marts.fct_requests))
       and week < date_trunc('week', (select max(requested_date) from marts.fct_requests))
    order by 1
    """
)
st.subheader("Requests opened per week (this year)")
st.caption(
    "The source extracts cover requests closed this year plus everything still open, so earlier "
    "periods are under-counted; the trend is limited to the current year."
)
st.plotly_chart(px.area(weekly, x="week", y="requests"), width="stretch")

left, right = st.columns(2)
with left:
    st.subheader("Days to close — 15 highest-volume services")
    perf = q(
        f"""with top as (
                select * from marts.mart_service_performance {svc_filter}
                {"and" if svc_filter else "where"} service_name != 'Unknown'
                order by total_requests desc limit 15)
            select * from top order by median_days_to_close desc"""
    )
    st.plotly_chart(
        px.bar(
            perf,
            x="median_days_to_close",
            y="service_name",
            orientation="h",
            labels={"median_days_to_close": "median days to close", "service_name": ""},
            hover_data=["total_requests", "p90_days_to_close", "pct_closed_within_sla"],
        ),
        width="stretch",
    )
with right:
    st.subheader("Council district scorecard")
    st.dataframe(
        q("select * from marts.mart_district_performance order by council_district"),
        hide_index=True,
        width="stretch",
    )

st.subheader("Where requests come from")
pts = q(
    f"""select latitude as lat, longitude as lon from marts.fct_requests
        {svc_filter} {"and" if svc_filter else "where"} has_valid_geo using sample 5000 rows"""
)
st.map(pts, size=20)

with st.expander("Service performance table"):
    st.dataframe(
        q(
            "select * from marts.mart_service_performance "
            f"{svc_filter} order by total_requests desc"
        ),
        hide_index=True,
        width="stretch",
    )
