"""Tests for Streamlit dashboard query helpers (open map + SLA trends)."""

from __future__ import annotations

import pytest

from sd311.dashboard import (
    list_services,
    open_requests_map,
    service_filter_sql,
    service_sla_weekly,
)


def test_service_filter_sql_empty():
    assert service_filter_sql(None) == ""
    assert service_filter_sql([]) == ""


def test_service_filter_sql_escapes_quotes():
    sql = service_filter_sql(["Pothole", "O'Brien Issue"])
    assert sql == "service_name in ('Pothole','O''Brien Issue')"


def test_open_requests_map_only_open_with_geo(built_warehouse):
    pts = open_requests_map(built_warehouse, sample=5000)
    assert {"lat", "lon", "service_name", "open_age_days", "council_district"} <= set(pts.columns)
    assert len(pts) > 0
    assert pts["lat"].notna().all()
    assert pts["lon"].notna().all()
    # Sample warehouse has ~1,480 open geocoded rows; map must not exceed that.
    assert len(pts) <= 1480


def test_open_requests_map_respects_service_filter(built_warehouse):
    services = list_services(built_warehouse)
    assert services
    # Pick the service with the largest open backlog so we get points.
    top = services[0]
    pts = open_requests_map(built_warehouse, services=[top], sample=5000)
    assert pts.empty or (pts["service_name"] == top).all()


def test_open_requests_map_rejects_bad_sample(built_warehouse):
    with pytest.raises(ValueError, match="sample"):
        open_requests_map(built_warehouse, sample=0)


def test_service_sla_weekly_shape_and_bounds(built_warehouse):
    trends = service_sla_weekly(built_warehouse, weeks=12, top_n=5)
    assert {"week", "service_name", "sla_pct", "closed_count"} <= set(trends.columns)
    assert len(trends) > 0
    assert trends["service_name"].nunique() <= 5
    assert trends["sla_pct"].between(0, 100).all()
    assert (trends["closed_count"] > 0).all()


def test_service_sla_weekly_drilldown_filters_service(built_warehouse):
    services = list_services(built_warehouse)
    picked = services[:2]
    trends = service_sla_weekly(built_warehouse, services=picked, weeks=26)
    assert set(trends["service_name"].unique()) <= set(picked)


def test_service_sla_weekly_rejects_bad_weeks(built_warehouse):
    with pytest.raises(ValueError, match="weeks"):
        service_sla_weekly(built_warehouse, weeks=0)


def test_list_services_nonempty(built_warehouse):
    names = list_services(built_warehouse)
    assert len(names) >= 5
    assert all(isinstance(n, str) and n for n in names)
