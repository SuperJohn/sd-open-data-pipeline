"""Integration tests: run dbt on the sample and assert on the marts."""

import duckdb


def _q(db, sql):
    with duckdb.connect(str(db), read_only=True) as con:
        return con.execute(sql).fetchall()


def test_fact_has_one_row_per_request(built_warehouse):
    ((n, distinct),) = _q(
        built_warehouse,
        "select count(*), count(distinct service_request_id) from marts.fct_requests",
    )
    assert n == distinct == 4000


def test_open_requests_have_no_resolution_time(built_warehouse):
    ((bad,),) = _q(
        built_warehouse,
        "select count(*) from marts.fct_requests where is_open and days_to_close is not null",
    )
    assert bad == 0


def test_bad_geocodes_are_nulled_in_fact(built_warehouse):
    ((bad,),) = _q(
        built_warehouse,
        "select count(*) from marts.fct_requests where not has_valid_geo and latitude is not null",
    )
    assert bad == 0


def test_service_performance_sla_is_a_percentage(built_warehouse):
    rows = _q(
        built_warehouse,
        "select pct_closed_within_sla from marts.mart_service_performance "
        "where pct_closed_within_sla is not null",
    )
    assert rows and all(0 <= r[0] <= 100 for r in rows)


def test_daily_mart_reconciles(built_warehouse):
    ((total,),) = _q(built_warehouse, "select sum(requests_opened) from marts.mart_service_daily")
    assert total == 4000
