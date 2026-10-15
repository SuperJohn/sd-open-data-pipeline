"""Incremental dbt models: new partitions, status upserts, and --full-refresh."""

from __future__ import annotations

import os
import shutil
import subprocess
import sys
from pathlib import Path

import duckdb
import pytest

from sd311 import config
from sd311.districts import load_districts, resolve_districts_file
from sd311.ingest import load_to_duckdb, resolve_source_files


def _dbt(db: Path, target: Path, *args: str) -> None:
    dbt = shutil.which("dbt") or str(Path(sys.executable).parent / "dbt")
    env = {**os.environ, "SD311_DUCKDB_PATH": str(db)}
    proc = subprocess.run(
        [
            dbt,
            *args,
            "--project-dir",
            str(config.DBT_PROJECT_DIR),
            "--profiles-dir",
            str(config.DBT_PROJECT_DIR),
            "--target-path",
            str(target),
            "--quiet",
        ],
        env=env,
        capture_output=True,
        text=True,
        check=False,
    )
    assert proc.returncode == 0, proc.stdout[-4000:] + proc.stderr[-2000:]


def _q(db: Path, sql: str):
    with duckdb.connect(str(db), read_only=True) as con:
        return con.execute(sql).fetchall()


def _bootstrap(db: Path, files: list[Path]) -> None:
    load_to_duckdb(files, db, full_refresh=True)
    load_districts(resolve_districts_file("sample"), db)


@pytest.fixture()
def incremental_wh(tmp_path: Path) -> tuple[Path, Path]:
    """Warehouse built from open + closed 2024/2025 only (no 2026 yet)."""
    db = tmp_path / "inc.duckdb"
    target = tmp_path / "dbt_target"
    files = resolve_source_files("sample", years=[2024, 2025], include_open=True)
    _bootstrap(db, files)
    _dbt(db, target, "build")
    return db, target


def test_incremental_materializations_are_tables(incremental_wh):
    db, _ = incremental_wh
    rows = _q(
        db,
        """
        select table_schema, table_name, table_type
        from information_schema.tables
        where table_schema in ('staging', 'marts')
          and table_name in ('stg_get_it_done__requests', 'fct_requests')
        order by 1, 2
        """,
    )
    assert rows == [
        ("marts", "fct_requests", "BASE TABLE"),
        ("staging", "stg_get_it_done__requests", "BASE TABLE"),
    ]


def test_incremental_picks_up_new_year_partition(incremental_wh):
    db, target = incremental_wh
    ((before,),) = _q(db, "select count(*) from marts.fct_requests")
    assert before == 3100  # 1500 open + 800 + 800

    # Load closed-2026 only; ingest stamps a fresh _loaded_at for that partition.
    load_to_duckdb(
        resolve_source_files("sample", years=[2026], include_open=False),
        db,
    )
    _dbt(db, target, "run", "--select", "stg_get_it_done__requests+")

    ((after,),) = _q(db, "select count(*) from marts.fct_requests")
    assert after == 4000
    ((distinct,),) = _q(db, "select count(distinct service_request_id) from marts.fct_requests")
    assert distinct == 4000

    # A known closed-2026 fixture row should now be in the fact.
    with open(config.SAMPLE_DIR / "get_it_done_closed_2026_sample.csv") as fh:
        next(fh)
        sample_id = next(fh).split(",", 1)[0]
    ((found,),) = _q(
        db,
        f"select count(*) from marts.fct_requests where service_request_id = '{sample_id}'",
    )
    assert found == 1


def test_incremental_upserts_open_to_closed(tmp_path: Path):
    db = tmp_path / "upsert.duckdb"
    target = tmp_path / "dbt_target"
    _bootstrap(db, resolve_source_files("sample"))
    _dbt(db, target, "build")

    # Pick an open request and land a closed twin with a newer _loaded_at.
    with duckdb.connect(str(db)) as con:
        row = con.execute(
            """
            select service_request_id, date_requested, service_name, lat, lng,
                   street_address, zipcode, council_district, case_origin
            from raw.get_it_done_requests
            where status = 'In Process'
            limit 1
            """
        ).fetchone()
        assert row is not None
        rid = row[0]
        con.execute(
            """
            insert into raw.get_it_done_requests (
                service_request_id, service_request_parent_id, sap_notification_number,
                date_requested, case_age_days, case_record_type, service_name,
                service_name_detail, date_closed, status, lat, lng, street_address,
                zipcode, council_district, comm_plan_code, comm_plan_name, park_name,
                case_origin, referred, iamfloc, floc,
                _source_file, _source_year, _loaded_at
            )
            select
                service_request_id, service_request_parent_id, sap_notification_number,
                date_requested, case_age_days, case_record_type, service_name,
                service_name_detail,
                '2026-10-14 12:00:00' as date_closed,
                'Closed' as status,
                lat, lng, street_address, zipcode, council_district, comm_plan_code,
                comm_plan_name, park_name, case_origin, referred, iamfloc, floc,
                'synthetic_closed_upsert.csv' as _source_file,
                2026 as _source_year,
                _loaded_at + interval '1' hour as _loaded_at
            from raw.get_it_done_requests
            where service_request_id = ? and status = 'In Process'
            limit 1
            """,
            [rid],
        )

    _dbt(db, target, "run", "--select", "stg_get_it_done__requests+")

    ((status, is_open, days),) = _q(
        db,
        f"""
        select status, is_open, days_to_close
        from marts.fct_requests
        where service_request_id = '{rid}'
        """,
    )
    assert status == "Closed"
    assert is_open is False
    assert days is not None and days >= 0

    ((n, distinct),) = _q(
        db,
        "select count(*), count(distinct service_request_id) from marts.fct_requests",
    )
    assert n == distinct == 4000


def test_full_refresh_rebuilds_equivalent_fact(tmp_path: Path):
    db = tmp_path / "fr.duckdb"
    target = tmp_path / "dbt_target"
    _bootstrap(db, resolve_source_files("sample"))
    _dbt(db, target, "build")

    # Second build is incremental (no-op on unchanged raw); then force full-refresh.
    _dbt(db, target, "run", "--select", "stg_get_it_done__requests fct_requests")
    _dbt(db, target, "build", "--full-refresh")

    ((n, distinct),) = _q(
        db,
        "select count(*), count(distinct service_request_id) from marts.fct_requests",
    )
    assert n == distinct == 4000
    ((daily,),) = _q(db, "select sum(requests_opened) from marts.mart_service_daily")
    assert daily == 4000
