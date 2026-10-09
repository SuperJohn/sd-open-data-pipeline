"""Raw CSV schema contracts: fail fast on added or removed columns.

DuckDB's ``union_by_name`` + selecting EXPECTED_COLUMNS used to silently drop
extra columns and leave missing ones as NULL. These checks catch both cases
before any row lands in the warehouse.
"""

from __future__ import annotations

import csv
from collections.abc import Sequence
from pathlib import Path

# Canonical Get It Done raw-extract column contract (order matches the city CSV).
EXPECTED_COLUMNS = [
    "service_request_id",
    "service_request_parent_id",
    "sap_notification_number",
    "date_requested",
    "case_age_days",
    "case_record_type",
    "service_name",
    "service_name_detail",
    "date_closed",
    "status",
    "lat",
    "lng",
    "street_address",
    "zipcode",
    "council_district",
    "comm_plan_code",
    "comm_plan_name",
    "park_name",
    "case_origin",
    "referred",
    "iamfloc",
    "floc",
]


class SchemaDriftError(ValueError):
    """Raised when a source CSV's headers do not match the expected contract."""


def check_headers(
    columns: Sequence[str],
    *,
    source: str,
    expected: Sequence[str] = EXPECTED_COLUMNS,
) -> None:
    """Compare *columns* to *expected*; raise SchemaDriftError on any drift.

    Order is ignored; only presence/absence matters. *source* is included in
    the error message so operators can see which file drifted.
    """
    expected_set = set(expected)
    actual_set = set(columns)
    missing = sorted(expected_set - actual_set)
    unexpected = sorted(actual_set - expected_set)
    if not missing and not unexpected:
        return

    parts: list[str] = []
    if missing:
        parts.append(f"missing columns: {missing}")
    if unexpected:
        parts.append(f"unexpected columns: {unexpected}")
    raise SchemaDriftError(f"schema drift in {source}: {'; '.join(parts)}")


def assert_csv_schema(
    path: Path | str,
    *,
    expected: Sequence[str] = EXPECTED_COLUMNS,
) -> None:
    """Read the CSV header of *path* and assert it matches *expected*."""
    path = Path(path)
    with path.open(newline="", encoding="utf-8") as fh:
        reader = csv.reader(fh)
        try:
            header = next(reader)
        except StopIteration as exc:
            raise SchemaDriftError(f"schema drift in {path.name}: empty file") from exc
    check_headers(header, source=path.name, expected=expected)
