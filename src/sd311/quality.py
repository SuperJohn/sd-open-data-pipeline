"""Pre-transform data quality gate on the raw landing table (pandera).

dbt tests cover the modeled layers; this gate catches problems *before* dbt
runs: a missing column, an empty extract, unparseable timestamps, or a load
that suddenly contains duplicate request ids.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path

import duckdb
import pandas as pd
import pandera.pandas as pa

from sd311 import config
from sd311.ingest import EXPECTED_COLUMNS

VALID_STATUSES = ["New", "In Process", "Closed", "Referred"]
MIN_ROWS = 100


def _parses_as_timestamp(s: pd.Series) -> pd.Series:
    return pd.to_datetime(s, errors="coerce").notna()


RAW_SCHEMA = pa.DataFrameSchema(
    {
        "service_request_id": pa.Column(str, nullable=False, unique=True),
        "date_requested": pa.Column(str, pa.Check(_parses_as_timestamp, element_wise=False)),
        "status": pa.Column(str, pa.Check.isin(VALID_STATUSES)),
        "service_name": pa.Column(str, nullable=True),
        "lat": pa.Column(str, nullable=True),
        "lng": pa.Column(str, nullable=True),
    },
    checks=[pa.Check(lambda df: len(df) >= MIN_ROWS, error=f"fewer than {MIN_ROWS} rows")],
    strict=False,
    coerce=False,
)


@dataclass
class QualityReport:
    passed: bool
    rows: int
    failures: list[str] = field(default_factory=list)


def validate_frame(df: pd.DataFrame) -> QualityReport:
    failures: list[str] = []
    missing = sorted(set(EXPECTED_COLUMNS) - set(df.columns))
    if missing:
        failures.append(f"missing columns: {missing}")
    try:
        RAW_SCHEMA.validate(df, lazy=True)
    except pa.errors.SchemaErrors as exc:
        summary = (
            exc.failure_cases.groupby(["column", "check"], dropna=False)
            .size()
            .reset_index(name="n")
        )
        for row in summary.itertuples():
            failures.append(f"{row.column}: {row.check} ({row.n} failing values)")
    return QualityReport(passed=not failures, rows=len(df), failures=failures)


def validate_raw_table(db_path: Path | None = None) -> QualityReport:
    db_path = Path(db_path or config.DUCKDB_PATH)
    with duckdb.connect(str(db_path), read_only=True) as con:
        df = con.execute(f"select * from {config.RAW_SCHEMA}.{config.RAW_TABLE}").df()
    df = df.astype({c: "object" for c in df.columns if c.startswith("_") is False})
    return validate_frame(df)
