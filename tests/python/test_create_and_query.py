import uuid
import pytest
from sqlalchemy import text
from sqlalchemy.engine import Connection
from sqlalchemy.exc import DBAPIError, ProgrammingError, OperationalError


def _temp_table_name(prefix: str = "pytest_") -> str:
    return prefix + uuid.uuid4().hex[:12]


@pytest.mark.parametrize("with_schema", [True, False])
def test_create_insert_select(conn: Connection, with_schema: bool):
    tbl = _temp_table_name()
    fq = f"public/{tbl}" if with_schema else tbl

    # Try to create a simple table; Clarium may restrict DDL — mark xfail on clear unsupported cases
    try:
        conn.execute(text(f"CREATE TABLE {fq} (_time INT, value FLOAT)"))
    except (ProgrammingError, OperationalError, DBAPIError) as e:
        # If the backend does not support CREATE TABLE, xfail with message
        pytest.xfail(f"CREATE TABLE not supported by pgwire: {e}")

    # Insert a few rows
    rows = [
        {"t": 1_000, "v": 1.5},
        {"t": 2_000, "v": 2.5},
        {"t": 3_000, "v": 3.0},
    ]
    for r in rows:
        conn.execute(text(f"INSERT INTO {fq} (_time, value) VALUES (:t, :v)"), r)

    # Basic selects
    count = conn.execute(text(f"SELECT COUNT(*) FROM {fq}")).scalar_one()
    assert count == len(rows)

    # Aggregation
    res = conn.execute(text(f"SELECT MIN(_time), MAX(_time), AVG(value) FROM {fq}"))
    min_t, max_t, avg_v = res.one()
    assert min_t == rows[0]["t"]
    assert max_t == rows[-1]["t"]
    assert round(avg_v, 6) == round(sum(r["v"] for r in rows) / len(rows), 6)

    # Attempt cleanup; ignore errors if DROP not supported
    try:
        conn.execute(text(f"DROP TABLE {fq}"))
    except Exception:
        pass
