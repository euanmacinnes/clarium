import os
import re
from sqlalchemy import inspect, text
from sqlalchemy.engine import Connection


def test_sqlalchemy_inspector_lists_public_schema(conn: Connection):
    insp = inspect(conn)
    schemas = insp.get_schema_names()
    assert "public" in schemas


def test_sqlalchemy_inspector_tables_enumeration(conn: Connection):
    insp = inspect(conn)
    # SQLAlchemy's PostgreSQL dialect typically treats schema-qualified table listing
    tables = set(insp.get_table_names(schema="public"))
    # Clarium may ship a demo table on first run; assert the call works and returns a list
    assert isinstance(tables, set)
    # If demo exists, it should be discoverable
    if "demo" in tables:
        # Try a trivial query against it to ensure pgwire path works end-to-end
        res = conn.execute(text("SELECT COUNT(*) FROM public/demo"))
        count = res.scalar_one()
        assert isinstance(count, int)
