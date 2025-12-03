import os
import pytest
from sqlalchemy import create_engine, text


def _engine_or_skip():
    url = os.getenv("CLARIUM_SQLA_URL", "postgresql+psycopg://clarium:clarium@127.0.0.1:5433/clarium")
    try:
        eng = create_engine(url)
        with eng.connect() as conn:
            conn.execute(text("SELECT 1"))
        return eng
    except Exception:
        pytest.skip("pgwire server not available; set CLARIUM_SQLA_URL or start clarium_server with --pgwire")


def test_ann_order_by_hint_end_to_end():
    eng = _engine_or_skip()
    with eng.begin() as conn:
        conn.execute(text("DROP TABLE IF EXISTS py_docs"))
        conn.execute(text("CREATE TABLE py_docs"))
        # Insert simple rows with 3-dim embeddings stored as comma-separated strings
        conn.execute(text("INSERT INTO py_docs (id, body_embed) VALUES (1, '0.1,0,0'), (2, '0.2,0,0'), (3, '0.3,0,0')"))
        # Create vector index sidecar via DDL
        conn.execute(text("DROP VECTOR INDEX IF EXISTS idx_py_docs_body"))
        conn.execute(text("CREATE VECTOR INDEX idx_py_docs_body ON py_docs(body_embed) USING hnsw WITH (metric='l2', dim=3)"))
        # Query with ANN hint
        rs = conn.execute(text(
            "WITH q AS (SELECT to_vec('[0.25,0,0]') v) "
            "SELECT id FROM py_docs ORDER BY vec_l2(py_docs.body_embed, (SELECT v FROM q)) USING ANN LIMIT 2"
        ))
        rows = rs.fetchall()
        assert len(rows) == 2


def test_scalar_udfs_available():
    eng = _engine_or_skip()
    with eng.connect() as conn:
        v = conn.execute(text("SELECT to_vec('[1, 2, 3]') AS v")).scalar()
        assert v == "1,2,3"
