# tests/test_audit.py
"""
Integration test for the audit circuit-breaker.

Requirements:
  - A running Postgres instance reachable via POSTGRES_DSN.
  - The schema from migrations/001_init.sql must already be applied.

Run with:
  POSTGRES_DSN=postgresql://rico:rico@localhost:5432/rico pytest tests/test_audit.py -v
"""
import json
import os
import uuid

import psycopg2
import pytest

from rico.audit import run as audit_run


# ── helpers ────────────────────────────────────────────────────────────────────

def _conn():
    return psycopg2.connect(os.environ["POSTGRES_DSN"])


def _seed_run(cur, run_id: str) -> None:
    cur.execute(
        """
        INSERT INTO pipeline_runs (run_id, dag_run_id, started_at, status, limit_param)
        VALUES (%s, %s, NOW(), 'running', 1)
        """,
        (run_id, f"test-dag-{run_id}"),
    )


def _seed_screen(cur, run_id: str, screen_id: int) -> None:
    cur.execute(
        """
        INSERT INTO screens_metadata
            (screen_id, png_path, hierarchy_json_path, run_id, source_fingerprint)
        VALUES (%s, %s, %s, %s, %s)
        ON CONFLICT (screen_id) DO UPDATE SET run_id = EXCLUDED.run_id
        """,
        (screen_id, f"screens/{screen_id}.png", f"screens/{screen_id}.json",
         run_id, "deadbeef"),
    )


def _seed_embedding(cur, run_id: str, screen_id: int, kind: str = "image") -> None:
    cur.execute(
        """
        INSERT INTO screens_embeddings
            (screen_id, model_name, model_version, embedding_kind, vector, run_id, source_fingerprint)
        VALUES (%s, 'test-model', 'test-model-v1', %s, %s::vector, %s, 'deadbeef')
        ON CONFLICT (screen_id, model_name, model_version, embedding_kind)
            DO UPDATE SET run_id = EXCLUDED.run_id
        """,
        (screen_id, kind, "[0.1, 0.2, 0.3]", run_id),
    )


def _cleanup(cur, run_id: str) -> None:
    cur.execute("DELETE FROM audit_results      WHERE run_id = %s", (run_id,))
    cur.execute("DELETE FROM screens_embeddings WHERE run_id = %s", (run_id,))
    cur.execute("DELETE FROM screens_metadata   WHERE run_id = %s", (run_id,))
    cur.execute("DELETE FROM pipeline_runs      WHERE run_id = %s", (run_id,))


# ── fixtures ───────────────────────────────────────────────────────────────────

@pytest.fixture()
def clean_run():
    """One run, one screen, one embedding — no cross-run overlap."""
    run_id    = str(uuid.uuid4())
    screen_id = 99901
    with _conn() as conn:
        with conn.cursor() as cur:
            _seed_run(cur, run_id)
            _seed_screen(cur, run_id, screen_id)
            _seed_embedding(cur, run_id, screen_id)
        conn.commit()
    yield run_id, screen_id
    with _conn() as conn:
        with conn.cursor() as cur:
            _cleanup(cur, run_id)
            # screen_id may have been reassigned to another run_id by the duplicate
            # fixture; clean it up fully
            cur.execute("DELETE FROM screens_embeddings WHERE screen_id = %s", (screen_id,))
            cur.execute("DELETE FROM screens_metadata   WHERE screen_id = %s", (screen_id,))
        conn.commit()


@pytest.fixture()
def cross_run_metadata_duplicate():
    """
    Two runs both claim the same screen_id in screens_metadata.
    This is the exact condition _check_metadata_duplicates detects:
    COUNT(DISTINCT run_id) > 1 for the same screen_id.
    """
    run_id_a  = str(uuid.uuid4())
    run_id_b  = str(uuid.uuid4())
    screen_id = 99902
    with _conn() as conn:
        with conn.cursor() as cur:
            _seed_run(cur, run_id_a)
            _seed_run(cur, run_id_b)
            # Seed screen under run_a first
            _seed_screen(cur, run_id_a, screen_id)
            # Then force the same screen_id to also appear under run_b
            # by directly updating — bypasses the ON CONFLICT upsert
            cur.execute(
                """
                INSERT INTO screens_metadata
                    (screen_id, png_path, hierarchy_json_path, run_id, source_fingerprint)
                VALUES (%s, %s, %s, %s, 'deadbeef')
                ON CONFLICT (screen_id) DO UPDATE SET run_id = EXCLUDED.run_id
                """,
                (screen_id, f"screens/{screen_id}.png",
                 f"screens/{screen_id}.json", run_id_b),
            )
        conn.commit()
    yield run_id_b, screen_id
    with _conn() as conn:
        with conn.cursor() as cur:
            cur.execute("DELETE FROM audit_results    WHERE run_id IN (%s, %s)", (run_id_a, run_id_b))
            cur.execute("DELETE FROM screens_metadata WHERE screen_id = %s", (screen_id,))
            cur.execute("DELETE FROM pipeline_runs    WHERE run_id IN (%s, %s)", (run_id_a, run_id_b))
        conn.commit()


@pytest.fixture()
def cross_run_embedding_duplicate():
    """
    Same screen_id has embedding rows attributed to two different run_ids.
    This is what _check_embedding_duplicates detects.
    """
    run_id_a  = str(uuid.uuid4())
    run_id_b  = str(uuid.uuid4())
    screen_id = 99903
    with _conn() as conn:
        with conn.cursor() as cur:
            _seed_run(cur, run_id_a)
            _seed_run(cur, run_id_b)
            _seed_screen(cur, run_id_b, screen_id)
            # Embedding under run_a
            _seed_embedding(cur, run_id_a, screen_id, kind="image")
            # Force the same embedding to also be attributed to run_b
            cur.execute(
                """
                UPDATE screens_embeddings
                SET run_id = %s
                WHERE screen_id = %s AND embedding_kind = 'image'
                """,
                (run_id_b, screen_id),
            )
            # Re-insert under run_a so both run_ids exist for the same screen+kind
            cur.execute(
                """
                INSERT INTO screens_embeddings
                    (screen_id, model_name, model_version, embedding_kind, vector, run_id, source_fingerprint)
                VALUES (%s, 'test-model', 'test-model-v2', 'image', '[0.1,0.2,0.3]'::vector, %s, 'deadbeef')
                """,
                (screen_id, run_id_a),
            )
        conn.commit()
    yield run_id_b, screen_id
    with _conn() as conn:
        with conn.cursor() as cur:
            cur.execute("DELETE FROM audit_results      WHERE run_id IN (%s, %s)", (run_id_a, run_id_b))
            cur.execute("DELETE FROM screens_embeddings WHERE screen_id = %s", (screen_id,))
            cur.execute("DELETE FROM screens_metadata   WHERE screen_id = %s", (screen_id,))
            cur.execute("DELETE FROM pipeline_runs      WHERE run_id IN (%s, %s)", (run_id_a, run_id_b))
        conn.commit()


# ── tests ──────────────────────────────────────────────────────────────────────

def test_audit_passes_on_clean_data(clean_run):
    """A run with no cross-run overlap must pass without raising."""
    run_id, _ = clean_run
    audit_run(run_id=run_id)  # must not raise

    with _conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT passed, details FROM audit_results WHERE run_id = %s",
                (run_id,)
            )
            row = cur.fetchone()

    assert row is not None
    passed, details = row
    assert passed is True
    assert details["metadata_duplicates"] == []
    assert details["embedding_duplicates"] == []


def test_audit_fails_on_cross_run_metadata_duplicate(cross_run_metadata_duplicate):
    """
    A screen_id claimed by two different run_ids in screens_metadata
    must cause the audit to raise AirflowFailException.
    """
    run_id, screen_id = cross_run_metadata_duplicate

    with pytest.raises(AirflowFailException):
        audit_run(run_id=run_id)

    with _conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT passed, details FROM audit_results WHERE run_id = %s",
                (run_id,)
            )
            row = cur.fetchone()

    assert row is not None
    passed, details = row
    assert passed is False
    assert screen_id in details["metadata_duplicates"]


def test_audit_fails_on_cross_run_embedding_duplicate(cross_run_embedding_duplicate):
    """
    A (screen_id, embedding_kind) attributed to two run_ids must fail the audit.
    """
    run_id, screen_id = cross_run_embedding_duplicate

    with pytest.raises(AirflowFailException):
        audit_run(run_id=run_id)

    with _conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT passed, details FROM audit_results WHERE run_id = %s",
                (run_id,)
            )
            row = cur.fetchone()

    assert row is not None
    passed, details = row
    assert passed is False
    dupes = details["embedding_duplicates"]
    assert any(d["screen_id"] == screen_id for d in dupes)


def test_audit_result_committed_before_raise(cross_run_metadata_duplicate):
    """
    The audit_results row must be committed before the exception propagates
    so the DAG's except-block can read it for the Slack notification.
    """
    run_id, _ = cross_run_metadata_duplicate
    with pytest.raises(AirflowFailException):
        audit_run(run_id=run_id)

    with _conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT COUNT(*) FROM audit_results WHERE run_id = %s",
                (run_id,)
            )
            count = cur.fetchone()[0]

    assert count == 1