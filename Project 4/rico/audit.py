# rico/audit.py

import json
import os

import psycopg2
from airflow.exceptions import AirflowFailException


INSERT_AUDIT_SQL = """
    INSERT INTO audit_results (run_id, audit_name, passed, details)
    VALUES (%s, %s, %s, %s::jsonb)
"""


def _check_metadata_duplicates(cur, run_id: str) -> list[int]:
    """
    Checks for screen_ids that appear in MORE THAN ONE pipeline run.
    This catches the case where a screen was written by a previous run
    and then written again by this run without the upsert working correctly,
    leaving two pipeline_runs rows claiming the same screen_id.
    """
    cur.execute(
        """
        SELECT screen_id, COUNT(DISTINCT run_id) AS n_runs
        FROM screens_metadata
        WHERE screen_id IN (
            SELECT screen_id FROM screens_metadata WHERE run_id = %s
        )
        GROUP BY screen_id
        HAVING COUNT(DISTINCT run_id) > 1
        """,
        (run_id,)
    )
    return [row[0] for row in cur.fetchall()]


def _check_embedding_duplicates(cur, run_id: str) -> list[dict]:
    """
    Checks for (screen_id, embedding_kind) combinations that appear in MORE
    THAN ONE pipeline run. The primary key on screens_embeddings prevents
    true row-level duplication, but this catches the case where a screen has
    embeddings attributed to two different run_ids — meaning it was processed
    twice and the upsert didn't consolidate them under a single run.
    """
    cur.execute(
        """
        SELECT screen_id, embedding_kind, COUNT(DISTINCT run_id) AS n_runs
        FROM screens_embeddings
        WHERE screen_id IN (
            SELECT screen_id FROM screens_metadata WHERE run_id = %s
        )
        GROUP BY screen_id, embedding_kind
        HAVING COUNT(DISTINCT run_id) > 1
        """,
        (run_id,)
    )
    return [
        {
            "screen_id":      row[0],
            "embedding_kind": row[1],
            "n_runs":         row[2],
        }
        for row in cur.fetchall()
    ]


def run(run_id: str) -> None:
    with psycopg2.connect(os.environ["POSTGRES_DSN"]) as conn:
        with conn.cursor() as cur:

            meta_dupes = _check_metadata_duplicates(cur, run_id)
            emb_dupes  = _check_embedding_duplicates(cur, run_id)

            passed  = not meta_dupes and not emb_dupes
            details = {
                "metadata_duplicates":   meta_dupes,
                "embedding_duplicates":  emb_dupes,
            }

            cur.execute(INSERT_AUDIT_SQL, (
                run_id,
                "duplicate_detection",
                passed,
                json.dumps(details),
            ))
        conn.commit()

    if passed:
        print(f"run_id={run_id} stage=audit passed=True")
    else:
        print(f"run_id={run_id} stage=audit passed=False details={json.dumps(details)}")
        raise AirflowFailException(
            f"AUDIT FAILED — duplicates found. "
            f"metadata_dupes={meta_dupes} "
            f"embedding_dupes={emb_dupes}"
        )