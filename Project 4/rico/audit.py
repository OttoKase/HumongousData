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
    cur.execute(
        """
        SELECT screen_id, COUNT(*) AS n
        FROM screens_metadata
        WHERE run_id = %s
        GROUP BY screen_id
        HAVING COUNT(*) > 1
        """,
        (run_id,)
    )
    return [row[0] for row in cur.fetchall()]


def _check_embedding_duplicates(cur, run_id: str) -> list[dict]:
    cur.execute(
        """
        SELECT screen_id, model_name, model_version, embedding_kind, COUNT(*) AS n
        FROM screens_embeddings
        WHERE run_id = %s
        GROUP BY screen_id, model_name, model_version, embedding_kind
        HAVING COUNT(*) > 1
        """,
        (run_id,)
    )
    return [
        {
            "screen_id":      row[0],
            "model_name":     row[1],
            "model_version":  row[2],
            "embedding_kind": row[3],
            "count":          row[4],
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