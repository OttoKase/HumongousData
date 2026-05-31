# rico/load.py
import os

import psycopg2
from airflow.exceptions import AirflowFailException
from pgvector.psycopg2 import register_vector


def run(run_id: str, screen_ids: list[int]) -> None:

    with psycopg2.connect(os.environ["POSTGRES_DSN"]) as conn:
        register_vector(conn)
        with conn.cursor() as cur:

            # --- metadata rows (global, by screen_id) ---
            cur.execute(
                "SELECT COUNT(*) FROM screens_metadata WHERE screen_id = ANY(%s)",
                (screen_ids,)
            )
            meta_count = cur.fetchone()[0]

            # --- metadata rows traced to THIS run ---
            cur.execute(
                """
                SELECT COUNT(*) FROM screens_metadata
                WHERE screen_id = ANY(%s) AND run_id = %s
                """,
                (screen_ids, run_id)
            )
            traced_meta = cur.fetchone()[0]

            cur.execute(
                """
                SELECT COUNT(*) FROM screens_embeddings
                WHERE screen_id = ANY(%s) AND run_id = %s
                """,
                (screen_ids, run_id)
            )
            emb_count = cur.fetchone()[0]

            # Each screen should have exactly 2 embedding rows: one 'image', one 'text'
            expected_emb = len(screen_ids) * 2

            print(
                f"run_id={run_id} stage=load "
                f"screens={len(screen_ids)} "
                f"metadata_rows={meta_count} "
                f"traced_meta={traced_meta} "
                f"embedding_rows_this_run={emb_count} "
                f"expected_emb={expected_emb}"
            )

            if meta_count != len(screen_ids):
                raise AirflowFailException(
                    f"load: expected {len(screen_ids)} metadata rows, found {meta_count}"
                )
            if traced_meta != len(screen_ids):
                raise AirflowFailException(
                    f"load: {len(screen_ids) - traced_meta} metadata rows missing run_id={run_id}"
                )
            if emb_count < expected_emb:
                raise AirflowFailException(
                    f"load: expected {expected_emb} embedding rows for run_id={run_id}, "
                    f"found {emb_count}. Some embed tasks may have silently failed."
                )

        conn.commit()

    print(f"run_id={run_id} stage=load complete ok")