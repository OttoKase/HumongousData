# rico/load.py
import os

import psycopg2
from pgvector.psycopg2 import register_vector


def run(run_id: str, screen_ids: list[int]) -> None:
    
    with psycopg2.connect(os.environ["POSTGRES_DSN"]) as conn:
        register_vector(conn)
        with conn.cursor() as cur:

            cur.execute(
                "SELECT COUNT(*) FROM screens_metadata WHERE screen_id = ANY(%s)",
                (screen_ids,)
            )
            meta_count = cur.fetchone()[0]

            cur.execute(
                "SELECT COUNT(*) FROM screens_embeddings WHERE screen_id = ANY(%s)",
                (screen_ids,)
            )
            emb_count = cur.fetchone()[0]

            cur.execute(
                """
                SELECT COUNT(*) FROM screens_metadata
                WHERE screen_id = ANY(%s) AND run_id = %s
                """,
                (screen_ids, run_id)
            )
            traced_count = cur.fetchone()[0]

            print(
                f"run_id={run_id} stage=load "
                f"screens={len(screen_ids)} "
                f"metadata_rows={meta_count} "
                f"embedding_rows={emb_count} "
                f"traced={traced_count}"
            )

            if meta_count != len(screen_ids):
                raise RuntimeError(
                    f"load: expected {len(screen_ids)} metadata rows, found {meta_count}"
                )
            if traced_count != len(screen_ids):
                raise RuntimeError(
                    f"load: {len(screen_ids) - traced_count} metadata rows missing run_id"
                )

        conn.commit()

    print(f"run_id={run_id} stage=load complete ok")