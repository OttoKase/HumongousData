# rico/eval.py

import os

import psycopg2
from pgvector.psycopg2 import register_vector
from sentence_transformers import SentenceTransformer

from rico.embed_text import SBERT_MODEL_VERSION

TEXT_NEAREST_SQL = """
    SELECT screen_id
    FROM screens_embeddings
    WHERE embedding_kind = 'text'
    ORDER BY vector <-> %s::vector
    LIMIT %s
"""

FETCH_TEXTS_SQL = """
    SELECT m.screen_id, e.vector
    FROM screens_embeddings e
    JOIN screens_metadata m USING (screen_id)
    WHERE e.embedding_kind = 'text'
      AND m.run_id = %s
    ORDER BY m.screen_id
"""

INSERT_EVAL_SQL = """
    INSERT INTO screens_eval (embedding_model_version, n_queries, recall_at_5, run_id)
    VALUES (%s, %s, %s, %s)
"""


def _recall_at_k(
    queries: list[tuple[int, str]],
    k: int,
    sbert: SentenceTransformer,
    conn,
) -> tuple[float, list[tuple[int, list[int]]]]:
    detail = []
    hits   = 0
    with conn.cursor() as cur:
        for expected_id, query_text in queries:
            qvec = sbert.encode([query_text], normalize_embeddings=True).astype("float32")[0]
            cur.execute(TEXT_NEAREST_SQL, (qvec, k))
            top = [row[0] for row in cur.fetchall()]
            detail.append((expected_id, top))
            if expected_id in top:
                hits += 1
    return hits / len(queries) if queries else 0.0, detail


def run(run_id: str) -> None:
   
    sbert = SentenceTransformer(SBERT_MODEL_VERSION)

    with psycopg2.connect(os.environ["POSTGRES_DSN"]) as conn:
        register_vector(conn)

        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT screen_id
                FROM screens_metadata
                WHERE run_id = %s
                ORDER BY screen_id
                """,
                (run_id,)
            )
            screen_ids = [row[0] for row in cur.fetchall()]

        if len(screen_ids) < 2:
            print(f"run_id={run_id} stage=eval skipped — fewer than 2 screens")
            return

        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT screen_id, vector
                FROM screens_embeddings
                WHERE embedding_kind = 'text'
                  AND screen_id = ANY(%s)
                ORDER BY screen_id
                """,
                (screen_ids,)
            )
            rows = cur.fetchall()

        sid_to_vec = {row[0]: row[1] for row in rows}

        n = len(screen_ids)
        holdout_queries = []
        for idx, sid in enumerate(screen_ids):
            other_sid = screen_ids[(idx + 1) % n]
            if other_sid in sid_to_vec:
                holdout_queries.append((sid, other_sid))

        k = min(5, n)
        hits = 0
        detail = []
        with conn.cursor() as cur:
            for expected_id, query_sid in holdout_queries:
                qvec = sid_to_vec[query_sid]
                cur.execute(TEXT_NEAREST_SQL, (qvec, k))
                top = [row[0] for row in cur.fetchall()]
                detail.append((expected_id, top))
                if expected_id in top:
                    hits += 1

        recall = hits / len(holdout_queries) if holdout_queries else 0.0

        with conn.cursor() as cur:
            cur.execute(INSERT_EVAL_SQL, (
                SBERT_MODEL_VERSION,
                len(holdout_queries),
                recall,
                run_id,
            ))
        conn.commit()

    for expected, top in detail:
        hit = "✓" if expected in top else "✗"
        print(f"run_id={run_id} stage=eval {hit} expected={expected} top={top}")

    print(
        f"run_id={run_id} stage=eval complete "
        f"recall@{k}={recall:.2f} "
        f"n_queries={len(holdout_queries)} "
        f"model={SBERT_MODEL_VERSION}"
    )