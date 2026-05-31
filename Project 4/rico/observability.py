# rico/observability.py

import os

import psycopg2

from rico.embed_image import CLIP_MODEL_VERSION
from rico.embed_text import SBERT_MODEL_VERSION

EXPECTED_DIMS = {
    "image": 512,   # CLIP ViT-B-32
    "text":  384,   # SBERT all-MiniLM-L6-v2
}

INSERT_METRIC_SQL = """
    INSERT INTO pipeline_metrics (run_id, metric_name, metric_value, metric_text)
    VALUES (%s, %s, %s, %s)
"""


def _record(cur, run_id: str, name: str, value: float = None, text: str = None) -> None:
    cur.execute(INSERT_METRIC_SQL, (run_id, name, value, text))


def run(run_id: str, task_durations: dict[str, float]) -> None:

    with psycopg2.connect(os.environ["POSTGRES_DSN"]) as conn:
        with conn.cursor() as cur:

            for task_name, duration in task_durations.items():
                _record(cur, run_id, f"duration_s.{task_name}", value=duration)

            total_duration = sum(task_durations.values())
            _record(cur, run_id, "duration_s.total", value=total_duration)

            cur.execute(
                """
                SELECT
                    COUNT(*)                                             AS row_count,
                    ROUND(100.0 * COUNT(extraction_payload) / COUNT(*), 2)
                                                                         AS pct_extracted,
                    ROUND(100.0 * COUNT(*) FILTER (WHERE confidence >= 0.5) / COUNT(*), 2)
                                                                         AS pct_high_conf
                FROM screens_metadata
                WHERE run_id = %s
                """,
                (run_id,)
            )
            row = cur.fetchone()
            meta_count, pct_extracted, pct_high_conf = row

            _record(cur, run_id, "metadata.row_count",      value=float(meta_count))
            _record(cur, run_id, "metadata.pct_extracted",  value=float(pct_extracted or 0))
            _record(cur, run_id, "metadata.pct_high_conf",  value=float(pct_high_conf or 0))

            cur.execute(
                "SELECT COUNT(*) FROM screens_review_queue WHERE run_id = %s",
                (run_id,)
            )
            review_count = cur.fetchone()[0]
            pct_review = round(100.0 * review_count / meta_count, 2) if meta_count else 0.0
            _record(cur, run_id, "metadata.pct_review_queue", value=pct_review)

            cur.execute(
                """
                SELECT
                    model_version,
                    embedding_kind,
                    COUNT(*)                                              AS row_count,
                    ROUND(AVG(vector_dims(vector))::numeric, 0)::int      AS avg_dims,
                    ROUND(100.0 * COUNT(*) FILTER (
                        WHERE (vector <#> vector) = 0
                    ) / COUNT(*), 2)                                      AS pct_zero_norm
                FROM screens_embeddings
                WHERE run_id = %s
                GROUP BY model_version, embedding_kind
                ORDER BY model_version, embedding_kind
                """,
                (run_id,)
            )
            emb_rows = cur.fetchall()

            for model_version, emb_kind, emb_count, avg_dims, pct_zero in emb_rows:
                prefix = f"embeddings.{emb_kind}"
                _record(cur, run_id, f"{prefix}.row_count",     value=float(emb_count))
                _record(cur, run_id, f"{prefix}.avg_dims",      value=float(avg_dims))
                _record(cur, run_id, f"{prefix}.pct_zero_norm", value=float(pct_zero or 0))
                _record(cur, run_id, f"{prefix}.model_version", text=model_version)

                expected = EXPECTED_DIMS.get(emb_kind)
                if expected and avg_dims and avg_dims != expected:
                    print(
                        f"run_id={run_id} WARNING unexpected vector dims "
                        f"model={model_version} kind={emb_kind} "
                        f"dims={avg_dims} expected={expected}"
                    )

            cur.execute(
                """
                SELECT
                    COUNT(DISTINCT app_package) AS distinct_packages,
                    COUNT(DISTINCT category)    AS distinct_categories
                FROM screens_metadata
                WHERE run_id = %s
                """,
                (run_id,)
            )
            distinct_packages, distinct_categories = cur.fetchone()
            _record(cur, run_id, "sanity.distinct_app_packages",  value=float(distinct_packages))
            _record(cur, run_id, "sanity.distinct_categories",    value=float(distinct_categories))

        conn.commit()

    print(
        f"\n{'─' * 60}\n"
        f"RUN SUMMARY  run_id={run_id}\n"
        f"{'─' * 60}\n"
        f"  duration          : {total_duration:.1f}s total\n"
        f"  metadata rows     : {meta_count}\n"
        f"  extracted         : {pct_extracted}%\n"
        f"  high confidence   : {pct_high_conf}%\n"
        f"  review queue      : {pct_review}% ({review_count} screens)\n"
        f"  distinct packages : {distinct_packages}\n"
        f"  distinct categories: {distinct_categories}\n"
        + "".join(
            f"  emb {r[1]:>5} rows={r[2]} dims={r[3]} zero_norm={r[4]}%\n"
            for r in emb_rows
        )
        + f"{'─' * 60}\n"
    )