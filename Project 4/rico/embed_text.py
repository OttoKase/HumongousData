# rico/embed_text.py
import os

import boto3
import numpy as np
import psycopg2
from pgvector.psycopg2 import register_vector
from sentence_transformers import SentenceTransformer

from rico.traceability import fingerprint

SBERT_MODEL_NAME    = "sentence-transformers"
SBERT_MODEL_VERSION = "sentence-transformers/all-MiniLM-L6-v2"

INSERT_EMB_SQL = """
    INSERT INTO screens_embeddings (
        screen_id, model_name, model_version, embedding_kind, vector,
        run_id, source_fingerprint
    ) VALUES (%s, %s, %s, %s, %s, %s, %s)
    ON CONFLICT (screen_id, model_name, model_version, embedding_kind) DO UPDATE SET
        vector             = EXCLUDED.vector,
        run_id             = EXCLUDED.run_id,
        source_fingerprint = EXCLUDED.source_fingerprint
"""


def _s3_client():
    return boto3.client(
        "s3",
        endpoint_url=os.environ["MINIO_ENDPOINT"],
        aws_access_key_id=os.environ["MINIO_ACCESS_KEY"],
        aws_secret_access_key=os.environ["MINIO_SECRET_KEY"],
        region_name="us-east-1",
    )


def run(run_id: str, screen_ids: list[int]) -> str:
    """
    Fetch text representations from MinIO, embed with SBERT all-MiniLM-L6-v2,
    insert into screens_embeddings with embedding_kind='text'.
    Returns model_version string for pipeline_runs.
    """
    s3     = _s3_client()
    bucket = os.environ["MINIO_BUCKET"]

    sbert = SentenceTransformer(SBERT_MODEL_VERSION)

    # Fetch text representations from MinIO
    texts      = []
    text_bytes_list = []
    for sid in screen_ids:
        raw = s3.get_object(Bucket=bucket, Key=f"screens/{sid}.txt")["Body"].read()
        texts.append(raw.decode("utf-8"))
        text_bytes_list.append(raw)

    # Single batched encode — normalize so L2 == cosine distance
    vecs_np = sbert.encode(texts, normalize_embeddings=True).astype("float32")

    # Insert into Postgres
    with psycopg2.connect(os.environ["POSTGRES_DSN"]) as conn:
        register_vector(conn)
        with conn.cursor() as cur:
            for sid, vec, text_bytes in zip(screen_ids, vecs_np, text_bytes_list):
                fp = fingerprint(text_bytes)
                cur.execute(INSERT_EMB_SQL, (
                    sid,
                    SBERT_MODEL_NAME,
                    SBERT_MODEL_VERSION,
                    "text",
                    vec,
                    run_id,
                    fp,
                ))
                print(f"run_id={run_id} stage=embed_text screen_id={sid} dims={vec.shape[0]} fp={fp[:12]}")
        conn.commit()

    print(f"run_id={run_id} stage=embed_text complete screens={len(screen_ids)} model={SBERT_MODEL_VERSION}")
    return SBERT_MODEL_VERSION