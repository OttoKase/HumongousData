# rico/embed_image.py
import os
from io import BytesIO

import boto3
import numpy as np
import open_clip
import psycopg2
import torch
from PIL import Image
from pgvector.psycopg2 import register_vector

from rico.traceability import fingerprint

CLIP_ARCH       = "ViT-B-32"
CLIP_PRETRAINED = "laion2b_s34b_b79k"
CLIP_MODEL_NAME = "open-clip"
CLIP_MODEL_VERSION = f"open-clip-{CLIP_ARCH}-{CLIP_PRETRAINED.replace('_', '-')}"

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


def _load_model():
    model, _, preprocess = open_clip.create_model_and_transforms(
        CLIP_ARCH, pretrained=CLIP_PRETRAINED
    )
    model.eval()
    return model, preprocess


def run(run_id: str, screen_ids: list[int]) -> str:
    """
    Fetch PNGs from MinIO, embed with CLIP ViT-B-32,
    L2-normalise, insert into screens_embeddings.
    Returns model_version string for pipeline_runs.
    """
    s3     = _s3_client()
    bucket = os.environ["MINIO_BUCKET"]

    model, preprocess = _load_model()

    # Fetch and preprocess all PNGs
    batch      = []
    png_bytes_list = []
    for sid in screen_ids:
        blob = s3.get_object(Bucket=bucket, Key=f"screens/{sid}.png")["Body"].read()
        img  = Image.open(BytesIO(blob)).convert("RGB")
        batch.append(preprocess(img))
        png_bytes_list.append(blob)

    images_tensor = torch.stack(batch)

    # Single forward pass
    with torch.no_grad():
        vecs = model.encode_image(images_tensor)
        vecs = vecs / vecs.norm(dim=-1, keepdim=True)
    vecs_np = vecs.cpu().numpy().astype("float32")

    # Insert into Postgres
    with psycopg2.connect(os.environ["POSTGRES_DSN"]) as conn:
        register_vector(conn)
        with conn.cursor() as cur:
            for sid, vec, png_bytes in zip(screen_ids, vecs_np, png_bytes_list):
                fp = fingerprint(png_bytes)
                cur.execute(INSERT_EMB_SQL, (
                    sid,
                    CLIP_MODEL_NAME,
                    CLIP_MODEL_VERSION,
                    "image",
                    vec,
                    run_id,
                    fp,
                ))
                print(f"run_id={run_id} stage=embed_image screen_id={sid} dims={vec.shape[0]} fp={fp[:12]}")
        conn.commit()

    print(f"run_id={run_id} stage=embed_image complete screens={len(screen_ids)} model={CLIP_MODEL_VERSION}")
    return CLIP_MODEL_VERSION