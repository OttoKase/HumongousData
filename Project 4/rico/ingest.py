# rico/ingest.py
import itertools
import os
from io import BytesIO

import boto3
import psycopg2
from datasets import load_dataset

from rico.traceability import fingerprint

DATASET = "rootsautomation/RICO-Screen2Words"

INSERT_METADATA_SQL = """
    INSERT INTO screens_metadata (
        screen_id, app_package, category,
        png_path, hierarchy_json_path,
        run_id, source_fingerprint
    ) VALUES (%s, %s, %s, %s, %s, %s, %s)
    ON CONFLICT (screen_id) DO UPDATE SET
        run_id             = EXCLUDED.run_id,
        source_fingerprint = EXCLUDED.source_fingerprint,
        updated_at         = NOW()
"""


def _s3_client():
    return boto3.client(
        "s3",
        endpoint_url=os.environ["MINIO_ENDPOINT"],
        aws_access_key_id=os.environ["MINIO_ACCESS_KEY"],
        aws_secret_access_key=os.environ["MINIO_SECRET_KEY"],
        region_name="us-east-1",
    )


def run(run_id: str, limit: int) -> list[int]:
    """
    Stream `limit` screens from HuggingFace, PUT PNG + JSON to MinIO,
    INSERT stub rows into screens_metadata.
    Returns list of ingested screen_ids.
    """
    s3 = _s3_client()
    bucket = os.environ["MINIO_BUCKET"]

    ds = load_dataset(DATASET, split="train", streaming=True, trust_remote_code=True)

    ingested = []

    with psycopg2.connect(os.environ["POSTGRES_DSN"]) as conn:
        with conn.cursor() as cur:
            for row in itertools.islice(ds, limit * 10):
                if len(ingested) >= limit:
                    break

                sid = int(row["screenId"])
                png_key  = f"screens/{sid}.png"
                hier_key = f"screens/{sid}.json"

                # PNG bytes
                buf = BytesIO()
                row["image"].save(buf, format="PNG")
                png_bytes = buf.getvalue()

                # JSON bytes
                hier_bytes = row["view_hierarchy"].encode("utf-8")

                # PUT to MinIO
                s3.put_object(Bucket=bucket, Key=png_key,  Body=png_bytes)
                s3.put_object(Bucket=bucket, Key=hier_key, Body=hier_bytes)

                # source_fingerprint = SHA-256 of PNG bytes
                fp = fingerprint(png_bytes)

                cur.execute(INSERT_METADATA_SQL, (
                    sid,
                    row["app_package_name"],
                    row["category"],
                    png_key,
                    hier_key,
                    run_id,
                    fp,
                ))

                ingested.append(sid)
                print(f"run_id={run_id} stage=ingest screen_id={sid} png={len(png_bytes)}B fp={fp[:12]}")

        conn.commit()

    print(f"run_id={run_id} stage=ingest complete screens={len(ingested)}")
    return ingested