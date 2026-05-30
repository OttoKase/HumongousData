# rico/extract.py
import json
import os

import boto3
import psycopg2
import requests

from rico.traceability import fingerprint
import pathlib

PROMPT_VERSION = "v1"
PROMPT_TEMPLATE = pathlib.Path("prompts/extract_v1.txt").read_text()

INSERT_REVIEW_SQL = """
    INSERT INTO screens_review_queue (screen_id, reason, raw_output, run_id, source_fingerprint)
    VALUES (%s, %s, %s, %s, %s)
"""

UPDATE_EXTRACTION_SQL = """
    UPDATE screens_metadata
    SET extraction_payload = %s::jsonb,
        prompt_version     = %s,
        confidence         = %s,
        updated_at         = NOW()
    WHERE screen_id = %s
"""


def _s3_client():
    return boto3.client(
        "s3",
        endpoint_url=os.environ["MINIO_ENDPOINT"],
        aws_access_key_id=os.environ["MINIO_ACCESS_KEY"],
        aws_secret_access_key=os.environ["MINIO_SECRET_KEY"],
        region_name="us-east-1",
    )


def _call_ollama(text_rep: str) -> dict:
    prompt = PROMPT_TEMPLATE.replace("{hierarchy_text}", text_rep)
    response = requests.post(
        f"{os.environ['OLLAMA_URL']}/api/generate",
        json={"model": os.environ["OLLAMA_MODEL"], "prompt": prompt, "stream": False},
        timeout=120,
    )
    response.raise_for_status()
    raw = response.json()["response"]
    obj, _ = json.JSONDecoder().raw_decode(raw.strip())
    return obj


def run(run_id: str, screen_ids: list[int]) -> str:
    s3     = _s3_client()
    bucket = os.environ["MINIO_BUCKET"]

    ok = 0
    failed = 0

    with psycopg2.connect(os.environ["POSTGRES_DSN"]) as conn:
        with conn.cursor() as cur:
            for sid in screen_ids:
                try:
                    raw = s3.get_object(Bucket=bucket, Key=f"screens/{sid}.txt")["Body"].read()
                    text_rep = raw.decode("utf-8")
                except Exception as e:
                    reason = f"text_missing: {e}"
                    fp = fingerprint(str(sid).encode())
                    cur.execute(INSERT_REVIEW_SQL, (sid, reason, None, run_id, fp))
                    print(f"run_id={run_id} stage=extract screen_id={sid} reason={reason}")
                    failed += 1
                    continue

                try:
                    payload = _call_ollama(text_rep)
                except json.JSONDecodeError as e:
                    reason = f"llm_bad_json: {e}"
                    fp = fingerprint(text_rep.encode())
                    cur.execute(INSERT_REVIEW_SQL, (sid, reason, str(e), run_id, fp))
                    print(f"run_id={run_id} stage=extract screen_id={sid} reason={reason}")
                    failed += 1
                    continue
                except Exception as e:
                    reason = f"llm_failed: {e}"
                    fp = fingerprint(text_rep.encode())
                    cur.execute(INSERT_REVIEW_SQL, (sid, reason, str(e), run_id, fp))
                    print(f"run_id={run_id} stage=extract screen_id={sid} reason={reason}")
                    failed += 1
                    continue

                confidence = float(payload.get("confidence", 0.0))
                body = {k: v for k, v in payload.items() if k != "confidence"}
                cur.execute(UPDATE_EXTRACTION_SQL, (
                    json.dumps(body),
                    PROMPT_VERSION,
                    confidence,
                    sid,
                ))
                print(f"run_id={run_id} stage=extract screen_id={sid} conf={confidence:.2f} ok")
                ok += 1

        conn.commit()

    print(f"run_id={run_id} stage=extract complete screens={len(screen_ids)} ok={ok} failed={failed}")
    return PROMPT_VERSION