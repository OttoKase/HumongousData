# rico/traceability.py
import hashlib
import os
import uuid
from datetime import datetime, timezone

import psycopg2


def get_conn():
    return psycopg2.connect(os.environ["POSTGRES_DSN"])


def start_run(dag_run_id: str, limit_param: int) -> str:
    run_id = str(uuid.uuid4())
    sql = """
        INSERT INTO pipeline_runs (
            run_id, dag_run_id, started_at, status, limit_param, git_sha
        ) VALUES (%s, %s, %s, 'running', %s, %s)
    """
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, (
                run_id,
                dag_run_id,
                datetime.now(timezone.utc),
                limit_param,
                os.environ.get("GIT_SHA", "unknown"),
            ))
    return run_id


def finish_run(run_id: str, status: str, model_versions: dict = None) -> None:
    model_versions = model_versions or {}
    # FIX: fall back to OLLAMA_MODEL env var so llm_model is never NULL when the
    # column was simply not passed by the caller (the old finalize task omitted it).
    llm_model = model_versions.get("llm") or os.environ.get("OLLAMA_MODEL")
    sql = """
        UPDATE pipeline_runs
        SET ended_at       = %s,
            status         = %s,
            clip_version   = %s,
            sbert_version  = %s,
            llm_model      = %s,
            prompt_version = %s
        WHERE run_id = %s
    """
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, (
                datetime.now(timezone.utc),
                status,
                model_versions.get("clip"),
                model_versions.get("sbert"),
                llm_model,
                model_versions.get("prompt"),
                run_id,
            ))


def fingerprint(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def get_run_metrics(run_id: str) -> dict:
    sql = """
        SELECT metric_name, metric_value
        FROM pipeline_metrics
        WHERE run_id = %s
    """
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, (run_id,))
            return {row[0]: row[1] for row in cur.fetchall()}