# rico/traceability.py
import os
import uuid
from datetime import datetime, timezone
import psycopg2


def get_conn():
    return psycopg2.connect(os.environ["POSTGRES_DSN"])


def start_run(dag_run_id: str, limit_param: int) -> str:
    """Insert a new pipeline_runs row and return the run_id."""
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
    """Update pipeline_runs row with final status, ended_at, and model versions."""
    model_versions = model_versions or {}
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
                model_versions.get("llm"),
                model_versions.get("prompt"),
                run_id,
            ))


def fingerprint(data: bytes) -> str:
    """SHA-256 hash of raw bytes — used as source_fingerprint."""
    import hashlib
    return hashlib.sha256(data).hexdigest()