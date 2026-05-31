# dags/rico_dag.py
import json
import os
import time
from datetime import datetime, timezone

from airflow.decorators import dag, task
from airflow.models.param import Param
from airflow.exceptions import AirflowFailException
import sys
sys.path.insert(0, "/opt/airflow")

from rico import (
    audit,
    embed_image,
    embed_text,
    extract,
    ingest,
    load,
    notifications,
    observability,
    parse,
    eval as rico_eval,
)
from rico.traceability import finish_run, start_run, get_run_metrics, get_conn


def _on_dag_failure(context):
    """
    DAG-level failure callback — fires when any task fails outside the audit path
    (e.g. embed, parse, ingest). Sends a 'run failed' Slack message so operators
    are notified even when finalize() never runs.
    """
    run_meta = context["dag_run"].conf or {}
    run_id   = run_meta.get("run_id", "unknown")
    task_id  = context["task_instance"].task_id
    try:
        finish_run(run_id=run_id, status="failed")
    except Exception:
        pass  # best-effort; don't mask the original failure
    notifications.notify_run_finished(
        run_id=run_id,
        status="failed",
        total_duration_s=0.0,
        summary={"failed_task": task_id},
    )


@dag(
    dag_id="rico_pipeline",
    schedule=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    params={"limit": Param(5, type="integer", description="Number of screens to process")},
    tags=["rico"],
    on_failure_callback=_on_dag_failure,
)
def rico_pipeline():

    @task
    def init_run(**context) -> dict:
        limit      = context["params"]["limit"]
        dag_run_id = context["run_id"]
        trigger    = context["dag_run"].run_type

        run_id = start_run(dag_run_id=dag_run_id, limit_param=limit)
        notifications.notify_run_started(run_id=run_id, limit=limit, trigger=trigger)

        return {"run_id": run_id, "limit": limit, "started_at": time.time()}

    @task
    def ingest_task(run_meta: dict) -> dict:
        t0         = time.time()
        screen_ids = ingest.run(run_id=run_meta["run_id"], limit=run_meta["limit"])
        return {**run_meta, "screen_ids": screen_ids, "dur_ingest": time.time() - t0}

    @task
    def parse_task(run_meta: dict) -> dict:
        t0 = time.time()
        parse.run(run_id=run_meta["run_id"], screen_ids=run_meta["screen_ids"])
        return {**run_meta, "dur_parse": time.time() - t0}

    @task
    def embed_image_task(run_meta: dict) -> dict:
        t0           = time.time()
        clip_version = embed_image.run(
            run_id=run_meta["run_id"],
            screen_ids=run_meta["screen_ids"],
        )
        return {**run_meta, "clip_version": clip_version, "dur_embed_image": time.time() - t0}

    @task
    def embed_text_task(run_meta: dict) -> dict:
        t0            = time.time()
        sbert_version = embed_text.run(
            run_id=run_meta["run_id"],
            screen_ids=run_meta["screen_ids"],
        )
        return {**run_meta, "sbert_version": sbert_version, "dur_embed_text": time.time() - t0}

    @task
    def extract_task(run_meta: dict) -> dict:
        t0             = time.time()
        prompt_version = extract.run(
            run_id=run_meta["run_id"],
            screen_ids=run_meta["screen_ids"],
        )
        return {**run_meta, "prompt_version": prompt_version, "dur_extract": time.time() - t0}

    @task
    def load_task(embed_img: dict, embed_txt: dict, ext: dict) -> dict:
        t0       = time.time()
        run_meta = {**embed_img, **embed_txt, **ext}
        load.run(run_id=run_meta["run_id"], screen_ids=run_meta["screen_ids"])
        return {**run_meta, "dur_load": time.time() - t0}

    @task
    def audit_task(run_meta: dict) -> dict:
        t0 = time.time()
        try:
            audit.run(run_id=run_meta["run_id"])
        except AirflowFailException:
            # FIX: read the actual audit details from audit_results so the Slack
            # message contains the real duplicate keys, not just the exception string.
            audit_details = _fetch_audit_details(run_meta["run_id"])
            notifications.notify_audit_failed(
                run_id=run_meta["run_id"],
                details=audit_details,
            )
            finish_run(run_id=run_meta["run_id"], status="paused-by-audit")
            raise  # re-raise so Airflow marks the task Failed and skips eval
        return {**run_meta, "dur_audit": time.time() - t0}

    @task
    def eval_task(run_meta: dict) -> dict:
        t0 = time.time()
        rico_eval.run(run_id=run_meta["run_id"])
        return {**run_meta, "dur_eval": time.time() - t0}

    @task
    def observability_task(run_meta: dict) -> dict:
        task_durations = {
            k.replace("dur_", ""): v
            for k, v in run_meta.items()
            if k.startswith("dur_")
        }
        observability.run(run_id=run_meta["run_id"], task_durations=task_durations)
        return run_meta

    @task
    def finalize(run_meta: dict) -> None:
        # FIX: pass llm_model explicitly so pipeline_runs.llm_model is never NULL
        model_versions = {
            "clip":   run_meta.get("clip_version"),
            "sbert":  run_meta.get("sbert_version"),
            "prompt": run_meta.get("prompt_version"),
            "llm":    os.environ.get("OLLAMA_MODEL"),
        }
        finish_run(
            run_id=run_meta["run_id"],
            status="succeeded",
            model_versions=model_versions,
        )
        total   = time.time() - run_meta["started_at"]
        metrics = get_run_metrics(run_meta["run_id"])
        notifications.notify_run_finished(
            run_id=run_meta["run_id"],
            status="succeeded",
            total_duration_s=total,
            summary={
                "metadata_row_count": metrics.get("metadata.row_count"),
                "pct_extracted":      metrics.get("metadata.pct_extracted"),
                "pct_high_conf":      metrics.get("metadata.pct_high_conf"),
                "pct_review_queue":   metrics.get("metadata.pct_review_queue"),
            },
        )

    # --- DAG wiring ---
    run_meta = init_run()
    ingested = ingest_task(run_meta)
    parsed   = parse_task(ingested)

    img_meta = embed_image_task(parsed)
    txt_meta = embed_text_task(parsed)
    ext_meta = extract_task(parsed)

    loaded   = load_task(img_meta, txt_meta, ext_meta)
    audited  = audit_task(loaded)
    evalled  = eval_task(audited)
    observed = observability_task(evalled)
    finalize(observed)


def _fetch_audit_details(run_id: str) -> dict:
    """
    Read the most recent audit_results row for this run and return its details
    dict. Used so notify_audit_failed receives real duplicate keys rather than
    the stringified exception message.
    """
    sql = """
        SELECT details
        FROM audit_results
        WHERE run_id = %s
        ORDER BY created_at DESC
        LIMIT 1
    """
    try:
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(sql, (run_id,))
                row = cur.fetchone()
                if row and row[0]:
                    return row[0]  # psycopg2 returns JSONB as a Python dict
    except Exception as e:
        print(f"run_id={run_id} WARNING could not fetch audit details: {e}")
    return {}


rico_pipeline()