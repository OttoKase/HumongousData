# dags/rico_dag.py
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
from rico.traceability import finish_run, start_run, get_run_metrics


@dag(
    dag_id="rico_pipeline",
    schedule=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    params={"limit": Param(5, type="integer", description="Number of screens to process")},
    tags=["rico"],
)
def rico_pipeline():

    @task
    def init_run(**context) -> dict:
        limit     = context["params"]["limit"]
        dag_run_id = context["run_id"]
        trigger   = context["dag_run"].run_type

        run_id = start_run(dag_run_id=dag_run_id, limit_param=limit)
        notifications.notify_run_started(run_id=run_id, limit=limit, trigger=trigger)

        return {"run_id": run_id, "limit": limit, "started_at": time.time()}

    @task
    def ingest_task(run_meta: dict) -> dict:
        t0 = time.time()
        screen_ids = ingest.run(run_id=run_meta["run_id"], limit=run_meta["limit"])
        return {**run_meta, "screen_ids": screen_ids, "dur_ingest": time.time() - t0}

    @task
    def parse_task(run_meta: dict) -> dict:
        t0 = time.time()
        parse.run(run_id=run_meta["run_id"], screen_ids=run_meta["screen_ids"])
        return {**run_meta, "dur_parse": time.time() - t0}

    @task
    def embed_image_task(run_meta: dict) -> dict:
        t0 = time.time()
        clip_version = embed_image.run(
            run_id=run_meta["run_id"],
            screen_ids=run_meta["screen_ids"],
        )
        return {**run_meta, "clip_version": clip_version, "dur_embed_image": time.time() - t0}

    @task
    def embed_text_task(run_meta: dict) -> dict:
        t0 = time.time()
        sbert_version = embed_text.run(
            run_id=run_meta["run_id"],
            screen_ids=run_meta["screen_ids"],
        )
        return {**run_meta, "sbert_version": sbert_version, "dur_embed_text": time.time() - t0}

    @task
    def extract_task(run_meta: dict) -> dict:
        t0 = time.time()
        prompt_version = extract.run(
            run_id=run_meta["run_id"],
            screen_ids=run_meta["screen_ids"],
        )
        return {**run_meta, "prompt_version": prompt_version, "dur_extract": time.time() - t0}

    @task
    def load_task(embed_img: dict, embed_txt: dict, ext: dict) -> dict:
        t0 = time.time()
        run_meta = {**embed_img, **embed_txt, **ext}
        load.run(run_id=run_meta["run_id"], screen_ids=run_meta["screen_ids"])
        return {**run_meta, "dur_load": time.time() - t0}

    @task
    def audit_task(run_meta: dict) -> dict:
        t0 = time.time()
        try:
            audit.run(run_id=run_meta["run_id"])
        except AirflowFailException as e:
            notifications.notify_audit_failed(
                run_id=run_meta["run_id"],
                details={"error": str(e)},
            )
            finish_run(run_id=run_meta["run_id"], status="paused-by-audit")
            raise
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
        model_versions = {
            "clip":   run_meta.get("clip_version"),
            "sbert":  run_meta.get("sbert_version"),
            "prompt": run_meta.get("prompt_version"),
        }
        finish_run(
            run_id=run_meta["run_id"],
            status="succeeded",
            model_versions=model_versions,
        )
        total = time.time() - run_meta["started_at"]
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

    run_meta      = init_run()
    ingested      = ingest_task(run_meta)
    parsed        = parse_task(ingested)

    img_meta      = embed_image_task(parsed)
    txt_meta      = embed_text_task(parsed)
    ext_meta      = extract_task(parsed)

    loaded        = load_task(img_meta, txt_meta, ext_meta)
    audited       = audit_task(loaded)
    evalled       = eval_task(audited)
    observed      = observability_task(evalled)
    finalize(observed)


rico_pipeline()