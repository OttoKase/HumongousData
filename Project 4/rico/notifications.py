# rico/notifications.py
"""
Slack notifications — observability, not a pipeline dependency.
Every call is wrapped in try/except: a Slack failure must never fail the run.
"""
import json
import logging
import os

import requests

log = logging.getLogger(__name__)


def _post(payload: dict) -> None:
    url = os.environ.get("SLACK_WEBHOOK_URL", "")
    if not url:
        log.info("SLACK_WEBHOOK_URL not set — skipping notification")
        return
    try:
        resp = requests.post(url, json=payload, timeout=10)
        resp.raise_for_status()
    except Exception as e:
        log.warning(f"Slack notification failed (non-fatal): {e}")


def notify_run_started(run_id: str, limit: int, trigger: str) -> None:
    try:
        _post({
            "text": (
                f":rocket: *RICO pipeline started*\n"
                f"• `run_id`: `{run_id}`\n"
                f"• `LIMIT`: {limit}\n"
                f"• triggered by: {trigger}"
            )
        })
    except Exception as e:
        log.warning(f"notify_run_started failed (non-fatal): {e}")


def notify_audit_failed(run_id: str, details: dict, log_url: str = "") -> None:
    try:
        meta_dupes = details.get("metadata_duplicates", [])
        emb_dupes  = details.get("embedding_duplicates", [])

        lines = [f":rotating_light: *AUDIT FAILED — pipeline halted*"]
        lines.append(f"• `run_id`: `{run_id}`")

        if meta_dupes:
            lines.append(f"• metadata duplicates: `{meta_dupes}`")
        if emb_dupes:
            lines.append(f"• embedding duplicates: `{emb_dupes}`")
        if log_url:
            lines.append(f"• <{log_url}|View Airflow task log>")

        lines.append(f"\n_eval was skipped. Investigate before re-triggering._")

        _post({"text": "\n".join(lines)})
    except Exception as e:
        log.warning(f"notify_audit_failed failed (non-fatal): {e}")


def notify_run_finished(
    run_id: str,
    status: str,
    total_duration_s: float,
    summary: dict,
) -> None:
    try:
        icon = {
            "succeeded":      ":white_check_mark:",
            "failed":         ":x:",
            "paused-by-audit": ":warning:",
        }.get(status, ":grey_question:")

        _post({
            "text": (
                f"{icon} *RICO pipeline {status}*\n"
                f"• `run_id`: `{run_id}`\n"
                f"• duration: {total_duration_s:.1f}s\n"
                f"• metadata rows: {summary.get('metadata_row_count', '?')}\n"
                f"• extracted: {summary.get('pct_extracted', '?')}%  "
                f"high-conf: {summary.get('pct_high_conf', '?')}%  "
                f"review queue: {summary.get('pct_review_queue', '?')}%"
            )
        })
    except Exception as e:
        log.warning(f"notify_run_finished failed (non-fatal): {e}")