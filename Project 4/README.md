# RICO Multimodal Pipeline

An Airflow DAG that ingests Android UI screenshots from the RICO dataset, embeds them with CLIP and SBERT, extracts structured metadata with a local LLM, and validates the result with a duplicate-detection audit. Every row in every table is traceable to the exact run, model version, and input bytes that produced it.

---

## Prerequisites

- Docker Desktop with at least 8 GB RAM allocated
- ~10 GB free disk space (model weights + Docker images)
- A Slack incoming webhook URL (optional — pipeline runs without it)

---

## Setup

```bash
cp .env.example .env          # fill in SLACK_WEBHOOK_URL if desired
make build                    # builds the Airflow image (~5 min on first run)
make up                       # starts all 8 containers
```

Wait ~60 seconds for `airflow-init` to finish, then open **http://localhost:8080** (example login: `admin` / `admin`).

---

## Running the Pipeline

```bash
make trigger           # runs with LIMIT=5 (default)
```

Watch progress in the Airflow UI. A full run with `LIMIT=5` takes 3–5 minutes, dominated by the Ollama LLM extraction step.

To reset all tables and MinIO objects between runs:

```bash
make reset
```

---

## Project Structure

```
dags/
  rico_dag.py          # Airflow DAG — orchestration only, no business logic
migrations/
  001_init.sql         # Schema for all 7 tables
prompts/
  extract_v1.txt       # Versioned LLM prompt
rico/
  ingest.py            # HuggingFace → MinIO + screens_metadata
  parse.py             # View hierarchy JSON → flat text → MinIO
  embed_image.py       # CLIP ViT-B-32 → screens_embeddings (kind='image')
  embed_text.py        # SBERT all-MiniLM-L6-v2 → screens_embeddings (kind='text')
  extract.py           # Ollama LLM → extraction_payload in screens_metadata
  load.py              # Validates row counts before audit
  audit.py             # Duplicate detection — halts pipeline on failure
  eval.py              # Recall@5 self-test → screens_eval
  observability.py     # Metrics collection → pipeline_metrics + log summary
  notifications.py     # Slack notifications (non-fatal)
  traceability.py      # run_id lifecycle + SHA-256 fingerprinting
tests/
  test_audit.py        # Integration tests for the audit circuit-breaker
```

---

## DAG Structure

```
ingest → parse → ┌─ embed_image ─┐
                 ├─ embed_text  ─┤ → load → audit → eval → observability → finalize
                 └─ extract    ─┘
```

The three middle tasks run in parallel. If `audit` fails, `eval` is skipped and the run is marked `paused-by-audit`.

---

## Traceability

Every row in every destination table carries two traceability columns:

**`run_id`** — UUID foreign key to `pipeline_runs`, which records the exact commit (`git_sha`), all model versions (`clip_version`, `sbert_version`, `llm_model`, `prompt_version`), start/end time, and final status for the run that wrote the row.

**`source_fingerprint`** — SHA-256 hash of the input bytes that produced the row. For `screens_metadata` this is the PNG bytes. For `screens_embeddings` it is the bytes fed to the embedder (PNG for image, UTF-8 text for text). So we know exactly if the model saw the right byte sequence.

To trace any row back to its run:

```sql
-- Which run produced screen 42, and with which models?
SELECT r.run_id, r.started_at, r.status,
       r.clip_version, r.sbert_version, r.llm_model, r.prompt_version, r.git_sha
FROM screens_metadata m
JOIN pipeline_runs r USING (run_id)
WHERE m.screen_id = 42;
```

---

## Audit

The audit runs after `load` and before `eval`. It checks for **cross-run duplicates**: screen IDs or (screen\_id, embedding\_kind) pairs that are attributed to more than one `run_id`. This detects cases where the upsert failed to consolidate a re-processed screen under the current run.

If duplicates are found:
- The full list of affected `screen_id`s is written to `audit_results` with `passed=false`
- A Slack alert is posted with the duplicate keys
- `AirflowFailException` is raised — `eval` is skipped, the run is marked `paused-by-audit`
- The bad data stays in place so it can be inspected

To query audit history:

```sql
SELECT run_id, passed, details, created_at
FROM audit_results
ORDER BY created_at DESC;
```

---

## Observability Metrics

At the end of every run, `observability_task` writes the following metrics to `pipeline_metrics` (keyed by `run_id` and `metric_name`) and prints a one-screen summary to the Airflow task log.

| Metric | Description |
|---|---|
| `duration_s.<task>` | Wall-clock seconds for each task |
| `duration_s.total` | Sum of all task durations |
| `metadata.row_count` | Screens processed in this run |
| `metadata.pct_extracted` | % of screens with a non-null LLM extraction payload |
| `metadata.pct_high_conf` | % of screens where LLM confidence ≥ 0.5 |
| `metadata.pct_review_queue` | % of screens routed to the review queue (failed extraction) |
| `embeddings.image.row_count` | Image embedding rows written |
| `embeddings.image.avg_dims` | Average vector dimensionality — should always be 512 |
| `embeddings.image.pct_zero_norm` | % of image vectors with zero L2 norm (embedder bug indicator) |
| `embeddings.text.row_count` | Text embedding rows written |
| `embeddings.text.avg_dims` | Average vector dimensionality — should always be 384 |
| `embeddings.text.pct_zero_norm` | % of text vectors with zero L2 norm |
| `sanity.distinct_app_packages` | Distinct app packages in this run (detects accidental duplicates) |
| `sanity.distinct_categories` | Distinct UI categories in this run |

To query metrics for the most recent run:

```sql
SELECT metric_name, metric_value, metric_text
FROM pipeline_metrics
WHERE run_id = (SELECT run_id FROM pipeline_runs ORDER BY started_at DESC LIMIT 1)
ORDER BY metric_name;
```

---

## Slack Notifications

Set `SLACK_WEBHOOK_URL` in `.env` to receive three notifications per run:

- **Run started** — `run_id`, `LIMIT`, trigger type
- **Audit failed** — duplicate `screen_id`s, `run_id`, instruction to investigate before re-triggering
- **Run finished** — final status, total duration, extracted/high-confidence/review-queue percentages

A failure to post to Slack is logged as a warning and never fails the pipeline.

To create a webhook URL: https://api.slack.com/apps → Create App → Incoming Webhooks → Add Webhook to Workspace.

---

## Idempotency

Re-running the DAG with the same `LIMIT` produces no new rows. Every write uses `INSERT ... ON CONFLICT DO UPDATE`, so the same screen processed twice is updated in place rather than duplicated. To verify:

```bash
make trigger LIMIT=5   # first run
make trigger LIMIT=5   # second run
```

```sql
SELECT COUNT(*) FROM screens_metadata;     -- still 5
SELECT COUNT(*) FROM screens_embeddings;   -- still 10
```

---

## Tests

Integration tests for the audit circuit-breaker. Requires a running Postgres instance.

```bash
POSTGRES_DSN=postgresql://rico:rico@localhost:5432/rico pytest tests/ -v
```

**`test_audit_passes_on_clean_data`** — a run with no cross-run overlap passes the audit without raising.

**`test_audit_fails_on_cross_run_metadata_duplicate`** — a `screen_id` claimed by two different `run_id`s in `screens_metadata` causes `AirflowFailException` and writes `passed=false` to `audit_results`.

**`test_audit_fails_on_cross_run_embedding_duplicate`** — a `(screen_id, embedding_kind)` attributed to two `run_id`s causes the same failure.

**`test_audit_result_committed_before_raise`** — confirms the `audit_results` row is committed to the database before the exception propagates, so the DAG's except-block can read it for the Slack notification.

### Manual audit circuit-breaker test

To verify the circuit-breaker end-to-end in the running stack:

```bash
make trigger LIMIT=5       # run once cleanly first
make audit-test            # injects a cross-run duplicate, triggers the DAG
```

Expected result: `audit_task` goes red in the Airflow UI, `eval_task` is skipped (grey).

To undo the injection:

```bash
make audit-test-cleanup
```

---

## Interpreting an Audit Failure

When `audit_task` goes red:

1. Check the duplicate keys in the Airflow task log or in Slack
2. Query the details directly:
```sql
SELECT details FROM audit_results
WHERE run_id = '<failed_run_id>';
```
3. Identify which screens are affected:
```sql
-- Which runs claimed the same screen?
SELECT screen_id, run_id, updated_at
FROM screens_metadata
WHERE screen_id = <affected_screen_id>
ORDER BY updated_at;
```
4. Run `make reset` to wipe all tables and MinIO, then re-trigger from scratch

Do not clear the failed audit task and re-run it — the audit found real data integrity problems. Reset and re-run the full pipeline.

---

## Database Schema

| Table | Purpose |
|---|---|
| `pipeline_runs` | One row per DAG run — models, git SHA, status, start/end time |
| `screens_metadata` | One row per screen — app, category, LLM extraction, confidence |
| `screens_embeddings` | One row per (screen, model, kind) — 512-dim image or 384-dim text vector |
| `screens_review_queue` | Screens that failed LLM extraction, with reason and raw LLM output |
| `audit_results` | Audit pass/fail history with full duplicate details as JSONB |
| `pipeline_metrics` | All observability metrics keyed by run_id and metric_name |
| `screens_eval` | Recall@5 score per run |

---

