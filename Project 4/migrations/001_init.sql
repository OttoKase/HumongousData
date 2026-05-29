SELECT 'CREATE DATABASE airflow'
WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = 'airflow')\gexec
\c rico
CREATE EXTENSION IF NOT EXISTS vector;

CREATE TABLE IF NOT EXISTS pipeline_runs (
    run_id          UUID PRIMARY KEY,
    dag_run_id      TEXT NOT NULL,
    started_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    ended_at        TIMESTAMPTZ,
    status          TEXT NOT NULL DEFAULT 'running'
                        CHECK (status IN ('running', 'succeeded', 'failed', 'paused-by-audit')),
    limit_param     INTEGER NOT NULL,
    git_sha         TEXT,
    clip_version    TEXT,
    sbert_version   TEXT,
    llm_model       TEXT,
    prompt_version  TEXT
);

CREATE TABLE IF NOT EXISTS audit_results (
    id          BIGSERIAL PRIMARY KEY,
    run_id      UUID NOT NULL REFERENCES pipeline_runs(run_id),
    audit_name  TEXT NOT NULL,
    passed      BOOLEAN NOT NULL,
    details     JSONB,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS pipeline_metrics (
    id            BIGSERIAL PRIMARY KEY,
    run_id        UUID NOT NULL REFERENCES pipeline_runs(run_id),
    metric_name   TEXT NOT NULL,
    metric_value  DOUBLE PRECISION,
    metric_text   TEXT,
    created_at    TIMESTAMPTZ NOT NULL DEFAULT NOW()
);


CREATE TABLE IF NOT EXISTS screens_metadata (
    screen_id           BIGINT PRIMARY KEY,
    app_package         TEXT,
    category            TEXT,
    png_path            TEXT NOT NULL,
    hierarchy_json_path TEXT NOT NULL,
    extraction_payload  JSONB,
    prompt_version      TEXT,
    confidence          DOUBLE PRECISION,
    created_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    run_id              UUID REFERENCES pipeline_runs(run_id),
    source_fingerprint  TEXT
);

CREATE TABLE IF NOT EXISTS screens_embeddings (
    screen_id           BIGINT NOT NULL,
    model_name          TEXT NOT NULL,
    model_version       TEXT NOT NULL,
    embedding_kind      TEXT NOT NULL CHECK (embedding_kind IN ('image', 'text')),
    vector              vector NOT NULL,
    created_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    run_id              UUID REFERENCES pipeline_runs(run_id),
    source_fingerprint  TEXT,
    PRIMARY KEY (screen_id, model_name, model_version, embedding_kind)
);

CREATE TABLE IF NOT EXISTS screens_review_queue (
    id          BIGSERIAL PRIMARY KEY,
    screen_id   BIGINT NOT NULL,
    reason      TEXT NOT NULL,
    raw_output  TEXT,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    run_id      UUID REFERENCES pipeline_runs(run_id)
);

CREATE TABLE IF NOT EXISTS screens_eval (
    id                       BIGSERIAL PRIMARY KEY,
    embedding_model_version  TEXT NOT NULL,
    n_queries                INTEGER NOT NULL,
    recall_at_5              DOUBLE PRECISION NOT NULL,
    created_at               TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    run_id                   UUID REFERENCES pipeline_runs(run_id)
);