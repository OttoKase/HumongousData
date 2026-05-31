.PHONY: build up down clean trigger reset migrate logs test audit-test help

build:
	docker compose build

up:
	docker compose up -d

down:
	docker compose down

clean:
	docker compose down -v

LIMIT ?= 5

trigger:
	docker compose exec airflow-scheduler airflow dags trigger rico_pipeline \
		--conf '{"limit": $(LIMIT)}'

reset:
	docker compose exec postgres psql -U rico -d rico -c \
		"TRUNCATE pipeline_runs, pipeline_metrics, audit_results, \
		screens_metadata, screens_embeddings, screens_review_queue, screens_eval \
		RESTART IDENTITY CASCADE;"
	docker compose run --rm \
		-e MC_HOST_local=http://minioadmin:minioadmin@minio:9000 \
		minio-init sh -c \
		"mc rm --recursive --force local/$${MINIO_BUCKET:-rico-raw} ; \
		 mc mb --ignore-existing local/$${MINIO_BUCKET:-rico-raw} && \
		 echo 'MinIO bucket cleared and recreated'"

migrate:
	docker compose exec postgres psql -U rico -d rico \
		-f /docker-entrypoint-initdb.d/001_init.sql

logs:
	docker compose logs -f

test:
	pytest tests/ -v

# Verifies the audit circuit-breaker fires on cross-run duplicates.
# Works by writing a second fake run_id against the same screen_ids that
# the real run already owns — this creates the cross-run duplicate that
# the audit is designed to detect.
audit-test:
	@echo "--- Step 1: injecting cross-run duplicate into screens_metadata ---"
	docker compose exec postgres psql -U rico -d rico -c "\
		INSERT INTO pipeline_runs (run_id, dag_run_id, started_at, status, limit_param) \
		VALUES ('00000000-0000-0000-0000-000000000001', 'audit-test-fake-run', NOW(), 'running', 5); \
		UPDATE screens_metadata \
		SET run_id = '00000000-0000-0000-0000-000000000001' \
		WHERE screen_id IN (SELECT screen_id FROM screens_metadata LIMIT 1);"
	@echo "--- Step 2: triggering DAG — audit task should go red ---"
	$(MAKE) trigger LIMIT=$(LIMIT)
	@echo "--- Check http://localhost:8080 — audit task should be red, eval skipped ---"

audit-test-cleanup:
	@echo "Removing injected fake run and restoring original run_id..."
	docker compose exec postgres psql -U rico -d rico -c "\
		UPDATE screens_metadata \
		SET run_id = ( \
			SELECT run_id FROM pipeline_runs \
			WHERE run_id != '00000000-0000-0000-0000-000000000001' \
			ORDER BY started_at DESC LIMIT 1 \
		) \
		WHERE run_id = '00000000-0000-0000-0000-000000000001'; \
		DELETE FROM pipeline_runs \
		WHERE run_id = '00000000-0000-0000-0000-000000000001';"

help:
	@echo "make build           - ehita Airflow image"
	@echo "make up              - käivita stack"
	@echo "make down            - peata stack"
	@echo "make clean           - peata stack ja kustuta volumes"
	@echo "make trigger         - käivita DAG (LIMIT=5 vaikimisi)"
	@echo "make reset           - tühjenda kõik tabelid + MinIO bucket"
	@echo "make migrate         - käivita 002 migration"
	@echo "make logs            - vaata logisid"
	@echo "make test            - käivita testid"
	@echo "make audit-test      - auditi test. NB! kasuta make audit-test-cleanup pärast"
	@echo "make audit-test-cleanup - auditi testi puhastus"