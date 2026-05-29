.PHONY: build up down clean trigger reset migrate logs test help

build:
	docker compose build

up:
	docker compose up -d

down:
	docker compose down

clean:
	docker compose down -v

trigger:
	docker compose exec airflow-scheduler airflow dags trigger rico_pipeline \
		--conf '{"limit": $(LIMIT:-5)}'

reset:
	docker compose exec postgres psql -U rico -d rico -c \
		"TRUNCATE pipeline_runs, pipeline_metrics, audit_results, \
		screens_metadata, screens_embeddings, screens_review_queue, screens_eval \
		RESTART IDENTITY CASCADE;"
	docker compose exec minio mc rm --recursive --force local/rico-raw

migrate:
	docker compose exec postgres psql -U rico -d rico \
		-f /docker-entrypoint-initdb.d/002_traceability.sql

logs:
	docker compose logs -f

test:
	pytest tests/

help:
	@echo "make build    - ehita Airflow image"
	@echo "make up       - käivita stack"
	@echo "make down     - peata stack"
	@echo "make clean    - peata stack ja kustuta volumes"
	@echo "make trigger  - käivita DAG (LIMIT=5 vaikimisi)"
	@echo "make reset    - tühjenda kõik tabelid + MinIO bucket"
	@echo "make migrate  - käivita 002 migration"
	@echo "make logs     - vaata logisid"
	@echo "make test     - käivita testid"