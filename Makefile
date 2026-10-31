SHELL := /bin/bash
.DEFAULT_GOAL := help

.PHONY: help env build up down logs trigger test lint format sample-data run-local summary clean

help:
	@echo "ecommerce-etl"
	@echo ""
	@echo "  make up          Create .env and start Airflow + PostgreSQL"
	@echo "  make down        Stop the stack (keeps the database volume)"
	@echo "  make trigger     Trigger the Airflow DAG manually"
	@echo "  make test        Run unit and integration tests"
	@echo "  make lint        Run ruff"
	@echo "  make run-local   Run the pipeline locally against the Docker warehouse"
	@echo "  make summary     Print warehouse KPIs"
	@echo "  make clean       Stop the stack and DELETE the database volume"

env:
	@test -f .env || cp .env.example .env
	@python3 scripts/setup_env.py

build: env
	docker compose build

up: env
	docker compose up -d --build

down:
	docker compose down

logs:
	docker compose logs -f --tail=100

trigger:
	docker compose exec -T airflow-scheduler airflow dags trigger ecommerce_orders_etl

test:
	uv run pytest

lint:
	uv run ruff check .

format:
	uv run ruff format .
	uv run ruff check --fix .

sample-data:
	uv run python scripts/generate_sample_data.py

run-local:
	uv run python -m ecommerce_etl.cli run \
		--input data/raw/orders_2026_10.xlsx \
		--dsn postgresql://etl:etl@localhost:5434/warehouse

summary:
	uv run python -m ecommerce_etl.cli summary \
		--dsn postgresql://etl:etl@localhost:5434/warehouse

clean:
	docker compose down --volumes --remove-orphans
