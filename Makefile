# Common tasks

.PHONY: venv-pipeline venv-local venv-backend build-delta publish-delta build-snapshot publish-snapshot serve update dev up down lint fmt test

venv-pipeline:
	cd pipeline && python -m venv .venv && . .venv/bin/activate && pip install -r requirements.txt

venv-local:
	cd local_app && python -m venv .venv && . .venv/bin/activate && pip install -r requirements.txt

venv-backend:
	cd backend && python -m venv .venv && . .venv/bin/activate && pip install -r requirements.txt

# These assume you already have data/exports/decisions.json.gz from your scrapers.
build-delta:
	python -m caselaw_pipeline.cli build-delta --export data/exports/decisions.json.gz --out _build --date $$(date -u +%F) --parquet

publish-delta:
	python -m caselaw_pipeline.cli publish-delta --build-dir _build --date $$(date -u +%F) --parquet

build-snapshot:
	python -m caselaw_pipeline.cli build-snapshot --export data/exports/decisions.json.gz --out _build --week $$(python -c "import datetime as dt; y,w,_=dt.date.today().isocalendar(); print(f'{y}-W{w:02d}')") --vacuum --parquet

publish-snapshot:
	python -m caselaw_pipeline.cli publish-snapshot --build-dir _build --week $$(python -c "import datetime as dt; y,w,_=dt.date.today().isocalendar(); print(f'{y}-W{w:02d}')") --parquet

consolidate-weekly:
	python -m caselaw_pipeline.cli consolidate-weekly --build-dir _build --week $$(python -c "import datetime as dt; y,w,_=dt.date.today().isocalendar(); print(f'{y}-W{w:02d}')") --parquet

update:
	python -m caselaw_local.cli update

serve:
	python -m caselaw_local.cli serve

# Backend / Docker targets
dev:
	docker compose up --build

up:
	docker compose up -d --build

down:
	docker compose down

lint:
	cd backend && ruff check .

fmt:
	cd backend && ruff format . && ruff check . --fix

test:
	python -m pytest tests/ -q
