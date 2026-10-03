.PHONY: up down init dbt-build dbt-test lint dashboard test sample

up:            ## Start Airflow locally
	docker compose up -d --build

down:          ## Stop and remove containers
	docker compose down

logs:
	docker compose logs -f airflow-scheduler

dbt-build:     ## Run all dbt models + tests against prod
	cd dbt/flightline && dbt deps && dbt build --profiles-dir . --target prod

dbt-test:
	cd dbt/flightline && dbt test --profiles-dir . --target prod

lint:
	sqlfluff lint dbt/flightline/models

dashboard:     ## Run the Streamlit dashboard
	streamlit run dashboard/app.py

sample:        ## Generate one month of synthetic BTS data locally
	FLIGHTLINE_SAMPLE=1 python -m ingestion.bts_extract

test:          ## Compile-check Python and run the unit tests
	python -m compileall -q ingestion dashboard airflow/dags
	python -m pytest -q tests
