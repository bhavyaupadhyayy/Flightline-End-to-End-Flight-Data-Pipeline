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
	sqlfluff lint dbt/flightline/models --dialect snowflake

dashboard:     ## Run the Streamlit dashboard
	streamlit run dashboard/app.py

sample:        ## Generate one month of synthetic BTS data locally
	FLIGHTLINE_SAMPLE=1 python -m ingestion.bts_extract

test:          ## Compile-check Python
	python -m compileall ingestion dashboard airflow/dags
