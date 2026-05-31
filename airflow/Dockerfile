FROM apache/airflow:2.10.5

USER airflow
COPY requirements-airflow.txt /tmp/requirements-airflow.txt
# Install in stages to avoid pip's backtracking resolver hitting its round limit
# on the combined Airflow + Snowflake + dbt dependency graph.
RUN pip install --no-cache-dir \
        "apache-airflow-providers-snowflake>=5.7" \
        "snowflake-connector-python>=3.12" \
 && pip install --no-cache-dir "dbt-snowflake>=1.8,<1.9" \
 && pip install --no-cache-dir "requests>=2.31"
