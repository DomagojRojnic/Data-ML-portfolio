## About
End-to-end data pipeline which aggregates flight data stored in Elasticsearch and saves it to Postgres database. This is accomplished using Airflow DAGs and Python. Dataset used is a sample Kibana dataset. Check `airflow/dags/aggregator.py` for more information on the logic of aggregations.

## How to run
- populate `init_vars.sh` with your values and source it
- `docker-compose up`:
    1. `/airflow`
    2. `/elastic`
    3. `/postgres`
- run the DAG