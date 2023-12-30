#! /bin/bash

# ELASTIC
export ELASTIC_HOST_EXT='localhost'
export ELASTIC_HOST_INT='es01'
export ELASTIC_USER=''
export ELASTIC_PASSWORD=''
export ELASTIC_PORT=9200
export KIBANA_PASSWORD=''
export KIBANA_PORT=5601

# AIRFLOW
export AIRFLOW_WEB_PORT=8080
export AIRFLOW_FLOWER_PORT=5555

# POSTGRES
export POSTGRES_HOST_EXT='localhost'
export POSTGRES_HOST_INT='postgres-db'
export POSTGRES_USER=''
export POSTGRES_PASSWORD=''
export POSTGRES_PORT=5432
export POSTGRES_DB_NAME='postgres'
export POSTGRES_VOLUME='/var/postgres_data'