import base64
import logging

import pandas as pd
import psycopg2
from google.cloud import bigquery
from kubernetes import client, config
from sqlalchemy import create_engine

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def df_from_postgres(query, connection_string):
    alchemyEngine = create_engine(connection_string)
    dbConnection = alchemyEngine.connect()

    return pd.read_sql(query,
        dbConnection)


def replace_old_table_in_bigquery(table_id: str, client: bigquery.Client):
    job_config = bigquery.CopyJobConfig()
    job_config.write_disposition = "WRITE_TRUNCATE"
    job = client.copy_table(
        table_id + "_staging", table_id, location="europe-north1", job_config=job_config,
    )

    job.result()

    logger.info(f"Replaced table {table_id} with {table_id}_staging")


def ensure_namespace_is_part_of_db_host(connection_string: str, namespace: str) -> str:
    connection_string_parts = connection_string.split(":5432")
    if len(connection_string_parts) != 2:
        raise Exception("invalid connection string format")
    
    before_port = connection_string_parts[0]
    after_port = connection_string_parts[1]

    if "." in before_port.split("@")[1]:
        return connection_string

    return before_port + f".{namespace}:5432" + after_port


if __name__=='__main__':
    config.load_incluster_config()
    v1 = client.CoreV1Api()

    logger.info("Listing secrets for all namespaces")
    secrets = v1.list_secret_for_all_namespaces(field_selector='metadata.name=airflow-db')
    namespaces_with_airflow = []
    for item in secrets.items:
        namespaces_with_airflow.append(item.metadata.namespace)

    namespaces_with_airflow = ["team-nada-oqs1"]
    
    for i, namespace in enumerate(namespaces_with_airflow):
        logger.info(f"Processing namespace {namespace}")
        sec = v1.read_namespaced_secret("airflow-db", namespace).data
        connection_string = base64.b64decode(sec["connection"]).decode()

        connection_string = ensure_namespace_is_part_of_db_host(connection_string, namespace)

        query_dag_runs = "select dag_run.*, dag.schedule_interval as schedule_interval, dag.max_active_tasks as max_active_tasks, dag.max_active_runs as max_active_runs from dag_run left join dag on dag.dag_id=dag_run.dag_id"
        df_dag_runs = df_from_postgres(query_dag_runs, connection_string)

        df_dag_runs["namespace"] = namespace

        df_bigquery = df_dag_runs[["dag_id", "execution_date", "state", "run_id", "external_trigger", "run_type", "schedule_interval", "max_active_tasks", "max_active_runs", "namespace"]]

        dataset_project = "nada-prod-6977"
        dataset = "knorten_north"

        table_id = f"{dataset_project}.{dataset}.airflow_dagruns"

        client = bigquery.Client()
        job_config = bigquery.LoadJobConfig(
                autodetect=True,
                write_disposition="WRITE_TRUNCATE" if i == 0 else "WRITE_APPEND",
        )

        logger.info(f"Writing stats for {namespace} to table {table_id}_staging")

        job = client.load_table_from_dataframe(
            df_bigquery, table_id + "_staging", job_config=job_config
        )

    logger.info(f"Replacing table {table_id} with {table_id}_staging")
    replace_old_table_in_bigquery(table_id, client)
