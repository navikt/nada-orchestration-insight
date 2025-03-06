import base64
import logging

import pandas as pd
import psycopg2
from google.cloud import bigquery
from kubernetes import client, config
from sqlalchemy import create_engine

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

STAGING_POSTFIX="_staging"

def df_from_postgres(query, connection_string):
    alchemyEngine = create_engine(connection_string)
    dbConnection = alchemyEngine.connect()

    return pd.read_sql(query,
        dbConnection)


def replace_old_table_in_bigquery(table_id: str, client: bigquery.Client):
    job_config = bigquery.CopyJobConfig()
    job_config.write_disposition = "WRITE_TRUNCATE"
    job = client.copy_table(
        table_id + STAGING_POSTFIX, table_id, location="europe-north1", job_config=job_config,
    )

    job.result()

    logger.info(f"Replaced table {table_id} with {table_id}{STAGING_POSTFIX}")


def delete_staging_table(table_id: str, client: bigquery.Client):
    client.delete_table(table_id + STAGING_POSTFIX, not_found_ok=True)

    logger.info(f"Deleted table {table_id}{STAGING_POSTFIX}")


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
    for secret in secrets.items:
        namespace = secret.metadata.namespace

        logger.info(f"Processing namespace {namespace}")
        connection_string = base64.b64decode(secret.data["connection"]).decode()

        connection_string = ensure_namespace_is_part_of_db_host(connection_string, namespace)

        query_dag_runs = """
select 
    task_instance.task_id as task_id, 
    task_instance.duration as task_duration, 
    task_instance.state as task_status, 
    dag_run.*, 
    dag.schedule_interval as schedule_interval, 
    dag.max_active_tasks as max_active_tasks, 
    dag.max_active_runs as max_active_runs 
from task_instance 
left join dag_run on dag_run.run_id=task_instance.run_id
left join dag on dag.dag_id=dag_run.dag_id
"""
        df_dag_runs = df_from_postgres(query_dag_runs, connection_string)

        df_dag_runs["namespace"] = namespace

        df_bigquery = df_dag_runs[["task_id", "task_duration", "task_status", "dag_id", "start_date", "end_date", "execution_date", "state", "run_id", "external_trigger", "run_type", "schedule_interval", "max_active_tasks", "max_active_runs", "namespace"]]

        dataset_project = "nada-prod-6977"
        dataset = "knorten_north"

        table_id = f"{dataset_project}.{dataset}.airflow_dagruns"

        client = bigquery.Client()
        job_config = bigquery.LoadJobConfig(
            autodetect=True,
            write_disposition="WRITE_APPEND",
        )
 
        logger.info(f"Writing stats for {namespace} to table {table_id}{STAGING_POSTFIX}")

        job = client.load_table_from_dataframe(
            df_bigquery, table_id + STAGING_POSTFIX, job_config=job_config
        )

    logger.info(f"Replacing table {table_id} with {table_id}{STAGING_POSTFIX}")
    replace_old_table_in_bigquery(table_id, client)
    #delete_staging_table(table_id, client)
