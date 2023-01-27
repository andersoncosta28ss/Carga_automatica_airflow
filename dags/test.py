from airflow import DAG
from airflow.decorators import task
from datetime import datetime
from airflow.models import Variable
from airflow.operators.bash import BashOperator
from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryCreateEmptyDatasetOperator,
    BigQueryCreateEmptyTableOperator,
    BigQueryDeleteDatasetOperator,
    BigQueryDeleteTableOperator,
    BigQueryGetDatasetTablesOperator,
    BigQueryUpdateDatasetOperator,
    BigQueryUpdateTableOperator,
    BigQueryUpdateTableSchemaOperator,
    BigQueryUpsertTableOperator,
)

with DAG(
    dag_id="1_test",
    start_date=datetime(2022, 1, 1),
    schedule_interval=None,
    max_active_runs=1
) as dag:

    get_dataset_tables = BigQueryGetDatasetTablesOperator(
        task_id="get_dataset_tables", dataset_id="bigquery_stage", gcp_conn_id="bigquery_stage"
    )

    @task
    def print1(ti=None):
        value = ti.xcom_pull(task_ids="get_dataset_tables")
        print(value)

    get_dataset_tables >> print1()
    # @task
    # def a(ti=None):
    #     ti.xcom_push(key="a", value="a")

    
    # b = BashOperator(
    #     task_id="tbo",
    #     bash_command="echo {{ti.xcom_pull(key='a')}}}",
    #     dag=dag
    # )

    # @task
    # def c(ti=None):
    #     ti.xcom_push(key="a", value="2")

    # a() >> b
