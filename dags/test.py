from airflow import DAG
from airflow.decorators import task
from datetime import datetime

with DAG(
    dag_id="test",
    start_date=datetime(2022, 1, 1),
    schedule_interval=None,
    max_active_runs=1
) as dag:

    @task
    def request():
        import requests
        request = requests.get("http://host.docker.internal:3005/ping")
        print(request.content)

    request()