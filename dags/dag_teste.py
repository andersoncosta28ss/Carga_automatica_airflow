import datetime
from airflow import DAG
from airflow.decorators import task
from airflow.operators.bash import BashOperator

with DAG(
    dag_id="1-TESTE",
    start_date=datetime.datetime(2022, 1, 1),
    schedule_interval=None,
    max_active_runs=1,
    catchup=False
) as dag:
    @task(task_id="GenerateRandomNumber")
    def GenerateRandomNumber():
        from random import randint
        return randint(1, 10)

    @task.branch
    def even_or_odd(ti=None):
        number = ti.xcom_pull(task_ids="GenerateRandomNumber")
        if number % 2 == 0:
            return 'even'
        return 'odd'

    even = BashOperator(
        task_id='even',
        bash_command='echo "even"',
    )

    odd = BashOperator(
        task_id='odd',
        bash_command='echo "odd"',
    )

    GenerateRandomNumber() >> even_or_odd() >> [even, odd]
