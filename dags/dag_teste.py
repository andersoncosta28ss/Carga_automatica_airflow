from datetime import datetime
from airflow import DAG
from airflow.decorators import task
from airflow.sensors.base import PokeReturnValue
from airflow.operators.bash import BashOperator

_1min = 60

with DAG(
    dag_id="1-TESTE",
    start_date=datetime(2022, 1, 1),
    schedule_interval="@hourly",
    max_active_runs=1,
    catchup=False
) as dag:
    @task.sensor(poke_interval=_1min, mode="reschedule", timeout=_1min * 60, soft_fail=True, task_id="SENSOR", max_wait=_1min * 2)
    def GenerateRandomNumber() -> PokeReturnValue:
        from random import randint
        return PokeReturnValue(is_done=True, xcom_value= randint(1, 10))

    @task.branch
    def even_or_odd(ti=None):
        number = ti.xcom_pull(task_ids="SENSOR")
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