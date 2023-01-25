import mysql.connector
from datetime import datetime
from airflow import DAG
from airflow.decorators import task
from airflow.sensors.base import PokeReturnValue

with DAG(
    dag_id="receber_credenciais",
    start_date=datetime(2022, 1, 1),    
    schedule_interval="@hourly",
) as dag:
    @task.sensor(poke_interval=10, timeout=3600, mode="reschedule", task_id="waitAnRegister", soft_fail=True)
    def waitAnRegister() -> PokeReturnValue:     
        db = mysql.connector.connect(host="host.docker.internal", user="root", password="1234", database="bq")
        cursor = db.cursor()   
        query = "SELECT id, senha FROM senhas WHERE create_at >= DATE_SUB(NOW(), interval 10 SECOND)"
        cursor.execute(query)
        value = cursor.fetchall()
        db.close()
        return PokeReturnValue(is_done=len(value) > 0, xcom_value=value)

    @task
    def imprimirValor(ti=None):
        values = ti.xcom_pull(task_ids="waitAnRegister")
        print(values)

    waitAnRegister() >> imprimirValor()