from datetime import datetime
from airflow import DAG
from airflow.decorators import task
from airflow.sensors.base import PokeReturnValue
from db_functions import Prod_Find_credentials, Local_Filter_credentials, Local_CreateCharges
from api_functions import SendToAPI
from airflow.models import Variable

with DAG(dag_id="1_receber_credenciais", start_date=datetime(2022, 1, 1), schedule_interval="@hourly", max_active_runs=1) as dag:

    @task.sensor(poke_interval=10, timeout=3600, mode="reschedule", soft_fail=True, task_id="VerificarSeExisteCredencialNova")
    def Sensor_VerificarSeExisteCredencialNova() -> PokeReturnValue:
        credenciais = Prod_Find_credentials(Variable)
        credenciais = Local_Filter_credentials(credenciais)
        return PokeReturnValue(is_done=len(credenciais) > 0, xcom_value=credenciais)

    @task(task_id="Enviar_Para_RoberthAPI")
    def Enviar_Para_RoberthAPI(ti=None):
        credenciais = ti.xcom_pull(task_ids="VerificarSeExisteCredencialNova")
        return SendToAPI(credenciais)

    @task
    def GuardarJobsLocalmente(ti=None):
        cargas = ti.xcom_pull(task_ids="Enviar_Para_RoberthAPI")
        Local_CreateCharges(cargas)

    Sensor_VerificarSeExisteCredencialNova() >> Enviar_Para_RoberthAPI() >> GuardarJobsLocalmente()
