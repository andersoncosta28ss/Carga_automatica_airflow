from datetime import datetime
from airflow import DAG
from airflow.decorators import task
from airflow.sensors.base import PokeReturnValue
from utils import getConexaoBQ, getConexaoLocal
from uuid import uuid4


with DAG(
    dag_id="receber_credenciais",
    start_date=datetime(2022, 1, 1),
    schedule_interval="@hourly",
    max_active_runs=1
) as dag:
    @task.sensor(poke_interval=10, timeout=3600, mode="reschedule", task_id="VerificarSeExisteCredencialNova", soft_fail=True)
    def Sensor_VerificarSeExisteCredencialNova() -> PokeReturnValue:
        db = getConexaoBQ()
        cursor = db.cursor()
        query = "SELECT id, name FROM credential WHERE create_at >= DATE_SUB(NOW(), interval 10 SECOND)"
        cursor.execute(query)
        value = cursor.fetchall()
        db.close()
        return PokeReturnValue(is_done=len(value) > 0, xcom_value=value)

    @task(task_id="CriarCarga")
    def CriarCarga(ti=None):
        credenciais = ti.xcom_pull(task_ids="VerificarSeExisteCredencialNova")
        db = getConexaoBQ()
        cursor = db.cursor()
        uuidList = []
        for credencial in credenciais:
            charge_uuid = str(uuid4())
            uuidList.append(charge_uuid)
            query = f"INSERT INTO charge (id, credential_id) values('{charge_uuid}', '{credencial[0]}')"
            cursor.execute(query)
        db.commit()
        db.close()
        return uuidList

    @task(task_id="Enviar_Para_RoberthAPI")
    def Enviar_Para_RoberthAPI():
        jobsId = []
        for i in range(0, 12):
            jobsId.append(str(uuid4()))
        return jobsId

    @task
    def GuardarJobsLocalmente(ti=None):
        idCargas = ti.xcom_pull(task_ids="CriarCarga")
        idJobs = ti.xcom_pull(task_ids="Enviar_Para_RoberthAPI")
        db = getConexaoBQ()
        cursor = db.cursor()
        for idCarga in idCargas:
            for idjob in idJobs:
                query = f"INSERT INTO job (id, id_charge) values('{idjob}', '{idCarga}')"
                cursor.execute(query)
        db.commit()
        db.close()

    Sensor_VerificarSeExisteCredencialNova() >> CriarCarga(
    ) >> Enviar_Para_RoberthAPI() >> GuardarJobsLocalmente()
