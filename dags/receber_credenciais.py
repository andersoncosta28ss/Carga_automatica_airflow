from datetime import datetime
from airflow import DAG
from airflow.decorators import task
from airflow.sensors.base import PokeReturnValue
from utils import getConexaoLocal, getConexaoBQ
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

    @task(task_id="Enviar_Para_RoberthAPI")
    def Enviar_Para_RoberthAPI(ti=None):
        import requests
        credenciais = ti.xcom_pull(task_ids="VerificarSeExisteCredencialNova")
        cargas = []
        for credencial in credenciais:
            idCarga = str(uuid4())
            idCredencial = credencial[0]            
            request = requests.get(f"http://host.docker.internal:3005/criar_carga2?id_charge={idCarga}&id_credential={idCredencial}")
            jobsId = request.json()
            cargas.append({"idCarga": idCarga, "idCredencial": idCredencial, "idJobs": jobsId})
        return cargas

    @task
    def GuardarJobsLocalmente(ti=None):
        cargas = ti.xcom_pull(task_ids="Enviar_Para_RoberthAPI")
        db = getConexaoLocal()
        cursor = db.cursor()
        for carga in cargas:
            idCarga = carga['idCarga']
            idCredencial = carga['idCredencial']
            jobsId = carga['idJobs']
            query = f"INSERT INTO charge (id, credential_id) values('{idCarga}', '{idCredencial}')"
            cursor.execute(query)
            for idJob in jobsId:
                query = f"INSERT INTO job (id, id_charge) values('{idJob}', '{idCarga}')"
                cursor.execute(query)
        db.commit()
        db.close()

    Sensor_VerificarSeExisteCredencialNova() >> Enviar_Para_RoberthAPI() >> GuardarJobsLocalmente()
