from datetime import datetime
from airflow import DAG
from airflow.decorators import task
from airflow.sensors.base import PokeReturnValue
from db_connections import getConexaoLocal, getConexaoBQ
from db_query import Local_InsertJobs, Local_InsertCharge, Local_InsertCredential, Local_SelectCrendetial, BQ_SelectCredential
from uuid import uuid4
from airflow.models import Variable

with DAG(
    dag_id="2_receber_credenciais",
    start_date=datetime(2022, 1, 1),
    schedule_interval="@hourly",
    max_active_runs=1
) as dag:
    @task.sensor(poke_interval=10, timeout=3600, mode="reschedule", soft_fail=True, task_id="VerificarSeExisteCredencialNova")
    def Sensor_VerificarSeExisteCredencialNova() -> PokeReturnValue:
        db = getConexaoBQ()
        cursor = db.cursor()
        query = BQ_SelectCredential()
        cursor.execute(query)
        credenciais = cursor.fetchall()
        db.close()
        credenciais = VerificarSeExisteCargaComEssasCredenciais(credenciais)
        return PokeReturnValue(is_done=len(credenciais) > 0, xcom_value=credenciais)

    def VerificarSeExisteCargaComEssasCredenciais(credenciais):
        credenciaisParaProsseguir = []

        for credencial in credenciais:
            idCredencial = credencial[0]
            db = getConexaoLocal()
            cursor = db.cursor()
            query = Local_SelectCrendetial(idCredencial)
            cursor.execute(query)
            consultaInterna = cursor.fetchall()
            if (len(consultaInterna) > 0):
                db.close()
                continue
            else:
                credenciaisParaProsseguir.append(credencial)
            db.close()
        return credenciaisParaProsseguir

    @task(task_id="Enviar_Para_RoberthAPI")
    def Enviar_Para_RoberthAPI(ti=None):
        import requests
        credenciais = ti.xcom_pull(task_ids="VerificarSeExisteCredencialNova")
        cargas = []
        for credencial in credenciais:
            idCarga = str(uuid4())
            idCredencial = credencial[0]
            request = requests.get(
                f"http://host.docker.internal:3005/criar_carga2?id_charge={idCarga}&id_credential={idCredencial}")
            jobsId = request.json()
            cargas.append(
                {"idCarga": idCarga, "idCredencial": idCredencial, "idJobs": jobsId})
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
            query = Local_InsertCredential(idCredencial)
            cursor.execute(query)
            db.commit()
            query = Local_InsertCharge(idCarga, idCredencial)
            cursor.execute(query)
            for idJob in jobsId:
                query = Local_InsertJobs(idJob, idCarga)
                cursor.execute(query)
        db.commit()
        db.close()

    Sensor_VerificarSeExisteCredencialNova(
    ) >> Enviar_Para_RoberthAPI() >> GuardarJobsLocalmente()
