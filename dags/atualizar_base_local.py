from datetime import datetime
from airflow import DAG
from airflow.decorators import task
from airflow.sensors.base import PokeReturnValue
from db_connections import getConexaoLocal, getConexaoBQ
from functions_list import Filter_Queue, Filter_Running, Filter_Failed, Filter_OverTryFailure
from db_query import Local_InsertJobResents, Local_UpdateCharge, Local_SelectJobsFromIdCharge, BQ_SelectJobs, Local_UpdateJob
import requests

url_base_base = "http://host.docker.internal:3005/"

with DAG(
    dag_id="1_atualizar_base_local",
    start_date=datetime(2022, 1, 1),
    schedule_interval="@hourly",
    max_active_runs=1
) as dag:

    @task.sensor(poke_interval=10, timeout=3600, mode="reschedule", soft_fail=True, task_id="PegarCargasPendentes")
    def PegarCargasPendentes() -> PokeReturnValue:
        db = getConexaoLocal()
        cursor = db.cursor()
        query = "SELECT id FROM charge WHERE status = 'Running'"
        cursor.execute(query)
        idsCarga = cursor.fetchall()
        db.close()
        return PokeReturnValue(is_done=len(idsCarga) > 0, xcom_value=idsCarga)

    @task(task_id="CapturarJobsPendentes")
    def CapturarJobsPendentes_Local(ti=None):
        idsCarga = ti.xcom_pull(task_ids="PegarCargasPendentes")
        db = getConexaoLocal()
        cursor = db.cursor()
        jobsPendentes = []
        for idCarga in idsCarga:
            query = Local_SelectJobsFromIdCharge(idCarga[0])
            cursor.execute(query)
            jobsPendentes += cursor.fetchall()
        db.close()
        return jobsPendentes

    @task(task_id="VerificarJobsPendentesNoBancoExterno")
    def VerificarJobsPendentesNoBancoExterno(ti=None):
        jobsPendentes = ti.xcom_pull(task_ids="CapturarJobsPendentes")
        db = getConexaoBQ()
        cursor = db.cursor()

        def MapearIds(x):
            return str(f"'{x[0]}'")

        jobsId = ','.join(map(MapearIds, jobsPendentes))
        query = BQ_SelectJobs(jobsId)
        print(query)
        cursor.execute(query)
        result = cursor.fetchall()
        db.close()
        return result

    @task
    def AtualizarBancoLocal(ti=None):
        db = getConexaoLocal()
        cursor = db.cursor()
        jobsEmProducao = ti.xcom_pull(
            task_ids="VerificarJobsPendentesNoBancoExterno")
        for job in jobsEmProducao:
            query = Local_UpdateJob(job)
            cursor.execute(query)
        db.commit()
        db.close()

    @task
    def TratarCargas(ti=None):
        cargas = ti.xcom_pull(task_ids="PegarCargasPendentes")
        db = getConexaoLocal()
        cursor = db.cursor()
        for carga in cargas:
            idCarga = carga[0]
            query = (Local_SelectJobsFromIdCharge(idCarga))
            cursor.execute(query)
            jobs = cursor.fetchall()
            jobs_EmFila = list(filter(Filter_Queue, jobs))
            jobs_Falhos = list(filter(Filter_Failed, jobs))
            jobs_FalhosPorExcessoDeTentativa = list(
                filter(Filter_OverTryFailure, jobs))
            jobs_Rodando = list(filter(Filter_Running, jobs))
            jobs_pendentes = len(jobs_EmFila) > 0 or len(
                jobs_Falhos) > 0 or len(jobs_Rodando) > 0
            if (len(jobs_Falhos) > 0):
                ReenviarJobs(idCarga)

            if (jobs_pendentes):
                continue

            else:
                AtualizarCargas(idCarga, len(
                    jobs_FalhosPorExcessoDeTentativa) > 0)

    def ReenviarJobs(idCarga):
        import requests
        request = requests.get(
            url_base_base + "resend_jobs_failed/?" + "id_charge=" + idCarga)
        result = request.json()
        db = getConexaoLocal()
        cursor = db.cursor()
        for job in result:
            query = Local_InsertJobResents(job)
            cursor.execute(query)
        db.commit()
        db.close()

    def AtualizarCargas(idCarga: str, parcialmenteCompleto: bool):
        state = 'Partially_Done' if parcialmenteCompleto else 'Done'
        db = getConexaoLocal()
        cursor = db.cursor()
        query = Local_UpdateCharge(idCarga, state)
        cursor.execute(query)
        db.commit()
        db.close()
        requests.get(
            f"{url_base_base}updateStateOfCharge/?id_charge={idCarga}&state={state}")

    PegarCargasPendentes() >> CapturarJobsPendentes_Local(
    ) >> VerificarJobsPendentesNoBancoExterno() >> AtualizarBancoLocal() >> TratarCargas()
