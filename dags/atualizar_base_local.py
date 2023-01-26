from datetime import datetime
from airflow import DAG
from airflow.decorators import task
from utils import getConexaoLocal, getConexaoBQ, Filter_Queue, Filter_Running, Filter_Failed, Filter_OverTryFailure, url_base_base
from airflow.sensors.base import PokeReturnValue
import requests

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
        cursor.execute("SELECT id FROM charge WHERE status = 'Running'")
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
            cursor.execute(f"SELECT id, status, was_sent, retry, id_parent FROM job WHERE id_charge =  + '{idCarga[0]}' AND status <> 'Done' ")
            jobsPendentes += cursor.fetchall()
        db.close()
        return jobsPendentes

    @task(task_id="VerificarJobsPendentesNoBancoExterno")
    def VerificarJobsPendentesNoBancoExterno(ti=None):
        jobsPendentes = ti.xcom_pull(task_ids="CapturarJobsPendentes")

        def MapearIds(x):
            return str(f"'{x[0]}'")

        jobsId = ','.join(map(MapearIds, jobsPendentes))
        db = getConexaoBQ()
        cursor = db.cursor()
        query = (f"SELECT id, status, was_sent, retry, id_parent FROM job WHERE id IN ({jobsId})")
        print(query)
        cursor.execute(query)
        result = cursor.fetchall()
        db.close()
        return result

    @task
    def AtualizarBancoLocal(ti=None):
        db = getConexaoLocal()
        cursor = db.cursor()
        jobsEmProducao = ti.xcom_pull(task_ids="VerificarJobsPendentesNoBancoExterno")
        for jobs in jobsEmProducao:
            query = f"UPDATE job SET status = '{jobs[1]}', was_sent = {jobs[2]}, retry = {jobs[3]}, id_parent = '{jobs[4]}' WHERE id = '{jobs[0]}'"
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
            query = (f"SELECT id, status, was_sent, retry, id_parent FROM job WHERE id_charge = '{carga[0]}'")
            cursor.execute(query)
            jobs = cursor.fetchall()
            jobs_EmFila = list(filter(Filter_Queue, jobs))
            jobs_Falhos = list(filter(Filter_Failed, jobs))
            jobs_FalhosPorExcessoDeTentativa = list(filter(Filter_OverTryFailure, jobs))
            jobs_Rodando = list(filter(Filter_Running, jobs))
            jobs_pendentes = len(jobs_EmFila) > 0 or len(jobs_Falhos) > 0 or len(jobs_Rodando) > 0
            print("Existe Jobs pendentes: " + str(jobs_pendentes))
            if (len(jobs_Falhos) > 0):
                ReenviarJobs(idCarga)

            if (jobs_pendentes):
                continue

            else:
                AtualizarCargas(idCarga, len(jobs_FalhosPorExcessoDeTentativa) > 0)

    def ReenviarJobs(idCarga):
        import requests
        request = requests.get(url_base_base + "resend_jobs_failed/?" + "id_charge=" + idCarga)
        result = request.json()
        db = getConexaoLocal()
        cursor = db.cursor()
        for job in result:
            id = job["id"]
            was_sent = job["was_sent"]
            retry = job["retry"]
            id_charge = job["id_charge"]
            status = job["status"]
            id_parent = job["id_parent"]
            query = f"INSERT INTO job(id, status, was_sent, retry, id_parent, id_charge) VALUES('{id}','{status}', {was_sent}, {retry}, '{id_parent}', '{id_charge}')"
            print(query)
            cursor.execute(query)
        db.commit()
        db.close()

    def AtualizarCargas(idCarga: str, parcialmenteCompleto: bool):
        state = 'Partially_Done' if parcialmenteCompleto else 'Done'
        db = getConexaoLocal()
        cursor = db.cursor()
        query = f"UPDATE charge SET status = '{state}' WHERE id = '{idCarga}'"
        cursor.execute(query)
        db.commit()
        db.close()        
        requests.get(f"{url_base_base}updateStateOfCharge/?id_charge={idCarga}&state={state}")

    PegarCargasPendentes() >> CapturarJobsPendentes_Local(
    ) >> VerificarJobsPendentesNoBancoExterno() >> AtualizarBancoLocal() >> TratarCargas()
