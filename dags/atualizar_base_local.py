from datetime import datetime
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow import DAG
from airflow.decorators import task
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.operators.bash import BashOperator
from utils import getConexaoLocal, getConexaoBQ, Filter_Queue, Filter_Running, Filter_Failed, Filter_OverTryFailure

with DAG(
    dag_id="atualizar_base_local",
    start_date=datetime(2022, 1, 1),
    schedule_interval="@hourly",
    max_active_runs=1
) as dag:

    @task(task_id="PegarCargasPendentes")
    def PegarCargasPendentes():
        db = getConexaoLocal()
        cursor = db.cursor()
        cursor.execute("SELECT id FROM charge WHERE status = 'Running'")
        idsCarga = cursor.fetchall()
        db.close()
        return idsCarga

    @task(task_id="CapturarJobsPendentes")
    def CapturarJobsPendentes_Local(ti=None):
        idsCarga = ti.xcom_pull(task_ids="PegarCargasPendentes")
        db = getConexaoLocal()
        cursor = db.cursor()
        jobsPendentes = []
        for idCarga in idsCarga:
            print(idCarga[0])
            cursor.execute(
                f"SELECT id, status, was_sent, retry, id_parent FROM job WHERE id_charge =  + '{idCarga[0]}' AND status <> 'Done' ")
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
        query = (
            f"SELECT id, status, was_sent, retry, id_parent FROM job WHERE id IN ({jobsId})")
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
        for jobs in jobsEmProducao:
            query = f"UPDATE job SET status = '{jobs[1]}', was_sent = {jobs[2]} WHERE id = '{jobs[0]}'"
            print(query)
            cursor.execute(query)
        db.commit()
        db.close()

    @task
    def TratarCargas(ti=None):
        idsCarga = ti.xcom_pull(task_ids="PegarCargasPendentes")
        db = getConexaoLocal()
        cursor = db.cursor()
        for idCarga in idsCarga:
            query = (
                f"SELECT id, status, was_sent, retry, id_parent FROM job WHERE id_charge = '{idCarga[0]}'")
            cursor.execute(query)
            jobs = cursor.fetchall()
            jobs_EmFila = list(filter(Filter_Queue, jobs))
            jobs_Falhos = list(filter(Filter_Failed, jobs))
            jobs_FalhosPorExcessoDeTentativa = list(
                filter(Filter_OverTryFailure, jobs))
            jobs_Rodando = list(filter(Filter_Running, jobs))
            jobs_pendentes = len(jobs_EmFila) > 0 | len(
                jobs_Falhos) > 0 | len(jobs_Rodando) > 0
            if (len(jobs_Falhos) > 0):
                ReenviarJobs()

            if (jobs_pendentes):
                continue

            else:
                AtualizarCargas(idCarga, len(
                    jobs_FalhosPorExcessoDeTentativa) > 0)

    def ReenviarJobs():
        pass

    def AtualizarCargas(idCarga: str, parcialmenteCompleto: bool):
        # Atualiza a idCarga para o status recebido pelo segundo parâmetro
        pass

    PegarCargasPendentes() >> CapturarJobsPendentes_Local(
    ) >> VerificarJobsPendentesNoBancoExterno() >> AtualizarBancoLocal() >> TratarCargas()
