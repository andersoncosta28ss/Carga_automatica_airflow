import mysql.connector
from datetime import datetime
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow import DAG
from airflow.decorators import task
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.operators.bash import BashOperator

with DAG(
    dag_id="atualizar_base_local",
    start_date=datetime(2022, 1, 1),    
    schedule_interval="@hourly",
) as dag:

    @task(task_id="PegarCargasPendentes")
    def PegarCargasPendentes():
        db = mysql.connector.connect(host="host.docker.internal", user="root", password="1234", database="local")
        cursor = db.cursor()
        cursor.execute("SELECT id FROM charge WHERE status = 'Running'")
        idsCarga = cursor.fetchall()
        db.close()
        return idsCarga

    @task(task_id="CapturarJobsPendentes")
    def CapturarJobsPendentes_Local(ti=None):
        idsCarga = ti.xcom_pull(task_ids="PegarCargasPendentes")
        db = mysql.connector.connect(
            host="host.docker.internal", user="root", password="1234", database="local")
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

        jobsId = map(MapearIds, jobsPendentes)
        db = mysql.connector.connect(host="host.docker.internal", user="root", password="1234", database="bq")
        cursor = db.cursor()
        query = (f"SELECT id, status, was_sent, retry, id_parent FROM job WHERE id IN ({','.join(jobsId)})")
        cursor.execute(query)
        result = cursor.fetchall()
        db.close()
        return result

    @task
    def AtualizarBancoLocal(ti=None):
        db = mysql.connector.connect(host="host.docker.internal", user="root", password="1234", database="local")
        cursor = db.cursor()
        jobsEmProducao = ti.xcom_pull(task_ids="VerificarJobsPendentesNoBancoExterno")
        for jobs in jobsEmProducao:
            query = f"UPDATE job SET status = '{jobs[1]}', was_sent = {jobs[2]} WHERE id = '{jobs[0]}'"
            print(query)
            cursor.execute(query)
        db.commit()

    @task
    def VerificarCargas(ti=None):
        idsCarga = ti.xcom_pull(task_ids="PegarCargasPendentes")
        db = mysql.connector.connect(host="host.docker.internal", user="root", password="1234", database="local")
        cursor = db.cursor()
        for idCarga in idsCarga:
            query = (f"SELECT id, status, was_sent, retry, id_parent FROM job WHERE id_charge = '{idCarga}")
            cursor.execute(query)
            jobs = cursor.fetchall()
            jobs_EmFila = list(filter(Filter_Queue, jobs))
            jobs_Falhos = list(filter(Filter_Failed, jobs))
            jobs_FalhosPorExcessoDeTentativa = list(filter(Filter_OverTryFailure, jobs))
            jobs_Rodando = list(filter(Filter_Running, jobs))
            jobs_pendentes = len(jobs_EmFila) > 0 | len(jobs_Falhos) > 0 | len(jobs_Rodando) > 0
            if (jobs_Falhos > 0):
                ReenviarJobs()
            
            if (jobs_pendentes):
                continue

            else:
                AtualizarCargas()

    def ReenviarJobs():
        pass            

    @task
    def AtualizarCargas(ti=None):
        pass    

    PegarCargasPendentes() >> CapturarJobsPendentes_Local() >> VerificarJobsPendentesNoBancoExterno() >> AtualizarBancoLocal() >> VerificarCargas()


def Filter_Queue(job):
    return job[1] == 'Queue'

def Filter_Running(job):
    return job[1] == 'Running'

def Filter_Failed(job):
    return job[1] == 'Failed' & job[3] < 3

def Filter_OverTryFailure(job):
    return job[1] == 'Failed' & job[3] >= 3
