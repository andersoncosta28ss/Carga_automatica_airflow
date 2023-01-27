from datetime import datetime
from airflow import DAG
from airflow.decorators import task
from airflow.sensors.base import PokeReturnValue
from db_connections import getConexaoLocal, getConexaoProd
from functions_list import Filter_Queue, Filter_Running, Filter_Failed, Filter_OverTryFailure
from api_functions import ResendJobs
from db_functions import Local_InsertJobsResend, Local_Find_PendingCharges, Local_Find_PendingChargesByCharge, BQ_Find_JobsByIds, Local_UpdateJobs, Local_UpdateCharge

with DAG(
    dag_id="1_atualizar_base_local",
    start_date=datetime(2022, 1, 1),
    schedule_interval="@hourly",
    max_active_runs=1
) as dag:

    @task.sensor(poke_interval=10, timeout=3600, mode="reschedule", soft_fail=True, task_id="PegarCargasPendentes")
    def PegarCargasPendentes() -> PokeReturnValue:
        idCharge = Local_Find_PendingCharges()
        return PokeReturnValue(is_done=len(idCharge) > 0, xcom_value=idCharge)

    @task(task_id="CapturarJobsPendentes")
    def CapturarJobsPendentes_Local(ti=None):
        idCharges = ti.xcom_pull(task_ids="PegarCargasPendentes")
        return Local_Find_PendingChargesByCharge(idCharges)

    @task(task_id="VerificarJobsPendentesNoBancoExterno")
    def VerificarJobsPendentesNoBancoExterno(ti=None):
        jobsPendentes = ti.xcom_pull(task_ids="CapturarJobsPendentes")        
        return BQ_Find_JobsByIds(jobsPendentes)

    @task
    def AtualizarBancoLocal(ti=None):  
        jobsEmProducao = ti.xcom_pull(task_ids="VerificarJobsPendentesNoBancoExterno")
        Local_UpdateJobs(jobsEmProducao)

    @task
    def TratarCargas(ti=None):
        charges = ti.xcom_pull(task_ids="PegarCargasPendentes")
        for charge in charges:
            idCharge = charge[0]
            jobs = Local_Find_PendingChargesByCharge(idCharge)
            jobs_EmFila = list(filter(Filter_Queue, jobs))
            jobs_Falhos = list(filter(Filter_Failed, jobs))
            jobs_FalhosPorExcessoDeTentativa = list(
                filter(Filter_OverTryFailure, jobs))
            jobs_Rodando = list(filter(Filter_Running, jobs))
            jobs_pendentes = len(jobs_EmFila) > 0 or len(jobs_Falhos) > 0 or len(jobs_Rodando) > 0
            if (len(jobs_Falhos) > 0):
                ReenviarJobs(idCharge)

            if (jobs_pendentes):
                continue

            else:
                AtualizarCargas(idCharge, len(
                    jobs_FalhosPorExcessoDeTentativa) > 0)

    def ReenviarJobs(idCharge):
        jobs = ResendJobs(idCharge)
        Local_InsertJobsResend(jobs)

    def AtualizarCargas(idCharge: str, parcialmenteCompleto: bool):
        import requests
        state = 'Partially_Done' if parcialmenteCompleto else 'Done'
        Local_UpdateCharge(idCharge, state)
        requests.get(f"http://host.docker.internal:3005/updateStateOfCharge/?id_charge={idCharge}&state={state}")

    PegarCargasPendentes() >> CapturarJobsPendentes_Local(
    ) >> VerificarJobsPendentesNoBancoExterno() >> AtualizarBancoLocal() >> TratarCargas()
