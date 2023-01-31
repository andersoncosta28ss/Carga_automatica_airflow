from datetime import datetime
from airflow import DAG
from airflow.decorators import task
from airflow.sensors.base import PokeReturnValue
from db_connections import getConnectionLocal
from utils_functions import Filter_Queue, Filter_Running, Filter_Failed_Local, Filter_OverTryFailure, Filter_Failed_BQ
from db_functions import Local_Select_PendingCharges, BQ_Select_JobsByIds, Local_Update_Charge, Local_Select_PendingJobs, BQ_Select_JobsChildrenByIdParent, Local_Select_PendingCharges
from db_query import Query_Local_SelectJobsFromIdCharge, Query_Local_InsertChildrenJob, Query_Local_UpdateJob, Query_Local_Insert_Splited_Jobs
from api_functions import Prod_SplitJob
from airflow.providers.mysql.operators.mysql import MySqlOperator
from airflow.models import Variable
from utils_conts import _1hr, _24hrs, _10s


with DAG(
    dag_id="1-producao_atualizar_base_local",
    start_date=datetime(2022, 1, 1),
    schedule_interval="@hourly",
    max_active_runs=1,
    default_args={"mysql_conn_id": "local_mysql"},
    render_template_as_native_obj=True
) as dag:

    @task.sensor(poke_interval=_10s, timeout=_24hrs, mode="reschedule", soft_fail=True, task_id="Sensor_CapturarJobsPendentes")
    def CapturarJobsPendentes() -> PokeReturnValue:
        pendingJobs = Local_Select_PendingJobs()
        return PokeReturnValue(is_done=len(pendingJobs) > 0, xcom_value=pendingJobs)

    @task(task_id="PegarJobsPendentesNaBigQuery")
    def PegarJobsPendentesNaBigQuery(ti=None):
        pendingJobs = ti.xcom_pull(task_ids="Sensor_CapturarJobsPendentes")
        _pendingJobs = BQ_Select_JobsByIds(pendingJobs, Variable)
        failedJobs = list(filter(Filter_Failed_BQ, _pendingJobs))
        ti.xcom_push(key="FailedJobs", value=failedJobs)
        return _pendingJobs

    @task(task_id="PegarJobsFilhosNaBigQuery")
    def PegarJobsFilhosNaBigQuery(ti=None):
        FailedJobs = ti.xcom_pull(key="FailedJobs")
        return BQ_Select_JobsChildrenByIdParent(FailedJobs, Variable)

    @task(task_id="QuebrarOsPeriodosDosJobsQueFalharam")
    def QuebrarOsPeriodosDosJobsQueFalharam(ti=None):
        FailedJobs = ti.xcom_pull(key="FailedJobs")
        newJobs = Prod_SplitJob(FailedJobs, Variable)
        return newJobs

    @task
    def PrepararSQLs(ti=None):
        jobsProd = ti.xcom_pull(task_ids="PegarJobsPendentesNaBigQuery")
        childrenJobs = ti.xcom_pull(task_ids="PegarJobsFilhosNaBigQuery")
        splitedJobs = ti.xcom_pull(
            task_ids="QuebrarOsPeriodosDosJobsQueFalharam")
        ti.xcom_push(key="SQL_INSERT_CHILDRENJOBS",
                     value=Query_Local_InsertChildrenJob(childrenJobs))
        ti.xcom_push(key="SQL_UPDATE_JOBS",
                     value=Query_Local_UpdateJob(jobsProd))
        ti.xcom_push(key="SQL_INSERT_SPLITED_JOBS",
                     value=Query_Local_Insert_Splited_Jobs(splitedJobs))

    InserirJobsFilhos = MySqlOperator(
        task_id="MYSQL_InserirJobsFilhos",
        sql="{{ti.xcom_pull(key='SQL_INSERT_CHILDRENJOBS')}}",
        dag=dag
    )

    AtualizarJobs = MySqlOperator(
        task_id="MYSQL_AtualizarJobs",
        sql="{{ti.xcom_pull(key='SQL_UPDATE_JOBS')}}",
        dag=dag
    )

    InserirJobsComPeriodosQuebrados = MySqlOperator(
        task_id="MYSQL_InserirJobsComPeriodosQuebrados",
        sql="SELECT 0",
        dag=dag
    )

    @task
    def TratarCargas(ti=None):
        cargas = Local_Select_PendingCharges()
        # Se tentarmos usar a função de Local_Find_PendingChargesByCharge dá erro de excesso de conexão, sem sentido nenhum
        db = getConnectionLocal()
        cursor = db.cursor()
        for carga in cargas:
            idCarga = carga[0]
            query = (Query_Local_SelectJobsFromIdCharge(idCarga))
            cursor.execute(query)
            jobs = cursor.fetchall()
            jobs_EmFila = list(filter(Filter_Queue, jobs))
            jobs_Falhos = list(filter(Filter_Failed_Local, jobs))
            jobs_FalhosPorExcessoDeTentativa = list(
                filter(Filter_OverTryFailure, jobs))
            jobs_Rodando = list(filter(Filter_Running, jobs))
            jobs_pendentes = len(jobs_EmFila) > 0 or len(
                jobs_Falhos) > 0 or len(jobs_Rodando) > 0
            if (jobs_pendentes):
                continue

            else:
                AtualizarCargas(idCarga, len(
                    jobs_FalhosPorExcessoDeTentativa) > 0)
        db.close()

    def AtualizarCargas(idCharge: str, parcialmenteCompleto: bool):
        state = 'Partially_Done' if parcialmenteCompleto else 'Done'
        Local_Update_Charge(idCharge, state)

    CapturarJobsPendentes() >> PegarJobsPendentesNaBigQuery(
    ) >> PegarJobsFilhosNaBigQuery() >> QuebrarOsPeriodosDosJobsQueFalharam() >> PrepararSQLs() >> InserirJobsFilhos >> AtualizarJobs >> InserirJobsComPeriodosQuebrados >> TratarCargas()
