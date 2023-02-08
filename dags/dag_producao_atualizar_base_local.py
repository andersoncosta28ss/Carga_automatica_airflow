from datetime import datetime
from airflow import DAG
from airflow.decorators import task
from utils_functions import BQ_Filter_Failed, BQ_Filter_OverTryFailure
from db_functions import BQ_Select_JobsByIds, Local_Select_PendingJobs, BQ_Select_JobsChildrenByIdParent
from db_query import Query_Local_Insert_ChildrenJob, Query_Local_Update_Job, Query_Local_Insert_Splited_Jobs
from api_functions import Prod_SplitJob
from airflow.providers.mysql.operators.mysql import MySqlOperator
from airflow.models import Variable
from airflow.exceptions import AirflowSkipException
import time


with DAG(
    dag_id="1-producao_atualizar_base_local",
    start_date=datetime(2022, 1, 1),
    schedule_interval="@hourly",
    max_active_runs=1,
    default_args={"mysql_conn_id": "local_mysql"},
    render_template_as_native_obj=True,
    catchup=False
) as dag:
    # @task.sensor(poke_interval=_1min, timeout=_1min * 5, mode="reschedule",soft_fail=True, task_id="Sensor_CapturarJobsPendentes")
    # def CapturarJobsPendentes() -> PokeReturnValue:
        # return PokeReturnValue(is_done=len(pendingJobs) > 0, xcom_value=pendingJobs)
    @task(task_id="CapturarJobsPendentes")
    def CapturarJobsPendentes():
        pendingJobs = Local_Select_PendingJobs()
        print("Quantidade de items capturados -> " + str(len(pendingJobs)))
        if(len(pendingJobs) == 0):
            raise AirflowSkipException
        else:
            return pendingJobs

    @task(task_id="PegarJobsPendentesNaBigQuery")
    def PegarJobsPendentesNaBigQuery(ti=None):
        pendingJobs = ti.xcom_pull(task_ids="CapturarJobsPendentes")
        _pendingJobs = BQ_Select_JobsByIds(pendingJobs, Variable)
        failedJobs = list(filter(BQ_Filter_Failed, _pendingJobs))
        jobsOverTryFailure = list(filter(BQ_Filter_OverTryFailure, _pendingJobs))
        ti.xcom_push(key="FailedJobs", value=failedJobs)
        ti.xcom_push(key="JobsOverTryFailure", value=jobsOverTryFailure)
        return _pendingJobs

    @task(task_id="PegarJobsFilhosNaBigQuery")
    def PegarJobsFilhosNaBigQuery(ti=None):
        FailedJobs = ti.xcom_pull(key="FailedJobs")
        return BQ_Select_JobsChildrenByIdParent(FailedJobs, Variable)

    @task(task_id="QuebrarOsPeriodosDosJobsQueFalharam")
    def QuebrarOsPeriodosDosJobsQueFalharam(ti=None):
        FailedJobs = ti.xcom_pull(key="JobsOverTryFailure")
        newJobs = Prod_SplitJob(FailedJobs, Variable)
        return newJobs

    @task
    def PrepararSQLs(ti=None):
        jobsProd = ti.xcom_pull(task_ids="PegarJobsPendentesNaBigQuery")
        childrenJobs = ti.xcom_pull(task_ids="PegarJobsFilhosNaBigQuery")
        splitedJobs = ti.xcom_pull(
            task_ids="QuebrarOsPeriodosDosJobsQueFalharam")
        ti.xcom_push(key="SQL_INSERT_CHILDRENJOBS",
                     value=Query_Local_Insert_ChildrenJob(childrenJobs))
        ti.xcom_push(key="SQL_UPDATE_JOBS",
                     value=Query_Local_Update_Job(jobsProd))
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
        sql="{{ti.xcom_pull(key='SQL_INSERT_SPLITED_JOBS')}}",
        dag=dag
    )

    CapturarJobsPendentes() >> PegarJobsPendentesNaBigQuery(
    ) >> PegarJobsFilhosNaBigQuery() >> QuebrarOsPeriodosDosJobsQueFalharam() >> PrepararSQLs() >> InserirJobsFilhos >> AtualizarJobs >> InserirJobsComPeriodosQuebrados
