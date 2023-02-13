import datetime
from airflow import DAG
from airflow.decorators import task
from utils_conts import SQL_JOB_Select_DefaultInternalFields
from db_functions import Local_Select_PendingCharges, Local_Update_Charge
from db_connections import getConnectionLocal
from utils_functions import Local_Filter_Failed, Filter_Queued, Local_Filter_OverTryFailure, Filter_Running, Map_InternalJobs
from airflow.exceptions import AirflowSkipException
from airflow.models import Variable
from utils_conts import ChargeStatus, JobStatus

with DAG(
    dag_id="3-Charge_control",
    start_date=datetime.datetime(2022, 1, 1),
    schedule_interval="@hourly",
    max_active_runs=1,
    default_args={"mysql_conn_id": "local_mysql"},
    render_template_as_native_obj=True,
    catchup=False
) as dag:
    @task(task_id="CapturePendingCharges")
    def CapturePendingCharges():
        pendingCharges = Local_Select_PendingCharges(Variable)
        print("Quantidade de items capturados -> " + str(len(pendingCharges)))
        if(len(pendingCharges) == 0):
            raise AirflowSkipException
        return pendingCharges

    @task
    def UpdateCharges(ti=None):
        charges = ti.xcom_pull(task_ids= "CapturePendingCharges")
        db = getConnectionLocal(Variable)
        cursor = db.cursor()
        for charge in charges:
            idCharge = charge[0]
            query = f"SELECT {SQL_JOB_Select_DefaultInternalFields} FROM job WHERE charge_id = '{idCharge}' AND status <> '{JobStatus.Done.value}'"
            cursor.execute(query)
            result = cursor.fetchall()
            result = list(map(Map_InternalJobs, result))
            jobs_queued = list(filter(Filter_Queued, result))
            jobs_failed = list(filter(Local_Filter_Failed, result))
            jobs_overTryFailure = list(filter(Local_Filter_OverTryFailure, result))
            jobs_running = list(filter(Filter_Running, result))
            jobs_pending = len(jobs_queued) > 0 or len(jobs_failed) > 0 or len(jobs_running) > 0
            print("id carga -> " + str(idCharge))
            print("Quantidade de Jobs em fila -> " + str(len(jobs_queued)))
            print("Quantidade de Jobs falhos -> " + str(len(jobs_failed)))
            print("Quantidade de Jobs falhos por excesso -> " + str(len(jobs_overTryFailure)))
            print("Quantidade de Jobs running -> " + str(len(jobs_running)))
            print("Quantidade de Jobs pendentes -> " + str(jobs_pending))
            if jobs_pending:
                continue

            else:
                state = ChargeStatus.Partially_Done.value if len(jobs_overTryFailure) > 0 else ChargeStatus.Done.value
                Local_Update_Charge(idCharge, state, Variable)
        db.close()


    CapturePendingCharges() >> UpdateCharges()

    