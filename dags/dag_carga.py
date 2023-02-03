from datetime import datetime
from airflow import DAG
from airflow.decorators import task
from airflow.sensors.base import PokeReturnValue
from airflow.models import Variable
from airflow.providers.mysql.operators.mysql import MySqlOperator
from utils_conts import _1hr, _24hrs, _10s, _1min
from db_functions import Local_Select_PendingCharges, Local_Update_Charge
from db_connections import getConnectionLocal
from utils_functions import Filter_Failed_ToValidCharge, Filter_Queued, Filter_OverTryFailure, Filter_Running, Map_InternalJobs
from db_query import Query_Local_Select_JobsFromIdCharge

with DAG(
    dag_id="3-carga",
    start_date=datetime(2022, 1, 1),
    schedule_interval="@hourly",
    max_active_runs=1,
    default_args={"mysql_conn_id": "local_mysql"},
    render_template_as_native_obj=True
) as dag:
    @task.sensor(poke_interval=_1min * 10, timeout=_24hrs, mode="reschedule", soft_fail=True, task_id="Sensor_CapturarCargasPendentes")
    def Sensor_CapturarCargasPendentes() -> PokeReturnValue:
        pendingCharges = Local_Select_PendingCharges()
        return PokeReturnValue(is_done=len(pendingCharges) > 0, xcom_value=pendingCharges)

    @task
    def AtualizarACarga(ti=None):
        charges = ti.xcom_pull(task_ids= "Sensor_CapturarCargasPendentes")
        db = getConnectionLocal()
        cursor = db.cursor()
        for charge in charges:
            idCharge = charge[0]
            query = (Query_Local_Select_JobsFromIdCharge(idCharge))
            cursor.execute(query)
            result = cursor.fetchall()
            result = list(map(Map_InternalJobs, result))
            jobs_EmFila = list(filter(Filter_Queued, result))
            jobs_Falhos = list(filter(Filter_Failed_ToValidCharge, result))
            jobs_FalhosPorExcessoDeTentativa = list(
                filter(Filter_OverTryFailure, result))
            jobs_Rodando = list(filter(Filter_Running, result))
            jobs_pendentes = len(jobs_EmFila) > 0 or len(jobs_Falhos) > 0 or len(jobs_Rodando) > 0
            print("id carga -> " + str(idCharge))
            print("Quantidade de Jobs em fila -> " + str(len(jobs_EmFila)))
            print("Quantidade de Jobs falhos -> " + str(len(jobs_Falhos)))
            print("Quantidade de Jobs falhos por excesso -> " + str(len(jobs_FalhosPorExcessoDeTentativa)))
            print("Quantidade de Jobs running -> " + str(len(jobs_Rodando)))
            print("Quantidade de Jobs pendentes -> " + str(jobs_pendentes))
            if (jobs_pendentes):
                continue

            else:
                state = 'partially_done' if len(jobs_FalhosPorExcessoDeTentativa) > 0 else 'done'
                Local_Update_Charge(idCharge, state)
        db.close()


    Sensor_CapturarCargasPendentes() >> AtualizarACarga()

    