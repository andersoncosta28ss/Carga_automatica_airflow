import datetime
from airflow import DAG
from airflow.decorators import task
from db_functions import Prod_Select_Credentials, Local_Filter_Credentials
from api_functions import Prod_SendToAPI
from airflow.models import Variable
from airflow.providers.mysql.operators.mysql import MySqlOperator
from db_query import Query_Local_Insert_Charge
from airflow.exceptions import AirflowSkipException


with DAG(
    dag_id="2-Credential_control",
    start_date=datetime.datetime(2022, 1, 1),
    schedule_interval="@hourly",
    max_active_runs=1,
    default_args={"mysql_conn_id": "local_mysql"},
    render_template_as_native_obj=True,
    catchup=False
) as dag:

    @task(task_id="CheckIfNewCredentialExists")
    def CheckIfNewCredentialExists():
        credenciais = Prod_Select_Credentials(Variable)
        credenciais = Local_Filter_Credentials(credenciais, Variable)
        print("Quantidade de items capturados -> " + str(len(credenciais)))
        if(len(credenciais) == 0):
            raise AirflowSkipException
        else:
            return credenciais

    @task(task_id="Submit_To_RobethAPI")
    def Submit_To_RobethAPI(ti=None):
        credenciais = ti.xcom_pull(task_ids="CheckIfNewCredentialExists")
        result = Prod_SendToAPI(credenciais, Variable)
        ti.xcom_push(key="SQL_INSERT_CHARGE", value=Query_Local_Insert_Charge(result))
        return result

    mysql_task = MySqlOperator(
        task_id="MYSQL_GuardarJobsLocalmente",
        sql="{{ti.xcom_pull(key='SQL_INSERT_CHARGE')}}",
        dag=dag,
    )

    CheckIfNewCredentialExists() >> Submit_To_RobethAPI() >> mysql_task
