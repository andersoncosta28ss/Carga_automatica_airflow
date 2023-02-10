import mysql.connector
from google.cloud import bigquery
import json


def getConnectionLocal(envs):
    return mysql.connector.connect(
        host=envs.get("DB_LOCAL_HOST"),
        user=envs.get("DB_LOCAL_USER"),
        password=envs.get("DB_LOCAL_PASSWORD"),
        database=envs.get("DB_LOCAL_DATABASE")
    )


def getConnectionProd(envs):
    return mysql.connector.connect(
        host=envs.get("DB_SOSSEGO_HOST"),
        user=envs.get("DB_SOSSEGO_USER"),
        password=envs.get("DB_SOSSEGO_PASSWORD"),
        database=envs.get("DB_SOSSEGO_DATABASE")
    )


def getConnectionBQ(envs):
    return bigquery.Client.from_service_account_info(json.loads(envs.get("BQ_JSON")))
