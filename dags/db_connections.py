import mysql.connector
from google.cloud import bigquery
import json

def getConnectionLocal():
    return mysql.connector.connect(
        host="host.docker.internal",
        user="root",
        password="1234",
        database="LOCAL2"
    )


def getConnectionLocal2():
    return mysql.connector.connect(
        host="host.docker.internal",
        user="root",
        password="1234",
        database="bq"
    )


def getConnectionProd(envs):
    return mysql.connector.connect(
        host=envs.get("DB_HOST"),
        user=envs.get("DB_USER"),
        password=envs.get("DB_PASSWORD"),
        database=envs.get("DB_DATABASE_SETTINGS")
    )


def getConnectionBQ(envs):
    return bigquery.Client.from_service_account_info(json.loads(envs.get("BQ_JSON")))
