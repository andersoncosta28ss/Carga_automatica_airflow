import mysql.connector

def getConexaoLocal():
    return mysql.connector.connect(
        host="host.docker.internal", 
        user="root", 
        password="1234", 
        database="local"
    )

def getConexaoBQ():
    return mysql.connector.connect(
        host="host.docker.internal", 
        user="root", 
        password="1234", 
        database="bq"
    )

def getConexaoStage(envs):
    return mysql.connector.connect(
        host=envs.get("DB_HOST"),
        user=envs.get("DB_USER"),
        password=envs.get("DB_PASSWORD"),
        database=envs.get("DB_DATABASE_SETTINGS")
    )