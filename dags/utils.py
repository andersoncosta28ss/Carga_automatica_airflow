import mysql.connector
# conexao_local = {"host":"host.docker.internal", "user":"root", "password":"1234", "database":"local"}
# conexao_bq = {"host":"host.docker.internal", "user":"root", "password":"1234", "database":"bq"}

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

def Filter_Queue(job):
    return job[1] == 'Queue'

def Filter_Running(job):
    return job[1] == 'Running'

def Filter_Failed(job):
    return job[1] == 'Failed' and job[3] < 3

def Filter_OverTryFailure(job):
    return job[1] == 'Failed' and job[3] >= 3