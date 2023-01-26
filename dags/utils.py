import mysql.connector

url_base_base = "http://host.docker.internal:3005/"

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

def inserirJobsNoBanco(id_carga, id_job):
    db = getConexaoLocal()
    cursor = db.cursor()
    query = f"INSERT INTO job values (id_carga, id_job) values ({id_carga}, {id_job})"
    cursor.execute(query)
    db.commit()
    db.close()

def Filter_Queue(job):
    return job[1] == 'Queue'

def Filter_Running(job):
    return job[1] == 'Running'

def Filter_Failed(job):
    return job[1] == 'Failed' and job[3] > 0 and job[2] == False

def Filter_OverTryFailure(job):
    return job[1] == 'Failed' and job[3] <= 0