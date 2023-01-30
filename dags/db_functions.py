from db_connections import getConexaoLocal, getConexaoProd, getConexaoStage
from db_query import Query_Local_SelectPendingJobs, Query_Local_SelectCrendetial, Query_Prod_SelectCredential, Query_BQ_Select_JobsById, Query_Local_UpdateCharge, Query_BQ_Select_JobsChildrenByIdParent
from google.cloud import bigquery
from functions_list import Map_IdJobs

def Prod_Select_Credentials(envs=None):
    if (envs is None):
        db = getConexaoProd()
        query = Query_Prod_SelectCredential()
    else:
        db = getConexaoStage(envs)
        query = "SELECT id FROM corretoras_senhas WHERE created >= DATE_SUB(NOW(), interval 30 SECOND)"
    cursor = db.cursor()
    cursor.execute(query)
    credentials = cursor.fetchall()
    db.close()
    return credentials


def Local_Filter_Credentials(credentials):
    credentialsToContinue = []
    db = getConexaoLocal()
    cursor = db.cursor()
    for credential in credentials:
        idCredential = credential[0]

        query = Query_Local_SelectCrendetial(idCredential)
        cursor.execute(query)
        consultaInterna = cursor.fetchall()
        if (len(consultaInterna) > 0):
            continue
        else:
            credentialsToContinue.append(idCredential)
    db.close()
    return credentialsToContinue


def Local_Select_PendingCharges():
    db = getConexaoLocal()
    cursor = db.cursor()
    query = "SELECT id FROM charge WHERE status = 'Running'"
    cursor.execute(query)
    idCharges = cursor.fetchall()
    db.close()
    return idCharges


def Local_Select_PendingJobs():
    pendingJobs = []
    db = getConexaoLocal()
    cursor = db.cursor()
    query = Query_Local_SelectPendingJobs()
    cursor.execute(query)
    pendingJobs = cursor.fetchall()
    db.close()
    db.disconnect()
    return pendingJobs


def Local_Update_Charge(idCharge, state):
    db = getConexaoLocal()
    cursor = db.cursor()
    query = Query_Local_UpdateCharge(idCharge, state)
    cursor.execute(query)
    db.commit()
    db.close()


def BQ_Find_JobsByIds(jobs):
    db = getConexaoProd()
    cursor = db.cursor()
    idJobs = ','.join(map(Map_IdJobs, jobs))
    cursor.execute(Query_BQ_Select_JobsById(idJobs))
    resultJobs = cursor.fetchall()
    cursor = db.cursor()
    db.close()
    return resultJobs


def BQ_Select_JobsChildrenByIdParent(jobs):
    jobsChildren = []
    db = getConexaoProd()
    cursor = db.cursor()
    idfailedJobs = ','.join(map(Map_IdJobs, jobs))
    if (len(idfailedJobs) > 0):
        query = Query_BQ_Select_JobsChildrenByIdParent(idfailedJobs)
        print(query)
        cursor.execute(query)
        jobsChildren = cursor.fetchall()
    db.close()
    return jobsChildren
