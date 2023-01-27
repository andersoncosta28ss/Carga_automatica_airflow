from db_connections import getConexaoLocal, getConexaoProd, getConexaoStage
from db_query import Query_Local_InsertJobs, Query_Local_InsertCharge, Query_Local_InsertCredential, Query_Local_SelectCrendetial, Query_Prod_SelectCredential, Query_Local_InsertJobResents, Query_Local_SelectJobsFromIdCharge, Query_BQ_SelectJobs, Query_Local_UpdateJob, Query_Local_UpdateCharge as Query_LocalUpdateCharge

def Prod_Find_credentials(envs = None):
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

def Local_Filter_credentials(credentials):
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

def Local_CreateCharges(charges):
    db = getConexaoLocal()
    cursor = db.cursor()
    for charge in charges:
        idCharge = charge['idCarga']
        idCredential = charge['idCredencial']
        jobsId = charge['idJobs']
        query = Query_Local_InsertCredential(idCredential)
        cursor.execute(query)
        db.commit()
        query = Query_Local_InsertCharge(idCharge, idCredential)
        cursor.execute(query)
        for idJob in jobsId:
            query = Query_Local_InsertJobs(idJob, idCharge)
            cursor.execute(query)
    db.commit()
    db.close()

def Local_InsertJobsResend(jobs):
    db = getConexaoLocal()
    cursor = db.cursor()
    for job in jobs:
        query = Query_Local_InsertJobResents(job)
        cursor.execute(query)
    db.commit()
    db.close()    

def Local_Find_PendingCharges():
    db = getConexaoLocal()
    cursor = db.cursor()
    query = "SELECT id FROM charge WHERE status = 'Running'"
    cursor.execute(query)
    idCharges = cursor.fetchall()
    db.close()
    return idCharges

def Local_Find_PendingJobsByCharge(charges):
    pendingJobs = []
    db = getConexaoLocal()
    cursor = db.cursor()
    for charge in charges:
        idCharge = charge[0]
        query = Query_Local_SelectJobsFromIdCharge(idCharge)
        cursor.execute(query)
        pendingJobs += cursor.fetchall()
    db.close()
    db.disconnect()
    return pendingJobs

def BQ_Find_JobsByIds(jobs):
    db = getConexaoProd()
    cursor = db.cursor()

    def MapearIds(x):
        return str(f"'{x[0]}'")

    jobsId = ','.join(map(MapearIds, jobs))
    query = Query_BQ_SelectJobs(jobsId)
    cursor.execute(query)
    result = cursor.fetchall()
    db.close()
    return result

def Local_UpdateCharge(idCharge, state):
    db = getConexaoLocal()
    cursor = db.cursor()
    query = Query_LocalUpdateCharge(idCharge, state)
    cursor.execute(query)
    db.commit()
    db.close()

def Local_UpdateJobs(jobs):
    db = getConexaoLocal()
    cursor = db.cursor()
    for job in jobs:
        query = Query_Local_UpdateJob(job)
        cursor.execute(query)
    db.commit()
    db.close()