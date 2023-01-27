from db_connections import getConexaoLocal, getConexaoProd
from db_query import Local_InsertJobs, Local_InsertCharge, Local_InsertCredential, Local_SelectCrendetial, Prod_SelectCredential, Local_InsertJobResents, Local_SelectJobsFromIdCharge, BQ_SelectJobs, Local_UpdateJob, Local_UpdateCharge as Query_LocalUpdateCharge

def Prod_Find_credentials():
    db = getConexaoProd()
    cursor = db.cursor()
    query = Prod_SelectCredential()
    cursor.execute(query)
    credentials = cursor.fetchall()
    db.close()
    return credentials

def Local_Filter_credentials(credentials):
    credentialsToContinue = []
    for credential in credentials:
        idCredential = credential[0]
        db = getConexaoLocal()
        cursor = db.cursor()
        query = Local_SelectCrendetial(idCredential)
        cursor.execute(query)
        consultaInterna = cursor.fetchall()
        if (len(consultaInterna) > 0):
            db.close()
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
        query = Local_InsertCredential(idCredential)
        cursor.execute(query)
        db.commit()
        query = Local_InsertCharge(idCharge, idCredential)
        cursor.execute(query)
        for idJob in jobsId:
            query = Local_InsertJobs(idJob, idCharge)
            cursor.execute(query)
    db.commit()
    db.close()

def Local_InsertJobsResend(jobs):
    db = getConexaoLocal()
    cursor = db.cursor()
    for job in jobs:
        query = Local_InsertJobResents(job)
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

def Local_Find_PendingChargesByCharge(charges):
    db = getConexaoLocal()
    cursor = db.cursor()
    pendingJobs = []
    for charge in charges:
        idCharge = charge[0]
        query = Local_SelectJobsFromIdCharge(idCharge)
        cursor.execute(query)
        pendingJobs += cursor.fetchall()
    db.close()
    return pendingJobs

def BQ_Find_JobsByIds(jobs):
    db = getConexaoProd()
    cursor = db.cursor()

    def MapearIds(x):
        return str(f"'{x[0]}'")

    jobsId = ','.join(map(MapearIds, jobs))
    query = BQ_SelectJobs(jobsId)
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
        query = Local_UpdateJob(job)
        cursor.execute(query)
    db.commit()
    db.close()