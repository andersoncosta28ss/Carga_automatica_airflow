from db_connections import getConnectionLocal, getConnectionProd, getConnectionBQ
from utils_functions import Map_IdJobs, Map_ExternalJobs, Map_InternalJobs
from utils_conts import SQL_JOB_Select_DefaultExternalFields, JobStatus, SQL_JOB_Select_DefaultInternalFields, ChargeStatus

# region Local


def Local_Filter_Credentials(credentials, envs):
    credentialsToContinue = []
    db = getConnectionLocal(envs)
    cursor = db.cursor()
    for credential in credentials:
        idCredential = credential[0]

        query = f"SELECT * FROM charge WHERE credential_id = {idCredential}"
        cursor.execute(query)
        consultaInterna = cursor.fetchall()
        if (len(consultaInterna) > 0):
            continue
        else:
            credentialsToContinue.append(idCredential)
    db.close()
    return credentialsToContinue


def Local_Select_PendingCharges(envs):
    db = getConnectionLocal(envs)
    cursor = db.cursor()
    query = f"SELECT id FROM charge WHERE status = '{ChargeStatus.Running.value}'"
    print(query)
    cursor.execute(query)
    idCharges = cursor.fetchall()
    db.close()
    return idCharges


def Local_Select_PendingJobs(envs):
    result = []
    db = getConnectionLocal(envs)
    cursor = db.cursor()
    cursor.execute(f"SELECT {SQL_JOB_Select_DefaultInternalFields} FROM job WHERE was_sent = false AND status NOT IN('{JobStatus.Done.value}') AND isInvalidCredential = false")
    result = cursor.fetchall()
    db.close()
    db.disconnect()
    return list(map(Map_InternalJobs, result))


def Local_Update_Charge(idCharge, state, envs):
    db = getConnectionLocal(envs)
    cursor = db.cursor()
    query = f"UPDATE charge SET status = '{state}' WHERE id = '{idCharge}'"
    cursor.execute(query)
    db.commit()
    db.close()


# endregion


# region Prod & BQ


def Prod_Select_Credentials(envs):
    db = getConnectionProd(envs)
    cursor = db.cursor()
    cursor.execute("SELECT id FROM corretoras_senhas WHERE created >= DATE_SUB(NOW(), INTERVAL 2 HOUR) AND loginvalido = true")
    credentials = cursor.fetchall()
    db.close()
    return credentials


def BQ_Select_JobsByIds(jobs, envs):
    db = getConnectionBQ(envs)
    idJobs = ','.join(map(Map_IdJobs, jobs))
    query = f"SELECT {SQL_JOB_Select_DefaultExternalFields} FROM sossego-data-bi-stage.sossegobot.sbot_jobs WHERE job_id IN ({idJobs})"
    query_job = db.query(query)
    result = list(map(Map_ExternalJobs, query_job.result()))
    db.close()
    return result


def BQ_Select_JobsChildrenByIdParent(jobs, envs):
    result = []
    db = getConnectionBQ(envs)
    idJobs = ','.join(map(Map_IdJobs, jobs))
    if (len(idJobs) > 0):
        query = f"SELECT {SQL_JOB_Select_DefaultExternalFields} FROM sossego-data-bi-stage.sossegobot.sbot_jobs WHERE parent_id IN ({idJobs})"
        query_job = db.query(query)
        result = list(map(Map_ExternalJobs, query_job))
    db.close()
    return result
# endregion

