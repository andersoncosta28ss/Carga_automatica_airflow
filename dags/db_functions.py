from db_connections import getConnectionLocal, getConnectionLocal2, getConnectionProd, getConnectionBQ
from utils_functions import Map_IdJobs, Map_ExternalJobs, Map_InternalJobs, GetNumberOfDaysBetweenTwoDates, Get_StartDate, Get_EndDate
from utils_conts import SQL_JOB_DefaultExternalFields, SQL_JOB_DefaultInternalFields

# region Local


def Local_Filter_Credentials(credentials):
    credentialsToContinue = []
    db = getConnectionLocal()
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


def Local_Select_PendingCharges():
    db = getConnectionLocal()
    cursor = db.cursor()
    query = "SELECT id FROM charge WHERE status = 'running'"
    cursor.execute(query)
    idCharges = cursor.fetchall()
    db.close()
    return idCharges


def Local_Select_PendingJobs():
    result = []
    db = getConnectionLocal()
    cursor = db.cursor()
    cursor.execute(
        f"SELECT {SQL_JOB_DefaultInternalFields} FROM job WHERE was_sent = false AND status NOT IN('done') AND isInvalidCredential = false")
    result = cursor.fetchall()
    db.close()
    db.disconnect()
    return list(map(Map_InternalJobs, result))


def Local_Update_Charge(idCharge, state):
    db = getConnectionLocal()
    cursor = db.cursor()
    cursor.execute(
        f"UPDATE charge SET status = '{state}' WHERE id = '{idCharge}'")
    db.commit()
    db.close()


# endregion


# region somente para ambiente de desenvolvimento


def Local2_Select_JobsByIds(jobs):
    db = getConnectionLocal2()
    cursor = db.cursor()
    idJobs = ','.join(map(Map_IdJobs, jobs))
    cursor.execute(
        f"SELECT {SQL_JOB_DefaultExternalFields} FROM job WHERE job_id IN ({idJobs})")
    result = cursor.fetchall()
    cursor = db.cursor()
    db.close()
    return list(map(Map_ExternalJobs, result))


def Local2_Select_JobsChildrenByIdParent(jobs):
    result = []
    db = getConnectionLocal2()
    cursor = db.cursor()
    idJobs = ','.join(map(Map_IdJobs, jobs))
    if (len(idJobs) > 0):
        cursor.execute(
            f"SELECT {SQL_JOB_DefaultExternalFields} FROM job WHERE parent_id IN ({idJobs})")
        result = cursor.fetchall()
    db.close()
    return list(map(Map_ExternalJobs, result))


def Local2_Select_Credentials():
    db = getConnectionLocal2()
    cursor = db.cursor()
    cursor.execute(
        "SELECT id FROM credential WHERE create_at >= DATE_SUB(NOW(), interval 30 SECOND)")
    credentials = cursor.fetchall()
    db.close()
    return credentials
# endregion


# region Prod & BQ


def Prod_Select_Credentials(envs):
    db = getConnectionProd(envs)
    cursor = db.cursor()
    cursor.execute(
        "SELECT id FROM corretoras_senhas WHERE created >= DATE_SUB(NOW(), INTERVAL 10 MINUTE) AND loginvalido = true")
    credentials = cursor.fetchall()
    db.close()
    return credentials


def BQ_Select_JobsByIds(jobs, envs):
    db = getConnectionBQ(envs)
    idJobs = ','.join(map(Map_IdJobs, jobs))
    query = f"SELECT {SQL_JOB_DefaultExternalFields} FROM sossego-data-bi-stage.sossegobot.sbot_jobs WHERE job_id IN ({idJobs})"
    query_job = db.query(query)
    result = list(map(Map_ExternalJobs, query_job))
    db.close()
    return result


def BQ_Select_JobsChildrenByIdParent(jobs, envs):
    result = []
    db = getConnectionBQ(envs)
    idJobs = ','.join(map(Map_IdJobs, jobs))
    if (len(idJobs) > 0):
        query = f"SELECT {SQL_JOB_DefaultExternalFields} FROM sossego-data-bi-stage.sossegobot.sbot_jobs WHERE parent_id IN ({idJobs})"
        query_job = db.query(query)
        result = list(map(Map_ExternalJobs, query_job))
    db.close()
    return result
# endregion

