from db_connections import getConnectionLocal, getConnectionLocal2, getConnectionProd, getConnectionBQ
from db_query import Query_Local_SelectCrendetial
from utils_functions import Map_IdJobs

# region Local


def Local_Filter_Credentials(credentials):
    credentialsToContinue = []
    db = getConnectionLocal()
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
    db = getConnectionLocal()
    cursor = db.cursor()
    query = "SELECT id FROM charge WHERE status = 'running'"
    cursor.execute(query)
    idCharges = cursor.fetchall()
    db.close()
    return idCharges


def Local_Select_PendingJobs():
    pendingJobs = []
    db = getConnectionLocal()
    cursor = db.cursor()
    cursor.execute(
        f"SELECT id, status, was_sent, retries, id_parent FROM job WHERE was_sent = false AND status NOT IN('done', 'timeout') AND isInvalidCredential = false")
    pendingJobs = cursor.fetchall()
    db.close()
    db.disconnect()
    return pendingJobs


def Local_Update_Charge(idCharge, state):
    db = getConnectionLocal()
    cursor = db.cursor()
    cursor.execute(
        f"UPDATE charge SET status = '{state}' WHERE id = '{idCharge}'")
    db.commit()
    db.close()

# region somente para ambiente de desenvolvimento


def Local2_Select_JobsByIds(jobs):
    db = getConnectionLocal2()
    cursor = db.cursor()
    idJobs = ','.join(map(Map_IdJobs, jobs))
    cursor.execute(
        f"SELECT id, status, retries, id_parent FROM job WHERE id IN ({idJobs})")
    resultJobs = cursor.fetchall()
    cursor = db.cursor()
    db.close()
    return resultJobs


def Local2_Select_JobsChildrenByIdParent(jobs):
    jobsChildren = []
    db = getConnectionLocal2()
    cursor = db.cursor()
    idfailedJobs = ','.join(map(Map_IdJobs, jobs))
    if (len(idfailedJobs) > 0):
        cursor.execute(
            f"SELECT id, status, retries, id_parent FROM job WHERE id_parent IN ({idfailedJobs})")
        jobsChildren = cursor.fetchall()
    db.close()
    return jobsChildren


def Local2_Select_Credentials():
    db = getConnectionLocal2()
    cursor = db.cursor()
    cursor.execute(
        "SELECT id FROM credential WHERE create_at >= DATE_SUB(NOW(), interval 30 SECOND)")
    credentials = cursor.fetchall()
    db.close()
    return credentials
# endregion

# endregion

# region Prod & BQ


def Prod_Select_Credentials(envs):
    db = getConnectionProd(envs)
    cursor = db.cursor()
    cursor.execute(
        "SELECT id FROM corretoras_senhas WHERE created >= DATE_SUB(NOW(), interval 30 SECOND)")
    credentials = cursor.fetchall()
    db.close()
    return credentials


def BQ_Select_JobsByIds(jobs, envs):
    print("---- Começa -----")
    print(jobs)
    db = getConnectionBQ(envs)
    idJobs = ','.join(map(Map_IdJobs, jobs))
    query_job = db.query(
        f"SELECT job_id, status, retries, parent_id, params, credential_id, errors FROM sossego-data-bi-stage.sossegobot.sbot_jobs WHERE job_id IN ({idJobs})")
    result = [list(dict(row).values()) for row in query_job]
    print(result)
    db.close()
    return result


def BQ_Select_JobsChildrenByIdParent(jobs, envs):
    jobsChildren = []
    db = getConnectionBQ(envs)
    idfailedJobs = ','.join(map(Map_IdJobs, jobs))
    if (len(idfailedJobs) > 0):
        query_job = db.query(
            f"SELECT job_id, status, retries, parent_id, params, credential_id FROM sossego-data-bi-stage.sossegobot.sbot_jobs WHERE id_parent IN ({idfailedJobs})")
        jobsChildren = [list(dict(row).values()) for row in query_job]
    db.close()
    return jobsChildren
# endregion
