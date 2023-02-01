from db_connections import getConnectionLocal, getConnectionLocal2, getConnectionProd, getConnectionBQ
from db_query import Query_Local_Select_Crendetial, Query_Local_Select_JobsFromIdCharge
from utils_functions import Map_IdJobs, Filter_Failed_ToValidCharge, Filter_Queue, Filter_OverTryFailure, Filter_Running

# region Local


def Local_Filter_Credentials(credentials):
    credentialsToContinue = []
    db = getConnectionLocal()
    cursor = db.cursor()
    for credential in credentials:
        idCredential = credential[0]

        query = Query_Local_Select_Crendetial(idCredential)
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
        "SELECT job_id, status, was_sent, retries, parent_id FROM job WHERE was_sent = false AND status NOT IN('done', 'timeout') AND isInvalidCredential = false")
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


def Local_HandleCharge():
    charges = Local_Select_PendingCharges()
    # Se tentarmos usar a função de Local_Find_PendingChargesByCharge dá erro de excesso de conexão, sem sentido nenhum
    db = getConnectionLocal()
    cursor = db.cursor()
    for charge in charges:
        idCharge = charge[0]
        query = (Query_Local_Select_JobsFromIdCharge(idCharge))
        cursor.execute(query)
        jobs = cursor.fetchall()
        jobs_EmFila = list(filter(Filter_Queue, jobs))
        jobs_Falhos = list(filter(Filter_Failed_ToValidCharge, jobs))
        jobs_FalhosPorExcessoDeTentativa = list(
            filter(Filter_OverTryFailure, jobs))
        jobs_Rodando = list(filter(Filter_Running, jobs))
        jobs_pendentes = len(jobs_EmFila) > 0 or len(
            jobs_Falhos) > 0 or len(jobs_Rodando) > 0
        if (jobs_pendentes):
            continue

        else:
            state = 'Partially_Done' if len(
                jobs_FalhosPorExcessoDeTentativa) > 0 else 'Done'
            Local_Update_Charge(idCharge, state)
    db.close()
# region somente para ambiente de desenvolvimento


def Local2_Select_JobsByIds(jobs):
    db = getConnectionLocal2()
    cursor = db.cursor()
    idJobs = ','.join(map(Map_IdJobs, jobs))
    cursor.execute(
        f"SELECT job_id, status, retries, parent_id, params, credential_id, errors FROM job WHERE job_id IN ({idJobs})")
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
            f"SELECT job_id, status, retries, parent_id, params, credential_id, errors FROM job WHERE parent_id IN ({idfailedJobs})")
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
        "SELECT id FROM corretoras_senhas WHERE created >= DATE_SUB(NOW(), interval 30 SECOND) AND loginvalido = true")
    credentials = cursor.fetchall()
    db.close()
    return credentials


def BQ_Select_JobsByIds(jobs, envs):
    db = getConnectionBQ(envs)
    idJobs = ','.join(map(Map_IdJobs, jobs))
    query_job = db.query(
        f"SELECT job_id, status, retries, parent_id, params, credential_id, errors FROM sossego-data-bi-stage.sossegobot.sbot_jobs WHERE job_id IN ({idJobs})")
    result = [list(dict(row).values()) for row in query_job]
    db.close()
    return result


def BQ_Select_JobsChildrenByIdParent(jobs, envs):
    jobsChildren = []
    db = getConnectionBQ(envs)
    idfailedJobs = ','.join(map(Map_IdJobs, jobs))
    if (len(idfailedJobs) > 0):
        query_job = db.query(
            f"SELECT job_id, status, retries, parent_id, params, credential_id FROM sossego-data-bi-stage.sossegobot.sbot_jobs WHERE parent_id IN ({idfailedJobs})")
        jobsChildren = [list(dict(row).values()) for row in query_job]
    db.close()
    return jobsChildren
# endregion
