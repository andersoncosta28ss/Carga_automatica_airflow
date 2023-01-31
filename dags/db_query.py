from db_connections import getConnectionLocal
from utils_functions import Get_IdCharge, IsErrorInvalidCredential
# region Local
def Query_Local_SelectPendingJobs():
    return f"SELECT id, status, was_sent, retries, id_parent FROM job WHERE was_sent = false AND status <> 'done'"

def Query_Local_InsertChildrenJob(jobs):
    query = ""
    if(len(jobs) == 0):
        query = "SELECT 0"
    for job in jobs:
        id = job[0]
        status = job[1]
        retries = job[2]
        id_parent = job[3]
        id_charge = Get_IdCharge(id_parent)
        query += f"""
                    INSERT INTO job(id, status, retries, id_parent, id_charge) VALUES('{id}','{status}', {retries}, '{id_parent}', '{id_charge}');
                    UPDATE job SET was_sent = true WHERE id = '{id_parent}';
        """
    return query

def Query_Local_SelectJobsFromIdCharge(idCharge):
    return f"SELECT id, status, was_sent, retries, id_parent FROM job WHERE id_charge = '{idCharge}' AND status <> 'done'"

def Query_Local_UpdateJob(jobs):
    query = ""
    for job in jobs:
        isInvalidCredential = IsErrorInvalidCredential(job[6])
        query += f"UPDATE job SET status = '{job[1]}', retries = {job[2]}, id_parent = '{job[3]}', isInvalidCredential = {isInvalidCredential} WHERE id = '{job[0]}';"
    return query


def Query_Local_InsertCharge(charges):
    query = ""
    for charge in charges:
        idCharge = charge['idCarga']
        idCredential = charge['idCredencial']
        idJobs = charge['idJobs']
        query += f"INSERT INTO credential(id) VALUES({idCredential});"
        query += f"INSERT INTO charge (id, credential_id) values('{idCharge}', '{idCredential}');"
        for idJob in idJobs:
            query += f"INSERT INTO job (id, id_charge) values('{idJob}', '{idCharge}');"
    return query


def Query_Local_SelectCrendetial(idCredential):
    return f"SELECT * FROM charge WHERE credential_id = {idCredential}"

def Query_Local_Insert_Splited_Jobs(jobsWithCredential):
    query = ""
    for jWc in jobsWithCredential:
        idJobs = jWc["idJobs"]
        idCredential = jWc["idCredential"]
        idCharge = jWc["idCharge"]
        for idJob in idJobs:
            query += f"INSERT INTO job (id, id_charge) values('{idJob}', '{idCharge}');"

    return query



# endregion
