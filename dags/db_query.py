from utils_functions import Get_IdCharge, IsErrorInvalidCredential
import json
# region Local

def Query_Local_Insert_ChildrenJob(jobs):
    query = ""
    if (len(jobs) == 0):
        return "SELECT 0"
    for job in jobs:
        id = job[0]
        status = job[1]
        retries = job[2]
        parent_id = job[3]
        params = json.dumps(job[4])
        id_charge = Get_IdCharge(parent_id)
        query += f"""
                    INSERT INTO job(job_id, status, retries, parent_id, id_charge, params) VALUES('{id}','{status}', {retries}, '{parent_id}', '{id_charge}', {params});
                    UPDATE job SET was_sent = true WHERE job_id = '{parent_id}';
        """
    return query


def Query_Local_Insert_Charge(charges):
    query = ""
    if (len(charges) == 0):
        return "SELECT 0"

    for charge in charges:
        idCharge = charge['idCharge']
        idCredential = charge['idCredential']
        idJobs = charge['idJobs']
        query += f"INSERT INTO credential(id) VALUES({idCredential});"
        query += f"INSERT INTO charge (id, credential_id) values('{idCharge}', '{idCredential}');"
        paramsDefault = {}
        for idJob in idJobs:
            query += f"INSERT INTO job (job_id, id_charge, params) values('{idJob}', '{idCharge}', '{paramsDefault}');"
    return query


def Query_Local_Insert_Splited_Jobs(jobsWithCredential):
    query = ""
    if(len(jobsWithCredential) == 0):
        return "SELECT 0"
    for jWc in jobsWithCredential:
        idJobs = jWc["idJobs"]
        idCharge = jWc["idCharge"]
        parent_id = jWc["parent_id"]
        idCredential = jWc["idCredential"]
        print("A Credencial é None? -> " + str(idCredential is None))
        params = '{}'
        query += f"UPDATE job SET was_sent = true WHERE job_id = '{parent_id}';"

        for idJob in idJobs:
            query += f"INSERT INTO job (job_id, id_charge, params) values('{idJob}', '{idCharge}', '{params}');"

    return query


def Query_Local_Select_Crendetial(idCredential):
    return f"SELECT * FROM charge WHERE credential_id = {idCredential}"


def Query_Local_Select_JobsFromIdCharge(idCharge):
    return f"SELECT job_id, status, was_sent, retries, parent_id FROM job WHERE id_charge = '{idCharge}' AND status <> 'done'"


def Query_Local_Update_Job(jobs):
    query = ""
    if (len(jobs) == 0):
        return "SELECT 0"
    for job in jobs:
        isInvalidCredential = IsErrorInvalidCredential(job[6])
        params = json.dumps(job[4])
        query += f"UPDATE job SET status = '{job[1]}', retries = {job[2]}, parent_id = '{job[3]}', params = {params}, isInvalidCredential = {isInvalidCredential} WHERE job_id = '{job[0]}';"
    return query

# endregion
