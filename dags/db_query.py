from utils_functions import Get_IdCharge, IsErrorInvalidCredential
from utils_conts import SQL_JOB_DefaultExternalFields, SQL_JOB_DefaultInternalFields
import json
from utils_functions import Get_EndDate, Get_StartDate, Get_IdCharge, GetNumberOfDaysBetweenTwoDates

# region Local


def Query_Local_Insert_ChildrenJob(jobs):
    query = ""
    if (len(jobs) == 0):
        return "SELECT 0"
    for job in jobs:
        id = job["job_id"]
        status = job["status"]
        retries = job["retries"]
        parent_id = "" if job["parent_id"] is None else job["parent_id"]
        params = json.dumps(job["params"])
        errors = "" if job["errors"] is None else str(job["errors"]).split("\n")[0].replace("'", '"')
        credential_id = job["credential_id"]
        charge_id = Get_IdCharge(parent_id)
        query += f"""
                    INSERT INTO job({SQL_JOB_DefaultExternalFields}, charge_id) VALUES('{id}','{status}', {retries}, '{parent_id}', {params}, '{errors}', '{credential_id}' ,'{charge_id}');
                    UPDATE job SET was_sent = true WHERE job_id = '{parent_id}';
        """
    return query


def Query_Local_Update_Job(jobs):
    query = ""
    if (len(jobs) == 0):
        return "SELECT 0"
    for job in jobs:
        id = job["job_id"]
        status = job["status"]
        retries = job["retries"]
        parent_id = "" if job["parent_id"] is None else job["parent_id"]
        credential_id = "" if job["credential_id"] is None else job["credential_id"]
        params = job["params"]
        startDate = Get_StartDate(params)
        endDate = Get_EndDate(params)
        numberOfDays = int(GetNumberOfDaysBetweenTwoDates(startDate["value"], endDate["value"]))
        errors = "" if job["errors"] is None else str(job["errors"]).split("\n")[0].replace("'", '"')
        isInvalidCredential = IsErrorInvalidCredential(errors)
        query += f"UPDATE job SET status = '{status}', retries = {retries}, parent_id = '{parent_id}', params = {json.dumps(params)}, errors = '{errors}', isInvalidCredential = {isInvalidCredential}, credential_id = {credential_id}, numberOfDays = {numberOfDays} WHERE job_id = '{id}';"
    return query


def Query_Local_Insert_Splited_Jobs(jobs):
    query = ""
    if (len(jobs) == 0):
        return "SELECT 0"
    for job in jobs:
        idJobs = job["idJobs"]
        idCharge = job["idCharge"]
        parent_id = "" if job["parent_id"] is None else job["parent_id"]
        jobFromSplited = parent_id
        params = {}
        query += f"UPDATE job SET was_sent = true WHERE job_id = '{parent_id}';"

        for idJob in idJobs:
            query += f"INSERT INTO job (job_id, charge_id, params, jobFromSplited) values('{idJob}', '{idCharge}', '{params}', '{jobFromSplited}');"

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
            query += f"INSERT INTO job (job_id, charge_id, params, credential_id) values('{idJob}', '{idCharge}', '{paramsDefault}', {idCredential});"
    return query


# endregion
