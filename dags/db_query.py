from utils_functions import Get_IdCharge, IsErrorInvalidCredential
from utils_conts import SQL_JOB_Insert_DefaultInternalFields
import json
from utils_functions import Get_EndDate, Get_StartDate, Get_IdCharge, GetNumberOfDaysBetweenTwoDates, GetErrors


# region Local


def Query_Local_Insert_ChildrenJob(jobs, envs):
    query = ""
    if (len(jobs) == 0):
        return "SELECT 0"
    for job in jobs:
        id = job["job_id"]
        status = job["status"]
        retries = job["retries"]
        parent_id = "" if job["parent_id"] is None else job["parent_id"]
        params = job["params"]
        errors = GetErrors(job["errors"])
        startDate = Get_StartDate(params)
        endDate = Get_EndDate(params)
        numberOfDays = int(GetNumberOfDaysBetweenTwoDates(startDate["value"], endDate["value"]))
        credential_id = job["credential_id"]
        charge_id = Get_IdCharge(parent_id, envs)
        query += f"""
                    INSERT INTO job({SQL_JOB_Insert_DefaultInternalFields}) VALUES('{id}','{status}', {retries}, '{parent_id}', {json.dumps(params)}, '{errors}', '{credential_id}' ,'{charge_id}', {numberOfDays});
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
        errors = GetErrors(job["errors"])
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
        
        params = {}
        query += f"UPDATE job SET was_sent = true WHERE job_id = '{parent_id}';"

        for idJob in idJobs:
            query += f"INSERT INTO job (job_id, charge_id, params, parent_id) values('{idJob}', '{idCharge}', '{params}', '{parent_id}');"

    return query


def Query_Local_Update_Stale_Jobs(jobs):
    query = ""
    if (len(jobs) == 0):
        return "SELECT 0"
    for job in jobs:
        idJobs = job["idJobs"]
        idCharge = job["idCharge"]
        parent_id = "" if job["parent_id"] is None else job["parent_id"]
        
        params = {}
        query += f"UPDATE job SET was_sent = true, status = 'stale' WHERE job_id = '{parent_id}';"

        for idJob in idJobs:
            query += f"INSERT INTO job (job_id, charge_id, params, parent_id) values('{idJob}', '{idCharge}', '{params}', '{parent_id}');"

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
