import json
from db_connections import getConnectionLocal
import _mysql_connector
from datetime import date
import re


def Filter_Queue(job):
    return job["status"] == 'queue'


def Filter_Running(job):
    return job["status"] == 'running'


def Filter_Failed(job):
    return job["status"] == 'failed' and job["retries"] > 0


def Filter_OverTryFailure(job):
    return job["status"] == 'failed' and job["retries"] <= 0


def Filter_Failed_ToValidCharge(job):
    return job["status"] == 'failed' and job["retries"] > 0 and job["was_sent"] == False


def Map_IdJobs(job):
    id = job["job_id"]
    return str(f"'{id}'")


def Map_ExternalJobs(job):
    return {
        "job_id": job[0],
        "status": job[1],
        "retries": job[2],
        "parent_id": job[3],
        "params": job[4],
        "errors": job[5],
        "credential_id": job[6]
    }


def Map_InternalJobs(job):
    return {
        "job_id": job[0],
        "status": job[1],
        "retries": job[2],
        "parent_id": job[3],
        "params": job[4],
        "errors": job[5],
        "was_sent": job[6],
        "isInvalidcredential": job[7],
        "credential_id": job[8]
    }


def Get_StartDate(_json: str) -> str:
    _dict = json.loads(_json)
    key = "startDate"
    value = ""
    if (_dict.__contains__("startDate")):
        value = _dict[key]

    elif (_dict.__contains__("startDateBrazil")):
        arrayValue = _dict["startDateBrazil"].split("/")
        value = f"{arrayValue[2]}-{arrayValue[1]}-{arrayValue[0]}"

    return {"str": f'"{key}": "{value}"', "key": key, "value": value}


def Get_EndDate(_json: str) -> str:
    _dict = json.loads(_json)
    key = "endDate"
    value = ""

    if (_dict.__contains__("endDate")):
        value = _dict[key]
    elif (_dict.__contains__("endDateBrazil")):
        arrayValue = _dict["endDateBrazil"].split("/")
        value = f"{arrayValue[2]}-{arrayValue[1]}-{arrayValue[0]}"

    return {"str": f'"{key}": "{value}"', "key": key, "value": value}


def Get_IdCharge(idJob: str):
    db = getConnectionLocal()
    cursor = db.cursor()
    cursor.execute(
        f"SELECT id_charge FROM job WHERE job_id = '{idJob}'")
    result = cursor.fetchone()
    db.close()
    return result[0]


def GetNumberOfDaysBetweenTwoDates(d1, d2) -> int:
    _d1 = d1.split("-")
    _d2 = d2.split("-")
    startDate = date(int(_d1[0]), int(_d1[1]), int(_d1[2]))
    endDate = date(int(_d2[0]), int(_d2[1]), int(_d2[2]))
    differ = endDate - startDate
    return int(differ.days)


def IsErrorInvalidCredential(errorMessage) -> bool:
    if (errorMessage is None):
        return False
    regex = re.search(
        "InvalidCredential: This credential is invalid", errorMessage)
    return bool(regex)
