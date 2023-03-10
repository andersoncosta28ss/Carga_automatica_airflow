import json
from db_connections import getConnectionLocal
import datetime
from utils_conts import JobStatus

import re


def Filter_Queued(job):
    return job["status"] == JobStatus.Queued.value 


def Filter_Running(job):
    return job["status"] == JobStatus.Running.value 


def BQ_Filter_Failed(job):
    return (job["status"] == JobStatus.Failed.value or job["status"] == JobStatus.Timeout.value) and job["retries"] > 0


def BQ_Filter_OverTryFailure(job):
    return (job["status"] == JobStatus.Failed.value or job["status"] == JobStatus.Timeout.value) and job["retries"] == 0


def Local_Filter_Failed(job):
    return (job["status"] == JobStatus.Failed.value or job["status"] == JobStatus.Timeout.value) and job["retries"] > 0 and job["was_sent"] == False and job["numberOfDay"] > 1


def Local_Filter_OverTryFailure(job):
    return (job["status"] == JobStatus.Failed.value or job["status"] == JobStatus.Timeout.value or job["status"] == 'stale') and job["retries"] == 0 and job["numberOfDays"] == 1


def BQ_Filter_Stale(job):
    return (job["status"] == JobStatus.Running.value) and ExceededExecutionTime(job) == True


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
        "credential_id": job[6],
        "updated": datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S') if job[7] is None else job[7].strftime('%Y-%m-%d %H:%M:%S')
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
        "credential_id": job[8],
        "numberOfDays": job[9]
    }


def Get_StartDate(_json: str) -> str:
    _dict: dict = json.loads(_json)
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


def Get_IdCharge(idJob: str, envs):
    db = getConnectionLocal(envs)
    cursor = db.cursor()
    cursor.execute(
        f"SELECT charge_id FROM job WHERE job_id = '{idJob}'")
    result = cursor.fetchone()
    db.close()
    return result[0]


def GetNumberOfDaysBetweenTwoDates(d1, d2) -> int:
    _d1 = d1.split("-")
    _d2 = d2.split("-")
    startDate = datetime.date(int(_d1[0]), int(_d1[1]), int(_d1[2]))
    endDate = datetime.date(int(_d2[0]), int(_d2[1]), int(_d2[2]))
    differ = endDate - startDate
    return int(differ.days)


def IsErrorInvalidCredential(errorMessage) -> bool:
    if (errorMessage is None):
        return False
    regex = re.search(
        "InvalidCredential: This credential is invalid", errorMessage)
    return bool(regex)


def ExceededExecutionTime(jobProd: str):
    updateDate: datetime.datetime = datetime.datetime.strptime(jobProd["updated"], '%Y-%m-%d %H:%M:%S')
    _now = datetime.datetime.now()
    diff: datetime.timedelta = _now - updateDate
    return diff.seconds > 43200

def GetErrors(error: str) -> str:
    return "" if error is None else str(error).split("\n")[0].replace("'", '"')[:255][0]
