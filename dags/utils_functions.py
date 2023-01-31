import json
from db_connections import getConnectionLocal
import _mysql_connector
from datetime import date
import re

def Filter_Queue(job):
    return job[1] == 'queue'


def Filter_Running(job):
    return job[1] == 'running'


def Filter_Failed_Local(job):
    return job[1] == 'failed' and job[3] > 0 and job[2] == False


def Filter_OverTryFailure(job):
    return job[1] == 'failed' and job[3] <= 0


def Filter_Failed_BQ(job):
    return job[1] == 'failed' and job[2] > 0


def Map_IdJobs(job):
    return str(f"'{job[0]}'")

def Get_StartDate(_json: str) -> str:
    _dict = json.loads(_json)
    key = "startDate"
    value = ""
    if (_dict.__contains__("startDate")):
        value = _dict[key]

    elif (_dict.__contains__("startDateBrazil")):   
        arrayValue = _dict["startDateBrazil"].split("/")
        value = f"{arrayValue[2]}-{arrayValue[1]}-{arrayValue[0]}"

    return {"str": f'"{key}": {value}', "key": key, "value": value}


def Get_EndDate(_json: str) -> str:
    _dict = json.loads(_json)
    key = "endDate"
    value = ""

    if (_dict.__contains__("endDate")):
        value = _dict[key]
    elif (_dict.__contains__("endDateBrazil")):
        arrayValue = _dict["endDateBrazil"].split("/")
        value = f"{arrayValue[2]}-{arrayValue[1]}-{arrayValue[0]}"

    return {"str": f'"{key}": {value}', "key": key, "value": value}

def Get_IdCharge(idJob: str):
    db = getConnectionLocal()
    cursor = db.cursor()
    cursor.execute(f"SELECT id_parent, id_charge FROM job WHERE id = '{idJob}'")
    result = cursor.fetchone()[1]
    db.close()

def GetNumberOfDaysBetweenTwoDates(d1, d2) -> int:
    _d1 = d1.split("-")
    _d2 = d2.split("-")
    startDate = date(int(_d1[0]), int(_d1[1]), int(_d1[2]))
    endDate = date(int(_d2[0]), int(_d2[1]), int(_d2[2]))
    differ = endDate - startDate
    return int(differ.days/2)

def IsErrorInvalidCredential(errorMessage) -> bool:
    if(errorMessage is None):
        return False
    regex = re.search("InvalidCredential: This credential is invalid", errorMessage)
    return bool(regex)