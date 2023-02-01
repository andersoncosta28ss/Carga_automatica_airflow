
import requests
import json
from utils_functions import Get_EndDate, Get_StartDate, Get_IdCharge, GetNumberOfDaysBetweenTwoDates
url_base_base = "http://host.docker.internal:3005/"


def Local_SendToAPI(idCredentials):
    import requests
    from uuid import uuid4
    charges = []
    for idCredencial in idCredentials:
        url = url_base_base + "create_charge"
        payload = json.dumps({"pastDays": 365, "splitDayInterval": 30})
        headers = {'Content-Type': 'application/json'}
        response = requests.request("POST", url, headers=headers, data=payload)
        charge = response.json()
        charges.append(
            {"idCarga": charge["uuid"], "idCredencial": idCredencial, "idJobs": charge["children"]})
    return charges


def Local_SplitJob(failedJobs):
    jobs = []
    for job in failedJobs:
        id = job[0]
        status = job[1]
        retries = job[2]
        parent_id = job[3]
        params = job[4]
        credential_id = job[5]
        startDate = Get_StartDate(params)
        endDate = Get_EndDate(params)
        splitDayInterval = f'"splitDayInterval": {int(GetNumberOfDaysBetweenTwoDates(startDate["value"], endDate["value"]) / 2)}' 
        payload = str({startDate["str"], endDate["str"], splitDayInterval}).replace("'", '')
        if (startDate["value"] == endDate["value"]):
            continue

        idCharge = Get_IdCharge(id)

        response = requests.request("POST", url=url_base_base+"splitjob", headers={"Content-Type": "application/json"}, data=payload)
        result = response.json()

        jobs.append({"idCredential": credential_id, "idJobs": result["children"], "idCharge": idCharge, "parent_id": id})
    return jobs


def Prod_SendToAPI(idCredentials, envs):
    charges = []
    for idCredencial in idCredentials:
        payload = json.dumps({
            "queue": "sbot-input",
            "action": "contract-fetch",
            "retries": 1,
            "credentialId": idCredencial,
            "priority": "normal",
            "procedure": [{"script": "{insurer}/contract-fetch", "params": {"pastDays":  5}}]
        })
        response = requests.request("POST", url=envs.get("API_URL"), headers={
            "Authorization": envs.get("API_AUTHORIZATION"),
            "Content-Type": "application/json"
        }, data=payload)
        charge = response.json()

        charges.append(
            {"idCharge": charge["uuid"], "idJobs": charge["children"]})
    return charges


def Prod_SplitJob(failedJobs, envs):
    jobs = []
    for job in failedJobs:
        id = job[0]
        status = job[1]
        retries = job[2]
        parent_id = job[3]
        params = json.loads(job[4])
        credential_id = json[5]
        startDate = Get_StartDate(params)
        endDate = Get_EndDate(params)
        splitDayInterval = f'"splitDayInterval": {int(GetNumberOfDaysBetweenTwoDates(startDate["value"], endDate["value"]) / 2)}'
        if (startDate["value"] == endDate["value"]):
            continue

        idCharge = Get_IdCharge(id)
        params =  str({startDate["str"], endDate["str"], splitDayInterval}).replace("'", '')
        payload = json.dumps({
            "queue": "sbot-input",
            "action": "contract-fetch",
            "retries": 1,
            "credentialId": credential_id,
            "priority": "normal",
            "procedure": [{"script": "{insurer}/contract-fetch", "params": params}]
        })
        response = requests.request("POST", url=envs.get("API_URL"), headers={
            "Authorization": envs.get("API_AUTHORIZATION"),
            "Content-Type": "application/json"
        }, data=payload)
        result = response.json()

        jobs.append({"idCredential": credential_id, "idJobs": result["children"], "idCharge": idCharge, "parent_id": id})
    return jobs
