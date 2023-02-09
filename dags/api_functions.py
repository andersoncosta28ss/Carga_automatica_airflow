import requests
import json
from utils_functions import Get_EndDate, Get_StartDate, Get_IdCharge, GetNumberOfDaysBetweenTwoDates
url_base_base = "http://host.docker.internal:3005/"


def Local_SendToAPI(idCredentials):
    import requests
    charges = []
    for idCredential in idCredentials:
        url = url_base_base + "create_charge"
        payload = json.dumps({"pastDays": 365, "splitDayInterval": 30})
        headers = {'Content-Type': 'application/json'}
        response = requests.request("POST", url, headers=headers, data=payload)
        charge = response.json()
        charges.append(
            {"idCharge": charge["uuid"], "credentialId": idCredential, "idJobs": charge["children"]})
    return charges


def Local_SplitJob(failedJobs):
    jobs = []
    for job in failedJobs:
        id = job["job_id"]
        status = job["status"]
        retries = job["retries"]
        parent_id = job["parent_id"]
        params = job["params"]
        idCharge = Get_IdCharge(id)
        startDate = Get_StartDate(params)
        endDate = Get_EndDate(params)
        splitDayInterval = f'"splitDayInterval": {int(GetNumberOfDaysBetweenTwoDates(startDate["value"], endDate["value"]) / 2)}'
        credential_id = job["credential_id"]
        credentialId = f'"credentialId": {int(credential_id)}'
        chargeId = f'"chargeId: {int(idCharge)}"'

        payload = str({startDate["str"], endDate["str"],
                      splitDayInterval, credentialId, chargeId}).replace("'", '')
        if (startDate["value"] == endDate["value"]):
            continue

        response = requests.request("POST", url=url_base_base+"splitjob", headers={
                                    "Content-Type": "application/json"}, data=payload)
        result = response.json()

        jobs.append({"idCredential": credential_id,
                    "idJobs": result["children"], "idCharge": idCharge, "parent_id": id})
    return jobs


def Prod_SendToAPI(idCredentials, envs):
    charges = []
    for idCredential in idCredentials:
        payload = json.dumps({
            "queue": "sbot-input",
            "action": "contract-fetch",
            "retries": 1,
            "credentialId": idCredential,
            "priority": "normal",
            "procedure": [{"script": "{insurer}/contract-fetch", "params": {"pastDays":  90, "splitDayInterval": 30}}]
        })
        response = requests.request("POST", url=envs.get("API_URL"), headers={
            "Authorization": envs.get("API_AUTHORIZATION"),
            "Content-Type": "application/json"
        }, data=payload)
        charge = response.json()
        if (charge.__contains__("error")):
            continue
        charges.append(
            {"idCharge": charge["uuid"], "idJobs": charge["children"], "idCredential": idCredential})
    return charges


def Prod_SplitJob(failedJobs, envs):
    jobs = []
    for job in failedJobs:
        id = job["job_id"]
        status = job["status"]
        retries = job["retries"]
        parent_id = job["parent_id"]
        params = job["params"]
        credential_id = int(job["credential_id"])
        startDate = Get_StartDate(params)
        endDate = Get_EndDate(params)
        splitDayInterval = int(GetNumberOfDaysBetweenTwoDates(
            startDate["value"], endDate["value"]) / 2)
        if (startDate["value"] == endDate["value"]):
            continue

        idCharge = Get_IdCharge(id)
        params = {"startDate": startDate["value"],
                  "endDate": endDate["value"], "splitDayInterval": splitDayInterval
                  }
        payload = json.dumps({
            "queue": "sbot-input",
            "action": "contract-fetch",
            "retries": 1,
            "credentialId": credential_id,
            "priority": "normal",
            "parentId": id,
            "procedure": [
                {
                    "script": "{insurer}/contract-fetch",
                    "params": params
                }
            ]
        })
        response = requests.request(
            "POST",
            url=envs.get("API_URL"),
            headers={
                "Authorization": envs.get("API_AUTHORIZATION"),
                "Content-Type": "application/json"
            },
            data=payload
        )
        result = response.json()

        jobs.append({
            "idCredential": credential_id,
            "idJobs": result["children"],
            "idCharge": idCharge,
            "parent_id": id
        })
    return jobs


def Prod_SendStaleJob(staleJobs, envs):
    jobs = []
    for job in staleJobs:
        id = job["job_id"]
        status = job["status"]
        retries = job["retries"]
        parent_id = job["parent_id"]
        params = job["params"]
        credential_id = int(job["credential_id"])
        startDate = Get_StartDate(params)
        endDate = Get_EndDate(params)

        idCharge = Get_IdCharge(id)
        params = {
            "startDate": startDate["value"],
            "endDate": endDate["value"]
        }

        payload = json.dumps({
            "queue": "sbot-input",
            "action": "contract-fetch",
            "retries": 1,
            "credentialId": credential_id,
            "priority": "normal",
            "parentId": id,
            "procedure": [
                {
                    "script": "{insurer}/contract-fetch",
                    "params": params
                }
            ]
        })
        response = requests.request(
            "POST",
            url=envs.get("API_URL"),
            headers={
                "Authorization": envs.get("API_AUTHORIZATION"),
                "Content-Type": "application/json"
            },
            data=payload
        )
        result = response.json()

        jobs.append({
            "idCredential": credential_id,
            "idJobs": result["children"],
            "idCharge": idCharge,
            "parent_id": id
        })
    return jobs
