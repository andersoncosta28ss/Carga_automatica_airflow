
import requests
import json
url_base_base = "http://host.docker.internal:3005/"

def Local_SendToAPI(idCredentials):
    import requests
    from uuid import uuid4
    charges = []
    for idCredencial in idCredentials:
        idCarga = str(uuid4())
        response = requests.get(f"http://host.docker.internal:3005/criar_carga2?id_charge={idCarga}&id_credential={idCredencial}")
        jobsId = response.json()
        charges.append(
            {"idCarga": idCarga, "idCredencial": idCredencial, "idJobs": jobsId})
    return charges


def Local_ResendJobs(idCharge):
    response = requests.get(
        url_base_base + "resend_jobs_failed/?" + "id_charge=" + idCharge)
    result = response.json()
    return result


def Prod_SendToAPI(idCredentials, envs):
    charges = []
    for idCredencial in idCredentials:
        payload = json.dumps({
            "queue": "sbot-input",
            "action": "contract-fetch",
            "retries": 1,
            "credentialId": idCredencial,
            "priority": "normal",
            "procedure": [{"script": "{insurer}/contract-fetch", "params": {"pastDays":  1}}]
        })
        response = requests.request("POST", url=envs.get("API_URL"), headers={
            "Authorization": envs.get("API_AUTHORIZATION"),
            "Content-Type": "application/json"
        }, data=payload)
        charge = response.json()
        
        charges.append(
            {"idCarga": charge["uuid"], "idCredencial": idCredencial, "idJobs": charge["children"]})        
    return charges