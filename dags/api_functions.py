
url_base_base = "http://host.docker.internal:3005/"

def SendToAPI(idCredentials):
    import requests
    from uuid import uuid4
    charges = []
    for idCredencial in idCredentials:
        idCarga = str(uuid4())
        request = requests.get(
            f"http://host.docker.internal:3005/criar_carga2?id_charge={idCarga}&id_credential={idCredencial}")
        jobsId = request.json()
        charges.append(
            {"idCarga": idCarga, "idCredencial": idCredencial, "idJobs": jobsId})
    return charges

def ResendJobs(idCharge):
    import requests
    request = requests.get(
        url_base_base + "resend_jobs_failed/?" + "id_charge=" + idCharge)
    result = request.json()
    return result

# def Find_JobsByIds(jobs):
#     db = getConexaoProd()
#     cursor = db.cursor()

#     def MapearIds(x):
#         return str(f"'{x[0]}'")

#     jobsId = ','.join(map(MapearIds, jobsPendentes))
#     query = BQ_SelectJobs(jobsId)
#     print(query)
#     cursor.execute(query)
#     result = cursor.fetchall()
#     db.close()