#region Local
def Local_SelectJobsFromIdCharge(idCarga):
    return f"SELECT id, status, was_sent, retry, id_parent FROM job WHERE id_charge = '{idCarga}' AND status <> 'Done'"

def Local_InsertJobs(idJob, idCarga):
    return f"INSERT INTO job (id, id_charge) values('{idJob}', '{idCarga}')"


def Local_InsertJobResents(job):
    id = job["id"]
    was_sent = job["was_sent"]
    retry = job["retry"]
    id_charge = job["id_charge"]
    status = job["status"]
    id_parent = job["id_parent"]
    return f"INSERT INTO job(id, status, was_sent, retry, id_parent, id_charge) VALUES('{id}','{status}', {was_sent}, {retry}, '{id_parent}', '{id_charge}')"


def Local_UpdateCharge(idCarga, state):
    return f"UPDATE charge SET status = '{state}' WHERE id = '{idCarga}'"


def Local_UpdateJob(job):
    return f"UPDATE job SET status = '{job[1]}', was_sent = {job[2]}, retry = {job[3]}, id_parent = '{job[4]}' WHERE id = '{job[0]}'"


def Local_InsertCharge(idCarga, idCredencial):
    return f"INSERT INTO charge (id, credential_id) values('{idCarga}', '{idCredencial}')"


def Local_InsertCredential(idCredencial):
    return f"INSERT INTO credential(id) VALUES({idCredencial})"


def Local_SelectCrendetial(idCredencial):
    return f"SELECT * FROM charge WHERE credential_id = {idCredencial}"

#endregion

#region BQ
def BQ_SelectJobs(jobsId):
    return f"SELECT id, status, was_sent, retry, id_parent FROM job WHERE id IN ({jobsId})"

def BQ_SelectCredential():
    return "SELECT id FROM credential WHERE create_at >= DATE_SUB(NOW(), interval 30 SECOND)"
#endregion

#region Stage
def BQ_SelectCredential():
    return 
#endregion