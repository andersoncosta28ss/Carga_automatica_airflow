#region Local
def Query_Local_SelectJobsFromIdCharge(idCharge):
    return f"SELECT id, status, was_sent, retry, id_parent FROM job WHERE id_charge = '{idCharge}' AND status <> 'Done'"

def Query_Local_InsertJobs(idJob, idCharge):
    return f"INSERT INTO job (id, id_charge) values('{idJob}', '{idCharge}')"


def Query_Local_InsertJobResents(job):
    id = job["id"]
    was_sent = job["was_sent"]
    retry = job["retry"]
    id_charge = job["id_charge"]
    status = job["status"]
    id_parent = job["id_parent"]
    return f"INSERT INTO job(id, status, was_sent, retry, id_parent, id_charge) VALUES('{id}','{status}', {was_sent}, {retry}, '{id_parent}', '{id_charge}')"


def Query_Local_UpdateCharge(idCharge, state):
    return f"UPDATE charge SET status = '{state}' WHERE id = '{idCharge}'"


def Query_Local_UpdateJob(job):
    return f"UPDATE job SET status = '{job[1]}', was_sent = {job[2]}, retry = {job[3]}, id_parent = '{job[4]}' WHERE id = '{job[0]}'"


def Query_Local_InsertCharge(idCharge, idCredential):
    return f"INSERT INTO charge (id, credential_id) values('{idCharge}', '{idCredential}')"


def Query_Local_InsertCredential(idCredential):
    return f"INSERT INTO credential(id) VALUES({idCredential})"


def Query_Local_SelectCrendetial(idCredential):
    return f"SELECT * FROM charge WHERE credential_id = {idCredential}"

#endregion

#region BQ
def Query_BQ_SelectJobs(jobsId):
    return f"SELECT id, status, was_sent, retry, id_parent FROM job WHERE id IN ({jobsId})"

def Query_Prod_SelectCredential():
    return "SELECT id FROM credential WHERE create_at >= DATE_SUB(NOW(), interval 30 SECOND)"
#endregion
