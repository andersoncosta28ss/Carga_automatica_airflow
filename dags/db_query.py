from db_connections import getConexaoLocal
#region Local
def Query_Local_SelectPendingJobs():
    return f"SELECT id, status, was_sent, retry, id_parent FROM job WHERE was_sent = false AND status <> 'Done'"

def Query_Local_SelectJobsFromIdCharge(idCharge):
    return f"SELECT id, status, was_sent, retry, id_parent FROM job WHERE id_charge = '{idCharge}' AND status <> 'Done'"


def Query_Local_InsertChildrenJob(jobs):
    query = ""
    if(len(jobs) == 0):
        query = "SELECT 0"
    for job in jobs:
        id = job[0]
        status = job[1]
        retry = job[2]
        id_parent = job[3]
        #region get id_charge
        db = getConexaoLocal()
        cursor = db.cursor()
        cursor.execute(f"SELECT id, status, was_sent, retry, id_parent, id_charge FROM job WHERE id = '{id_parent}'")
        result = cursor.fetchone()
        db.close()
        #endregion
        id_charge = result[5]
        query += f"""
                    INSERT INTO job(id, status, retry, id_parent, id_charge) VALUES('{id}','{status}', {retry}, '{id_parent}', '{id_charge}');
                    UPDATE job SET was_sent = true WHERE id = '{id_parent}';
        """
    return query

def Query_Local_UpdateCharge(idCharge, state):
    return f"UPDATE charge SET status = '{state}' WHERE id = '{idCharge}'"

def Query_Local_UpdateJob(jobs):
    query = ""
    for job in jobs:
        # was_sent = job[1] == "Failed" and job[2] == 0
        query += f"UPDATE job SET status = '{job[1]}', retry = {job[2]}, id_parent = '{job[3]}' WHERE id = '{job[0]}';"
    return query

def Query_Local_InsertCharge(charges):
    query = ""
    for charge in charges:
        idCharge = charge['idCarga']
        idCredential = charge['idCredencial']
        idJobs = charge['idJobs']
        query += f"INSERT INTO credential(id) VALUES({idCredential});"
        query += f"INSERT INTO charge (id, credential_id) values('{idCharge}', '{idCredential}');"
        for idJob in idJobs:
            query += f"INSERT INTO job (id, id_charge) values('{idJob}', '{idCharge}');"
    return query

def Query_Local_SelectCrendetial(idCredential):
    return f"SELECT * FROM charge WHERE credential_id = {idCredential}"

#endregion

#region BQ
def Query_BQ_Select_JobsById(idJobs):
    return f"SELECT id, status, retry, id_parent FROM job WHERE id IN ({idJobs})"

def Query_BQ_Select_JobsChildrenByIdParent(idJob):
    return f"SELECT id, status, retry, id_parent FROM job WHERE id_parent IN ({idJob})"

def Query_Prod_SelectCredential():
    return "SELECT id FROM credential WHERE create_at >= DATE_SUB(NOW(), interval 30 SECOND)"
#endregion
