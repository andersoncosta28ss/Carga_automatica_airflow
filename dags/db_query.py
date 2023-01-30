from db_connections import getConnectionLocal
# region Local
def Query_Local_SelectPendingJobs():
    return f"SELECT id, status, was_sent, retry, id_parent FROM job WHERE was_sent = false AND status <> 'done'"

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
        db = getConnectionLocal()
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

def Query_Local_SelectJobsFromIdCharge(idCharge):
    return f"SELECT id, status, was_sent, retry, id_parent FROM job WHERE id_charge = '{idCharge}' AND status <> 'done'"

def Query_Local_UpdateJob(jobs):
    query = ""
    for job in jobs:
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

# endregion
