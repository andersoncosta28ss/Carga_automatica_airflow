from enum import Enum

SQL_JOB_Insert_DefaultInternalFields = "job_id, status, retries, parent_id, params, errors, credential_id, charge_id, numberOfDays"
SQL_JOB_Select_DefaultExternalFields = "job_id, status, retries, parent_id, params, errors, credential_id, updated"
SQL_JOB_Select_DefaultInternalFields = "job_id, status, retries, parent_id, params, errors, was_sent, isInvalidCredential, credential_id, numberOfDays"

class JobStatus(Enum):
    Queued = 'queued'
    Running = 'running'
    Done = 'done'
    Timeout = 'timeout'
    Stale = 'stale'
    Failed = 'failed'

class ChargeStatus(Enum):
    Done = 'done'
    Partially_Done = 'partially_done'
    Running = 'running'
