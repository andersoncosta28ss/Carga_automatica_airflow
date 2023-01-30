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
