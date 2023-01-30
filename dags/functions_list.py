def Filter_Queue(job):
    return job[1] == 'Queue'


def Filter_Running(job):
    return job[1] == 'Running'


def Filter_Failed_Local(job):
    return job[1] == 'Failed' and job[3] > 0 and job[2] == False


def Filter_OverTryFailure(job):
    return job[1] == 'Failed' and job[3] <= 0


def Filter_Failed_BQ(job):
    return job[1] == 'Failed' and job[2] > 0


def Map_IdJobs(job):
    return str(f"'{job[0]}'")
