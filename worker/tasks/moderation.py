import time

from common import config

def run(job):
    time.sleep(config.PROCESS_SECONDS)

    return (
        job.data,
        "Simulation review complete; "
        "content was not assessed; original file retuned",
    )