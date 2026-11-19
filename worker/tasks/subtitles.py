import time 

from common import config 

def run(job):
    time.sleep(config.PROCESS_SECONDS)

    return (
        job.data,
        "subtitles: simulated subtitles processing; "
        "no speech recognition performed; original file returned"    
    )