import time

from common import config 
from worker.tasks import (
    moderation,
    subtitles,
    thumbnail,
    transcode
)

TASKS = {
    "transcode": transcode.run,
    "thumbnail": thumbnail.run,
    "subtitles": subtitles.run,
    "moderation": moderation.run, 
}

def process_job(job):
    task_type = job.task_type or config.DEFAULT_TASK

    if task_type not in TASKS:
        raise ValueError(f"Unknown task type: {task_type}")

    return TASKS[task_type](job)