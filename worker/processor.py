import time

def process_job(job):

    print(f"[Worker] Processing job: {job.job_id}")

    time.sleep(2)

    processed_data = job.data

    detail = f"Successfully processed {job.filename}"

    print(f"[Worker] Job {job.job_id} completed")

    return processed_data, detail