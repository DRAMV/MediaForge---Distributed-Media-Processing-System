import grpc
import threading

from concurrent import futures

from protos import farm_pb2, farm_pb2_grpc
from worker.processor import process_job, TASKS
from worker.manager_client import report_result

from common import config
from common.clock import LamportClock
from common.logging_setup import setup_logging, event
from dashboard.status_page import WorkerStatus, start_status_server


MANAGER_ADDRESS = "localhost:50051"
WORKER_PORT = 50052
MAX_MSG = 64 * 1024 * 1024

class WorkerService(farm_pb2_grpc.WorkerServicer):
    def __init__(self, pool, status):
        self.pool = pool
        self.status = status
        self.clock = LamportClock(owner=config.WORKER_ID)
    
    def SubmitJob(self, request, context):
        task_type = request.task_type or config.DEFAULT_TASK
        if task_type not in TASKS:
            context.abort(
                grpc.StatusCode.INVALID_ARGUMENT,
                f"Unknown task type: {task_type}",
            )

        if config.ENABLE_CLOCKS:
            self.clock.recv(request.lamport)

        self.status.update(request, "QUEUED")

        print(
            f"[worker:{config.WORKER_ID}]"
            f"accepted job={request.job_id} task={task_type}"
        )
        self.pool.submit(self.process_in_background, request)

        return farm_pb2.JobAck(
            job_id=request.job_id,
            accepted=True,
            worker_id=config.WORKER_ID,
            lamport=self.clock.send() if config.ENABLE_CLOCKS else 0,
        )


    def process_in_background(self, job):
        self.status.update(job, "RUNNING")
        print(
            f"[worker:{config.WORKER_ID}]"
            f"started job={job.job_id}"
        )

        processing_failed = False

        try:
            data, detail = process_job(job)
            if len(data) >= config.MAX_MSG - 4096:
                raise ValueError("Result exceeds gRPC message limit")

        except Exception as error:
            processing_failed = True
            data = b""
            detail = f"FAILED: {type(error).__name__}: {error}"

        report_lamport = self.clock.send() if config.ENABLE_CLOCKS else 0
        if config.ENABLE_CLOCKS:
            print(f"[worker:{config.WORKER_ID}] clock job={job.job_id} "
                  f"report lamport={report_lamport}")

        try:
            accepted = report_result(
                config.MANAGER_ADDRESS,
                job.job_id,
                data, detail,
                report_lamport,
            )

            if not accepted:
                raise RuntimeError(
                    "Manager rejected result; job may have timed out"
                )

            state = "FAILED" if processing_failed else "DONE"

        except Exception as error:
            state = "FAILED"
            detail = (
                f"Result delivery failed: {error};"
                f"processing details: {detail}"  
            )

        self.status.update(job, state, detail)

        print(
            f"[worker:{config.WORKER_ID}]"
            f"{state} job={job.job_id}: {detail}"
        )


def serve():
    log = setup_logging("worker")

    if config.WORKER_CONCURRENCY < 1:
        raise ValueError("FARM_WORKER_CONCURRENCY must be at least 1")

    status = WorkerStatus(
        config.WORKER_ID,
        config.WORKER_PORT,
        config.WORKER_CONCURRENCY,
    )
    with futures.ThreadPoolExecutor(
        max_workers=config.WORKER_CONCURRENCY
    ) as pool:
        server = grpc.server(
            futures.ThreadPoolExecutor(max_workers=10),
            options=config.CHANNEL_OPTIONS,
        )

        farm_pb2_grpc.add_WorkerServicer_to_server(
            WorkerService(pool, status),
            server,
        )

        bound_port = server.add_insecure_port(
            f"[::]:{config.WORKER_PORT}"
        )

        if not bound_port:
            raise RuntimeError(
                f"Cannot bind worker port {config.WORKER_PORT}"
            )

        page = start_status_server(status, config.STATUS_PORT)

        try:
            server.start()

            print(
                f"[worker:{config.WORKER_ID}]"
                f"lsitening on port {config.WORKER_PORT}"
                f"concurrency={config.WORKER_CONCURRENCY}"
            )
            print(
                f"[worker:{config.WORKER_ID}]"
                f"status: http://localhost:{config.STATUS_PORT}"
            )

            server.wait_for_termination()

        except KeyboardInterrupt:
            print(f"[worker:{config.WORKER_ID}] shutting down")

        finally:
            server.stop(config.GRACE_SECONDS).wait()
            page.shutdown()
            page.server_close()

if __name__ == "__main__":
    serve()