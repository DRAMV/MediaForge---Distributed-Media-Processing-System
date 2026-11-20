import uuid
from concurrent import futures

import grpc

from common import config
from protos import farm_pb2, farm_pb2_grpc
from scheduler.scheduler_core import SchedulerCore
from scheduler.state_store import JobStore


class Manager(farm_pb2_grpc.ManagerServicer):
    def __init__(self):
        self.store = JobStore()
        self.scheduler = SchedulerCore(config.WORKER_ADDRESSES)

    def _pick_task(self, request):
        if request.task_type:
            return request.task_type
        name = request.filename.lower()
        for task in config.TASK_TYPES:
            if task in name:
                return task
        return config.DEFAULT_TASK

    def _fail_reply(self, job_id, task_type, detail):
        self.store.fail(job_id)
        self.store.remove(job_id)
        print(f"[manager] job={job_id} FAILED: {detail}")
        return farm_pb2.UploadReply(
            job_id=job_id, data=b"", detail=detail, task_type=task_type
        )

    def UploadVideo(self, request, context):
        task_type = self._pick_task(request)
        if task_type not in config.TASK_TYPES:
            context.abort(
                grpc.StatusCode.INVALID_ARGUMENT,
                f"Unknown task type: {task_type}",
            )

        job_id = "J-" + uuid.uuid4().hex[:8]
        print(f"[manager] H2 got video job={job_id} "
              f"file={request.filename} bytes={len(request.data)}")
        self.store.create(job_id, request.filename, task_type)

        ack = None
        for worker_address in self.scheduler.order():
            self.store.assign(job_id, worker_address)
            print(f"[manager] H3 scheduling job={job_id} -> worker at {worker_address}")
            try:
                with grpc.insecure_channel(
                    worker_address, options=config.CHANNEL_OPTIONS
                ) as channel:
                    worker = farm_pb2_grpc.WorkerStub(channel)
                    ack = worker.SubmitJob(
                        farm_pb2.Job(
                            job_id=job_id,
                            filename=request.filename,
                            data=request.data,
                            task_type=task_type,
                        ),
                        timeout=config.WORKER_ACK_TIMEOUT,
                    )
            except grpc.RpcError as error:
                print(f"[manager] worker {worker_address} unavailable "
                      f"({error.code().name}) - trying next")
                ack = None
                continue
            if ack.accepted:
                break
            print(f"[manager] worker {worker_address} refused job={job_id} - trying next")
            ack = None

        if ack is None:
            return self._fail_reply(job_id, task_type, "no worker available")
        
        print(f"[manager] H5 worker={ack.worker_id} got job={job_id} "
              f"task={task_type} (accepted={ack.accepted})")
                
        if not self.store.wait_done(job_id, config.JOB_RESULT_TIMEOUT):
            return self._fail_reply(
                job_id, task_type, "timeout waiting for worker to finish job"
            )

        result = self.store.result(job_id)
        self.store.remove(job_id)
        print(f"[manager] H8 sending result to client job={job_id} "
              f"bytes={len(result.data)}")
        context.add_callback(
            lambda: print(f"[manager] H9 delivered job={job_id} - happy path complete")
        )
        return farm_pb2.UploadReply(
            job_id=job_id,
            data=result.data,
            detail=result.detail,
            task_type=task_type,
        )
    
    def ReportResult(self, request, context):
        print(f"[manager] H7 worker={request.worker_id} done job={request.job_id} "
              f"bytes={len(request.data)}")
        accepted = self.store.complete(request.job_id, request)
        return farm_pb2.ReportAck(ok=accepted)


def serve():
    server = grpc.server(
        futures.ThreadPoolExecutor(max_workers=10),
        options=config.CHANNEL_OPTIONS,
    )
    farm_pb2_grpc.add_ManagerServicer_to_server(Manager(), server)
    server.add_insecure_port(f"[::]:{config.MANAGER_PORT}")
    server.start()
    print(f"[manager] listening on port {config.MANAGER_PORT}")
    try:
        server.wait_for_termination()
    except KeyboardInterrupt:
        print("[manager] received interrupt signal")
        server.stop(grace=config.GRACE_SECONDS)


if __name__ == "__main__":
    serve()