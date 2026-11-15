import uuid
import threading
from concurrent import futures
import grpc
from protos import farm_pb2, farm_pb2_grpc

from common import config
from scheduler.state_store import JobStore
from scheduler.scheduler_core import SchedulerCore

MAX_MSG = 64 * 1024 * 1024

class Manager(farm_pb2_grpc.ManagerServicer):
    def __init__(self):
        # Replace the raw dict with our thread-safe JobStore
        self.job_store = JobStore()
        
        # Get workers list from config or defaults, and plug into SchedulerCore
        workers_list = getattr(config, "WORKERS", ["localhost:50052"])
        self.scheduler = SchedulerCore(workers_list)

    def UploadVideo(self, request, context):
        job_id = "J-" + uuid.uuid4().hex[:8]
        print(f"[manager] H2 got video job={job_id} "
              f"file={request.filename} bytes={len(request.data)}")

        # 1. Create job in JobStore as QUEUED with a threading.Event
        self.job_store.create(job_id, request.filename, "transcode")

        # 2. Pick the next worker using SchedulerCore (Round-Robin)
        worker_address = self.scheduler.next_worker()
        self.job_store.assign(job_id, worker_address)

        print(f"[manager] H3 scheduling job={job_id} -> worker at {worker_address}")
        
        try:
            channel = grpc.insecure_channel(
                worker_address, 
                options=[
                    ("grpc.max_send_message_length", MAX_MSG),
                    ("grpc.max_receive_message_length", MAX_MSG),
                ]
            )
            worker = farm_pb2_grpc.WorkerStub(channel)
            ack = worker.SubmitJob(
                farm_pb2.Job(job_id=job_id, filename=request.filename, data=request.data),
                timeout=5,
            )
            accepted = ack.accepted
        except Exception as e:
            print(f"[manager] Error contacting worker {worker_address}: {e}")
            self.job_store.fail(job_id)
            accepted = False

        print(f"[manager] H5 worker got job={job_id} (accepted={accepted})")

        # 3. Wait for the worker to finish using JobStore's thread-safe wait_done
        finished = self.job_store.wait_done(job_id, timeout=30)
        if not finished:
            self.job_store.remove(job_id)
            return farm_pb2.UploadReply(
                job_id=job_id, data=b"", detail="timeout waiting for worker to finish job"
            )

        # Retrieve result from JobStore
        result_data = self.job_store.result(job_id)
        self.job_store.remove(job_id)

        # Fallback empty result if none populated
        class DummyResult:
            data = b""
            detail = "ok"
        
        res = result_data if result_data else DummyResult()

        print(f"[manager] H8 sending result to client job={job_id} "
              f"bytes={len(res.data)}")
        context.add_callback(
            lambda: print(f"[manager] H9 delivered job={job_id} happy path complete")
        )
        return farm_pb2.UploadReply(
            job_id=job_id, data=res.data, detail=getattr(res, "detail", "ok")
        )

    def ReportResult(self, request, context):
        print(f"[manager] H7 worker done job={request.job_id} "
              f"bytes={len(request.data)}")
        
        # Complete the job in our thread-safe JobStore
        success = self.job_store.complete(request.job_id, request)
        if not success:
            return farm_pb2.ReportAck(ok=False)
            
        return farm_pb2.ReportAck(ok=True)

def serve():
    server = grpc.server(
        futures.ThreadPoolExecutor(max_workers=10),
        options=[
            ("grpc.max_send_message_length", MAX_MSG),
            ("grpc.max_receive_message_length", MAX_MSG),
        ],
    )
    farm_pb2_grpc.add_ManagerServicer_to_server(Manager(), server)
    server.add_insecure_port("[::]:50051")
    server.start()
    print("[manager] listening on port 50051")
    try:
        server.wait_for_termination()
    except KeyboardInterrupt:
        print("[manager] received interrupt signal")
        server.stop(grace=2)

if __name__ == "__main__":
    serve()