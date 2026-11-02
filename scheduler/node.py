import uuid
from concurrent import futures
import grpc
from protos import farm_pb2, farm_pb2_grpc

WORKER_ADDRESS = "localhost:50052"

class Manager(farm_pb2_grpc.ManagerServicer):
    def UploadVideo(self, request, context):
        job_id = "J-" + uuid.uuid4().hex[:8]
        print(f"[manager] H2 got video job={job_id} "
              f"file={request.filename} bytes={len(request.data)}")
        
        print(f"[manager] H3 scheduling job={job_id} -> worker at {WORKER_ADDRESS}")
        channel = grpc.insecure_channel(WORKER_ADDRESS)
        worker = farm_pb2_grpc.WorkerStub(channel)
        ack = worker.SubmitJob(
            farm_pb2.Job(job_id=job_id, filename=request.filename, data=request.data),
            timeout=5,
        )
        print(f"[manager] H5 worker got job={job_id} (accepted={ack.accepted})")
        
        return farm_pb2.UploadReply(
            job_id=job_id, data=b"", detail="received (worker not connected yet)"
        )

def serve():
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
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