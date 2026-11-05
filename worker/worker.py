import grpc
import threading

from concurrent import futures

from protos import farm_pb2, farm_pb2_grpc
from worker.processor import process_job
from worker.manager_client import report_result


MANAGER_ADDRESS = "localhost:50051"
WORKER_PORT = 50052
MAX_MSG = 64 * 1024 * 1024

class WorkerService(farm_pb2_grpc.WorkerServicer):
    def SubmitJob(self, request, context):
        print(f"[Worker] Received Job {request.job_id}")

        thread = threading.Thread(
            target=self.process_in_background,
            args=(request,),
            daemon=True
        )

        thread.start()

        return farm_pb2.JobAck(
            job_id=request.job_id,
            accepted=True
        )

    def process_in_background(self, job):

        try:
            data, detail = process_job(job)

            report_result(
                MANAGER_ADDRESS,
                job.job_id,
                data,
                detail
            )

        except Exception as error:
            print(f"[Worker] Error processing job {job.job_id}: {error}")


def serve():

    server = grpc.server(
        futures.ThreadPoolExecutor(max_workers=10),
        options=[
            ("grpc.max_send_message_length", MAX_MSG),
            ("grpc.max_receive_message_length", MAX_MSG),
        ],
    )

    farm_pb2_grpc.add_WorkerServicer_to_server(
        WorkerService(),
        server
    )

    server.add_insecure_port(f"[::]:{WORKER_PORT}")

    server.start()

    print(f"[Worker] Running on port {WORKER_PORT}")

    server.wait_for_termination()

if __name__ == "__main__":
    serve()