import grpc

from protos import farm_pb2, farm_pb2_grpc
from common import config

def report_result(manager_address, job_id, data, detail, lamport=0) :

    with grpc.insecure_channel(
        manager_address,
        options=config.CHANNEL_OPTIONS,
    ) as channel:
        stub = farm_pb2_grpc.ManagerStub(channel)

        response = stub.ReportResult(
            farm_pb2.ResultReport(
                job_id = job_id,
                data=data,
                detail=detail,
                worker_id=config.WORKER_ID,
                lamport=lamport,
            ),
            timeout=config.WORKER_ACK_TIMEOUT,
        )

    print(
        f"[worker:{config.WORKER_ID}]"
        f"manager acknowledged job={job_id}: {response.ok}"
    )

    return response.ok