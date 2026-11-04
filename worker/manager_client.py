import grpc

import farm_pb2
import farm_pb2_grpc


def report_result(manager_address, job_id, data, details) :

    manager_stub = farm_pb2_grpc.ManagerStub(channel)

    response = manager_stub.ReportResult(
        farm_pb2.ResultReport(
            job_id = job_id,
            data = data,
            detail = detail
        )
    )


    print(f"[Worker] Manager acknowledfed result: {response.ok}")

    return response.ok