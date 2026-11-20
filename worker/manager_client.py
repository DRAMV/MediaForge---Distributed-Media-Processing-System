import grpc

from protos import farm_pb2, farm_pb2_grpc


def report_result(manager_address, job_id, data, details) :

    channel = grpc.insecure_channel(manager_address, options=[
        ("grpc.max_send_message_length", 64 * 1024 * 1024),
        ("grpc.max_receive_message_length", 64 * 1024 * 1024),
    ])
    
    manager_stub = farm_pb2_grpc.ManagerStub(channel)

    response = manager_stub.ReportResult(
        farm_pb2.ResultReport(
            job_id = job_id,
            data = data,
            detail = details
        )
    )


    print(f"[Worker] Manager acknowledged result: {response.ok}")

    return response.ok