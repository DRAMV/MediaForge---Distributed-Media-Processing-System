import grpc
import time

from contextlib import ExitStack
from protos import farm_pb2, farm_pb2_grpc
from common import config

def _manager_addresses(primary):
    peers = config.PEER_ADDRESSES if config.ENABLE_ELECTION else []
    return tuple(dict.fromkeys([primary, *peers]))

def report_result(manager_address, job_id, data, detail, lamport=0) :
    request = farm_pb2.ResultReport(
        job_id=job_id,
        data=data,
        detail=detail,
        worker_id=config.WORKER_ID,
        lamport=lamport,    
    )

    addresses = _manager_addresses(manager_address)

    deadline = time.monotonic() + config.JOB_RESULT_TIMEOUT

    while True:
        for address in addresses:
            timeout = config.WORKER_ACK_TIMEOUT
            if config.ENABLE_ELECTION:
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    return False

                timeout = min(timeout, remaining)

            try:
                with grpc.insecure_channel(
                    address, options=config.CHANNEL_OPTIONS
                ) as channel:
                    stub = farm_pb2_grpc.ManagerStub(channel)
                    response = stub.ReportResult(request , timeout=timeout)
            except grpc.RpcError as error:
                if not config.ENABLE_ELECTION or error.code() not in (
                    grpc.StatusCode.UNAVAILABLE,
                    grpc.StatusCode.DEADLINE_EXCEEDED,
                ):
                    raise
                continue

            if response.ok:
                print(f"[worker:{config.WORKER_ID}] manager acknowledged job={job_id}")
                return True

        if not config.ENABLE_ELECTION:
            return False

        remaining = deadline - time.monotonic()
        if remaining <= 0:
            return False

        time.sleep(min(0.25, remaining))

def heartbeat_loop(stop , clock , log):
    timeout = min(config.WORKER_ACK_TIMEOUT, config.HEARTBEAT_INTERVAL)

    with ExitStack() as channels:
        stubs = {}
        for address in _manager_addresses(config.MANAGER_ADDRESS):
            channel = channels.enter_context(
                grpc.insecure_channel(address , options=config.CHANNEL_OPTIONS)
            )
            stubs[address] = farm_pb2_grpc.ManagerStub(channel)

        while not stop.is_set():
            next_ping = time.monotonic() + config.HEARTBEAT_INTERVAL
            pending = {}

            for address, stub in stubs.items():
                request = farm_pb2.HeartbeatRequest(
                    worker_id=config.WORKER_ID,
                    lamport=clock.send() if config.ENABLE_CLOCKS else 0,
                )

                pending[address] = stub.Heartbeat.future(request , timeout=timeout)

            for address, call in pending.items():
                try:
                    ack = call.result()
                except grpc.RpcError as error:
                    log.debug("Heartbeat to %s failed: %s", address, error.code())
                    continue

                if config.ENABLE_CLOCKS:
                    clock.recv(ack.lamport)

            stop.wait(max(0, next_ping - time.monotonic()))

        
 