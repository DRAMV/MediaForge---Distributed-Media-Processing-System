import threading
import uuid
import json
import time
from concurrent import futures

import grpc

from common import config
from protos import farm_pb2, farm_pb2_grpc
from scheduler.election import ElectionNode, LEADER
from scheduler.replicated_log import ReplicatedLog
from scheduler.scheduler_core import SchedulerCore
from scheduler.state_store import JobStore

REPLICATION_TIMEOUT = 5.0   # seconds to wait for a majority to copy a job

class Manager(farm_pb2_grpc.ManagerServicer):
    def __init__(self, election=None, log=None):
        self.election = election
        self.log = log
        self.applied = 0                      # log entries already applied
        self._apply_lock = threading.Lock()
        self.store = JobStore()
        self.scheduler = SchedulerCore(config.WORKER_ADDRESSES)

    def apply_committed(self):
        """Copy newly committed log entries into this manager's own JobStore."""
        if self.log is None:
            return
        with self._apply_lock:
            while self.applied < self.log.commit_index:
                self.applied += 1
                command = self.log.entries[self.applied - 1]["command"]
                if (command.get("op") == "ENQUEUE_JOB"
                        and self.store.state(command["job_id"]) is None):
                    self.store.create(
                        command["job_id"], command["filename"], command["task_type"]
                    )

    def _replicate(self, job_id, filename, task_type):
        """Write the job to the log and wait until a majority has a copy."""
        index = self.log.append(self.election.term, {
            "op": "ENQUEUE_JOB",
            "job_id": job_id,
            "filename": filename,
            "task_type": task_type,
        })
        end = time.time() + REPLICATION_TIMEOUT
        while time.time() < end:
            if self.log.commit_index >= index:
                self.apply_committed()
                print(f"[manager] job={job_id} committed in log (index {index})")
                return True
            time.sleep(0.05)
        return False

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
        if self.election is not None and self.election.state != LEADER:
            leader = self.election.leader_id or "unknown"
            print(f"[manager] rejected upload: not the leader (leader={leader})")
            context.abort(
                grpc.StatusCode.FAILED_PRECONDITION,
                f"NOT_LEADER leader={leader}",
            )
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
        if self.log is not None and not self._replicate(
                job_id, request.filename, task_type):
            return self._fail_reply(
                job_id, task_type,
                "could not copy the job to a majority of managers",
            )
            
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

class RaftService(farm_pb2_grpc.RaftServicer):
    """The door other managers knock on: RequestVote and AppendEntries."""

    def __init__(self, election, log=None, on_commit=None):
        self.election = election
        self.log = log
        self.on_commit = on_commit

    def RequestVote(self, request, context):
        term, granted = self.election.handle_vote_request(
            request.term, request.candidate_id
        )
        return farm_pb2.VoteReply(term=term, vote_granted=granted)

    def AppendEntries(self, request, context):
        term, ok = self.election.handle_heartbeat(request.term, request.leader_id)
        if not ok or self.log is None:
            return farm_pb2.AppendEntriesReply(term=term, success=ok)

        entries = [{"term": e.term, "command": json.loads(e.command)}
                   for e in request.entries]
        matched = self.log.accept(
            request.prev_log_index, request.prev_log_term, entries
        )
        if matched:
            last_new = request.prev_log_index + len(entries)
            self.log.commit_to(min(request.leader_commit, last_new))
            if self.on_commit:
                self.on_commit()
        return farm_pb2.AppendEntriesReply(term=term, success=matched)


class GrpcTransport:
    """Lets ElectionNode phone the other managers. None = no answer."""

    def __init__(self, rpc_timeout=0.5, log=None):
        self.rpc_timeout = rpc_timeout
        self.log = log
        self._stubs = {}
        self._lock = threading.Lock()
        self._next_index = {}          # peer -> next log entry to send it
        self._leader_term = None

    def _stub(self, address):
        with self._lock:
            if address not in self._stubs:
                channel = grpc.insecure_channel(address)
                self._stubs[address] = farm_pb2_grpc.RaftStub(channel)
            return self._stubs[address]

    def request_vote(self, peer, term, candidate_id):
        try:
            reply = self._stub(peer).RequestVote(
                farm_pb2.VoteRequest(term=term, candidate_id=candidate_id),
                timeout=self.rpc_timeout,
            )
            return reply.term, reply.vote_granted
        except grpc.RpcError:
            return None

    def _build_append(self, peer, term, leader_id):
        if self.log is None:
            return farm_pb2.AppendEntriesRequest(term=term, leader_id=leader_id)
        if term != self._leader_term:            # new term: start over
            self._leader_term = term
            self._next_index = {}
        next_index = self._next_index.get(peer, self.log.last_index() + 1)
        prev = next_index - 1
        entries = [
            farm_pb2.LogEntry(term=e["term"], index=next_index + n,
                              command=json.dumps(e["command"]).encode())
            for n, e in enumerate(self.log.entries_from(next_index))
        ]
        return farm_pb2.AppendEntriesRequest(
            term=term, leader_id=leader_id,
            prev_log_index=prev, prev_log_term=self.log.term_at(prev),
            entries=entries, leader_commit=self.log.commit_index,
        )

    def send_heartbeat(self, peer, term, leader_id):
        request = self._build_append(peer, term, leader_id)
        try:
            reply = self._stub(peer).AppendEntries(
                request, timeout=self.rpc_timeout
            )
        except grpc.RpcError:
            return None
        if self.log is not None and reply.term <= term:
            if reply.success:
                match = request.prev_log_index + len(request.entries)
                self._next_index[peer] = match + 1
                self.log.record_ack(match, peer)
            else:                                  # follower is missing older entries
                self._next_index[peer] = max(1, request.prev_log_index)
        return reply.term, reply.success


def other_managers():
    """All manager addresses except mine (NODE_ID is my name or my address)."""
    me = config.NODE_ID
    return [a for a in config.PEER_ADDRESSES
            if a != me and a.split(":")[0] != me]


def build_election_node():
    return ElectionNode(
        config.NODE_ID,
        other_managers(),
        transport or GrpcTransport(),
        timeout_range=(config.ELECTION_TIMEOUT_MIN, config.ELECTION_TIMEOUT_MAX),
        heartbeat_every=config.RAFT_HEARTBEAT_INTERVAL,
    )
    
def serve():
    server = grpc.server(
        futures.ThreadPoolExecutor(max_workers=10),
        options=config.CHANNEL_OPTIONS,
    )
    election = None
    log = None
    if config.ENABLE_ELECTION:
        log = ReplicatedLog(config.NODE_ID, len(other_managers()) + 1)
        election = build_election_node(GrpcTransport(log=log))
    manager = Manager(election, log)
    if election:
        farm_pb2_grpc.add_RaftServicer_to_server(
            RaftService(election, log, manager.apply_committed), server
        )
    farm_pb2_grpc.add_ManagerServicer_to_server(manager, server)
    server.add_insecure_port(f"[::]:{config.MANAGER_PORT}")
    server.start()
    print(f"[manager] listening on port {config.MANAGER_PORT}")
    if election:
        election.start()
    try:
        server.wait_for_termination()
    except KeyboardInterrupt:
        print("[manager] received interrupt signal")
        if election:
            election.stop()
        server.stop(grace=config.GRACE_SECONDS)


if __name__ == "__main__":
    serve()