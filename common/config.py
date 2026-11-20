"""
Every value that can differ between machines is read from an environment
variable (prefix FARM_) with a sane default, so:

  - local run:   defaults just work (same ports as phase 1)
  - docker run:  docker-compose.yml sets the FARM_* env vars
  - mechanisms:  each one sits behind a flag, default OFF, so the happy
                 path from phase 1 never breaks while they are developed

Import style everywhere in the codebase:
    from common import config
    port = config.MANAGER_PORT
"""

import os
import socket


# ---- tiny env helpers (do not touch) ----------------------------------------
def _str(name: str, default: str) -> str:
    return os.environ.get(name, default)


def _int(name: str, default: int) -> int:
    return int(os.environ.get(name, default))


def _float(name: str, default: float) -> float:
    return float(os.environ.get(name, default))


def _bool(name: str, default: bool) -> bool:
    raw = os.environ.get(name, "1" if default else "0").strip().lower()
    return raw in ("1", "true", "yes", "on")


def _list(name: str, default: str) -> list:
    return [x.strip() for x in os.environ.get(name, default).split(",") if x.strip()]



MANAGER_PORT = _int("FARM_MANAGER_PORT", 50051)
MANAGER_ADDRESS = _str("FARM_MANAGER_ADDRESS", f"localhost:{MANAGER_PORT}")
WORKER_PORT = _int("FARM_WORKER_PORT", 50052)

# Who am I? Docker gives each container a unique hostname automatically.
# Local run with 2 workers on one PC: start them with FARM_WORKER_ID=worker-1 / worker-2.
WORKER_ID = _str("FARM_WORKER_ID", socket.gethostname())

# M2's worker registry (comma-separated). Phase-1 default = the one worker.
WORKER_ADDRESSES = _list("FARM_WORKERS", f"localhost:{WORKER_PORT}")

# ---- gRPC message limits (shared by every channel and server) -----------------
MAX_MSG = _int("FARM_MAX_MSG", 64 * 1024 * 1024)
CHANNEL_OPTIONS = [
    ("grpc.max_send_message_length", MAX_MSG),
    ("grpc.max_receive_message_length", MAX_MSG),
]

# ---- timeouts (seconds) -------------------------------------------------------
CLIENT_RPC_TIMEOUT = _float("FARM_CLIENT_TIMEOUT", 60.0)      # client waits for UploadVideo; must exceed JOB_RESULT_TIMEOUT
WORKER_ACK_TIMEOUT = _float("FARM_WORKER_ACK_TIMEOUT", 5.0)   # manager waits for SubmitJob ack
JOB_RESULT_TIMEOUT = _float("FARM_JOB_RESULT_TIMEOUT", 30.0)  # manager waits for ReportResult
GRACE_SECONDS = _float("FARM_GRACE", 2.0)                     # server.stop(grace=...) on Ctrl+C

# ---- processing ---------------------------------------------------------------
TASK_TYPES = ("transcode", "thumbnail", "subtitles", "moderation")
DEFAULT_TASK = _str("FARM_DEFAULT_TASK", "transcode")
PROCESS_SECONDS = _float("FARM_PROCESS_SECONDS", 2.0)         # dummy processing duration
RESULTS_DIR = _str("FARM_RESULTS_DIR", "results")

# ---- mechanism flags (default OFF = phase-1 behaviour) ------------------------
ENABLE_CLOCKS = _bool("FARM_ENABLE_CLOCKS", False)            # M1: lamport stamps on jobs/logs
ENABLE_HEARTBEATS = _bool("FARM_ENABLE_HEARTBEATS", False)    # M3: worker pings
HEARTBEAT_INTERVAL = _float("FARM_HEARTBEAT_INTERVAL", 5.0)   # M3: ping every N s
DEAD_AFTER_SILENCE = _float("FARM_DEAD_AFTER_SILENCE", 15.0)  # M3: ALIVE -> DEAD after N s quiet
ENABLE_LEASES = _bool("FARM_ENABLE_LEASES", False)            # M4: jobs expire and get requeued
LEASE_SECONDS = _float("FARM_LEASE_SECONDS", 15.0)            # M4: lease duration
ENABLE_DEDUP = _bool("FARM_ENABLE_DEDUP", False)              # M4: same job_id processed once


def summary(component: str) -> str:
    """One dump for startup logs, e.g. print(config.summary('manager'))."""
    return (
        f"[{component}] config:\n"
        f"  manager={MANAGER_ADDRESS}  workers={WORKER_ADDRESSES}\n"
        f"  worker_id={WORKER_ID}  max_msg={MAX_MSG // (1024 * 1024)}MB\n"
        f"  timeouts: client={CLIENT_RPC_TIMEOUT}s ack={WORKER_ACK_TIMEOUT}s "
        f"result={JOB_RESULT_TIMEOUT}s grace={GRACE_SECONDS}s\n"
        f"  tasks={list(TASK_TYPES)}  process={PROCESS_SECONDS}s  results_dir={RESULTS_DIR}\n"
        f"  flags: clocks={ENABLE_CLOCKS} heartbeats={ENABLE_HEARTBEATS} "
        f"leases={ENABLE_LEASES} dedup={ENABLE_DEDUP}"
    )
