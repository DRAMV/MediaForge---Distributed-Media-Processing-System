import argparse
import logging

import grpc
from common import config
from protos import farm_pb2, farm_pb2_grpc

# The manager's gRPC pool has 10 threads and each UploadVideo holds one while it
# waits for a worker. ReportResult needs a free thread too, so stay below 10.
MAX_SAFE_UPLOADS = 8

log = logging.getLogger("client")


def parse_args():
    p = argparse.ArgumentParser(description="Upload the same video several times in parallel")
    p.add_argument("file")
    p.add_argument("-n", "--count", type=int, default=4, help="number of parallel uploads")
    p.add_argument("--manager", default=config.MANAGER_ADDRESS)
    p.add_argument("--out", default="results")
    p.add_argument("--timeout", type=float, default=config.CLIENT_RPC_TIMEOUT)
    p.add_argument("--task",
                   choices=("transcode", "thumbnail", "subtitles", "moderation"),
                   help="task type for every upload, omit this to let the manager choose")
    args = p.parse_args()
    if not 1 <= args.count <= MAX_SAFE_UPLOADS:
        p.error(f"--count must be between 1 and {MAX_SAFE_UPLOADS} "
                f"(the manager's thread pool would starve above that)")
    return args
