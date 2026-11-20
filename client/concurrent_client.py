import argparse
import logging
import pathlib
import sys
import time

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

def upload_one(index, stub, filename, data, task, timeout, out_dir):  #returns a dict for the summary table
    result = {
        "index": index,
        "job_id": "-",
        "ok": False,
        "seconds": 0.0,
        "bytes": 0,
        "task": task or "-",
        "detail": ""
    }
    start = time.perf_counter()
    log.info("upload #%d started", index)
    try:
        reply = stub.UploadVideo(
            farm_pb2.UploadRequest(filename=filename, data=data, task_type=task or ""),
            timeout=timeout
        )
        
        result["job_id"] = reply.job_id or ""
        result["task"] = reply.task_type or result["task"]
        result["detail"] = reply.detail

        if reply.data:
            out_path = out_dir / f"{reply.job_id}_{filename}"
            out_path.write_bytes(reply.data)
            result["ok"] = True

    except grpc.RpcError as e:
        result["detail"] = f"{e.code().name}: {e.details()}"
    except Exception as e:
        result["detail"] = f"{type(e).__name__}: {e}"

    result["seconds"] = time.perf_counter() - start
    log.info("upload #%d %s in %.1fs", index, "done" if result["ok"] else "FAILED",
             result["seconds"])
    return result

def print_table(results, wall):
    print()
    print(f"{'#':>2}  {'job_id':<12} {'status':<7} {'task':<11} {'secs':>6} {'bytes':>9}  detail")
    print("-" * 92)
    for r in results:
        detail = r["detail"] if len(r["detail"]) <= 36 else r["detail"][:33] + "..."
        print(f"{r['index']:>2}  {r['job_id'][:12]:<12} {'OK' if r['ok'] else 'FAILED':<7} "
              f"{r['task']:<11} {r['seconds']:>6.1f} {r['bytes']:>9}  {detail}")
    ok = sum(r["ok"] for r in results)
    print("-" * 92)
    print(f"{ok}/{len(results)} succeeded, total wall time {wall:.1f}s")

def main():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [client] %(message)s",
                        datefmt="%H:%M:%S")

    args = parse_args()
    path = pathlib.Path(args.file)
    if not path.is_file():
        sys.exit(f"[client] file not found: {path}")

    data = path.read_bytes()
    out_dir = pathlib.Path(args.out)
    out_dir.mkdir(parents=True, exist_ok=True)

    channel = grpc.insecure_channel(args.manager, options=config.CHANNEL_OPTIONS)
    stub = farm_pb2_grpc.ManagerStub(channel)  # channels and stubs are thread-safe

    log.info("firing %d parallel uploads of %s (%d bytes) at %s",
             args.count, path.name, len(data), args.manager)
    wall_start = time.perf_counter()
    with concurrent.futures.ThreadPoolExecutor(max_workers=args.count) as pool:
        futures = [pool.submit(upload_one, i + 1, stub, path.name, data,
                               args.task, args.timeout, out_dir)
                   for i in range(args.count)]
        results = [f.result() for f in futures]   # submit order, so the table is ordered
    print_table(results, time.perf_counter() - wall_start)
    sys.exit(0 if all(r["ok"] for r in results) else 1)


if __name__ == "__main__":
    main()