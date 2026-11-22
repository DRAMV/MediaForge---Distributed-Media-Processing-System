import argparse, pathlib, sys, time
import grpc
from common import config
from protos import farm_pb2, farm_pb2_grpc

MAX_MSG = 64 * 1024 * 1024

def parse_args():
    p = argparse.ArgumentParser(description="Upload a video to the media farm")
    p.add_argument("file")
    p.add_argument("--manager", default="localhost:50051")
    p.add_argument("--out", default="results")
    p.add_argument("--timeout", type=float, default=60) #must exceed manager's 30s wait
    p.add_argument(
        "--task",
        choices=("transcode", "thumbnail", "subtitles", "moderation"),
        help="Task to perform; omit to let the manager choose",
    )
    return p.parse_args()

def main():
    from common.logging_setup import setup_logging, event
    log = setup_logging("client")

    args = parse_args()
    path = pathlib.Path(args.file)

    if not path.is_file():
        sys.exit(f"[client] file not found: {path}")

    data = path.read_bytes()
    channel = grpc.insecure_channel(args.manager, options=[
        ("grpc.max_send_message_length", MAX_MSG),
        ("grpc.max_receive_message_length", MAX_MSG)
    ])
    stub = farm_pb2_grpc.ManagerStub(channel)
    
    event(log, "H1", f"uploading {path.name} ({len(data)} bytes) to {args.manager}")
    start = time.time()

    try:
        reply = stub.UploadVideo(
            farm_pb2.UploadRequest(
                filename=path.name,
                data=data,
                task_type=args.task or "",
            ),            timeout=args.timeout
        )
    except grpc.RpcError as e:
        sys.exit(f"[client] RPC failed: {e.code().name} - {e.details()}")

    if not reply.data:
        sys.exit(f"[client] no result for job={reply.job_id}: {reply.detail}")

    out = pathlib.Path(args.out); out.mkdir(parents=True, exist_ok=True)
    out_path = out / f"{reply.job_id}_{path.name}"

    if reply.detail.startswith("thumbnail: created"):
        out_path = out_path.with_suffix(".jpg")
    
    elif reply.detail.startswith("transcode: converted"):
        out_path = out_path.with_suffix(".mp4")

    elif reply.task_type == "subtitles":
        out_path = out_path.with_suffix(".srt")
        
    elif reply.task_type == "moderation":
        out_path = out_path.with_suffix(".json")
    
    out_path.write_bytes(reply.data)

    stamp = f" lamport={reply.lamport}" if config.ENABLE_CLOCKS else ""
    event(log, "H9", f"job={reply.job_id} done in {time.time()-start:.1f}s: {reply.detail}{stamp}")
    print(f"[client] saved {len(reply.data)} bytes -> {out_path}")

if __name__ == "__main__":
    main()