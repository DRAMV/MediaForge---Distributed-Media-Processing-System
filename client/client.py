import argparse, pathlib, sys, time
import grpc
from protos import farm_pb2, farm_pb2_grpc

MAX_MSG = 64 * 1024 * 1024

def parse_args():
    p = argparse.ArgumentParser(description="Upload a video to the media farm")
    p.add_argument("file")
    p.add_argument("--manager", default="localhost:50051")
    p.add_argument("--out", default="results")
    p.add_argument("--timeout", type=float, default=60) #must exceed manager's 30s wait
    return p.parse_args()

def main():
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
    
    print(f"[client] H1 uploading {path.name} ({len(data)} bytes) to {args.manager}")
    start = time.time()

    try:
        reply = stub.UploadVideo(
            farm_pb2.UploadRequest(filename=path.name, data=data),
            timeout=args.timeout
        )
    except grpc.RpcError as e:
        sys.exit(f"[client] RPC failed: {e.code().name} - {e.details()}")

    if not reply.data:
        sys.exit(f"[client] no result for job={reply.job_id}: {reply.detail}")

    out = pathlib.Path(args.out); out.mkdir(parents=True, exist_ok=True)
    out_path = out / f"{reply.job_id}_{path.name}"
    out_path.write_bytes(reply.data)

    print(f"[client] H9 job={reply.job_id} done in {time.time()-start:.1f}s: {reply.detail}")
    print(f"[client] saved {len(reply.data)} bytes -> {out_path}")

if __name__ == "__main__":
    main()