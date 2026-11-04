import argparse

MAX_MSG = 64 * 1024 * 1024

def parse_args():
    p = argparse.ArgumentParser(description="Upload a video to the media farm")
    p.add_argument("file")
    p.add_argument("--manager", default="localhost:50051")
    p.add_argument("--out", default="results")
    p.add_argument("--timeout", type=float, default=60) #must exceed manager's 30s wait
    return p.parse_args()