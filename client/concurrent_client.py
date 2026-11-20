import argparse
import grpc
from common import config
from protos import farm_pb2, farm_pb2_grpc

# The manager's gRPC pool has 10 threads and each UploadVideo holds one while it
# waits for a worker. ReportResult needs a free thread too, so stay below 10.
MAX_SAFE_UPLOADS = 8