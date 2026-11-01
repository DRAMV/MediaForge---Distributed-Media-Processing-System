from concurrent import futures
import grpc
from protos import farm_pb2_grpc

class Manager(farm_pb2_grpc.ManagerServicer):
    pass

def serve():
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    farm_pb2_grpc.add_ManagerServicer_to_server(Manager(), server)
    server.add_insecure_port("[::]:50051")
    server.start()
    print("[manager] listening on port 50051")
    try:
        server.wait_for_termination()
    except KeyboardInterrupt:
        print("[manager] received interrupt signal")
        server.stop(grace=2)

if __name__ == "__main__":
    serve()