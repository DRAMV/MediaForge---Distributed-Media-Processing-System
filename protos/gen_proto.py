#!/usr/bin/env python3
"""Regenerate Python gRPC stubs from protos/farm.proto.

Run from the repo root:   python protos/gen_proto.py
Re-run EVERY time farm.proto changes, then commit the new stub files.
"""
import pathlib
import subprocess
import sys

ROOT = pathlib.Path(__file__).resolve().parents[1]
OUT = ROOT / "protos"

def main():
    (OUT / "__init__.py").touch(exist_ok=True)
    subprocess.run([
        sys.executable, "-m", "grpc_tools.protoc",
        f"--proto_path={OUT}",
        f"--python_out={OUT}",
        f"--grpc_python_out={OUT}",
        str(OUT / "farm.proto"),
    ],check = True)

    # grpc_tools writes "import farm_pb2 as farm__pb2" (top-level import) —
    # rewrite it so the stubs import cleanly as a package.

    stub = OUT / "farm_pb2_grpc.py"
    stub.write_text(stub.read_text().replace(
        "import farm_pb2 as farm__pb2",
        "from protos import farm_pb2 as farm__pb2"
    ))
    print ("generated -> protos/farm_pb2.py, protos/farm_pb2_grpc.py")

if __name__ == "__main__":
    main()