# syntax=docker/dockerfile:1
# ---------------------------------------------------------------------------
# MediaForge — ONE shared image for every service (manager, worker, client,
# dashboard). docker-compose.yml (M2) picks what each container RUNS via its
# `command:` override; the image itself is identical everywhere.
#
# Build:   docker build -t mediaforge .
# ---------------------------------------------------------------------------

FROM python:3.12-slim

# PYTHONUNBUFFERED=1 is critical: without it Python buffers stdout inside
# containers and `docker compose logs` shows our H1..H9 story late (or never).
ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_NO_CACHE_DIR=1

WORKDIR /app

# 1) dependencies first — this layer is cached and only re-runs when
#    requirements.txt changes (grpcio 1.85 has cp312 wheels: no compiler needed)
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# 2) application code — everything .dockerignore didn't exclude
COPY . .

# 3) build the gRPC stubs fresh from the committed farm.proto, so the image
#    always matches the contract (never trusts hand-committed stub files)
RUN python protos/gen_proto.py

# client saves results here; LOG_FILE=1 mirrors logs here
RUN mkdir -p results logs

# manager listens here (informational; compose -P / docker run -P uses it)
EXPOSE 50051

# default = manager. docker-compose overrides this per service, e.g.
#   worker:  command: python -m worker.worker
#   client:  command: python -m client.client --file assets/sample.mp4 --manager manager:50051
CMD ["python", "-m", "scheduler.node"]
