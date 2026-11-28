import argparse 
import json
import threading
import time 

from collections import OrderedDict
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.request import urlopen
from common import config


class WorkerStatus:
    def __init__(self, worker_id, port, concurrency):
        self.worker = {
            "worker_id": worker_id,
            "address": f"localhost:{port}",
            "concurrency": concurrency,
        }

        self.jobs = OrderedDict()
        self.lock = threading.Lock()

    def update(self, job, state, detail=""):
        with self.lock:
            self.jobs[job.job_id] = {
                "job_id": job.job_id,
                "worker_id": self.worker["worker_id"],
                "filename": job.filename,
                "task": job.task_type or config.DEFAULT_TASK,
                "state": state,
                "detail": detail,
                 "updated": time.time(),
            }

            finished = [
                key
                for key, value in self.jobs.items()
                if value["state"] in ("DONE", "FAILED")
            ]

            for key in finished[:-100]:
                del self.jobs[key]

    def snapshot(self):
        with self.lock:
            return {
                "workers": [self.worker.copy()],
                "jobs": list(self.jobs.values()),
            }

class ClusterStatus:
    def __init__(self, urls):
        self.urls = urls

    def snapshot(self):
        workers = []
        jobs = []
        for url in self.urls:
            try:
                endpoint = url.rstrip("/") + "/api/status"
                with urlopen(endpoint, timeout=1) as response:
                    data = json.load(response)
                    for worker in data["workers"]:
                        workers.append({
                            **worker,
                            "state": "REACHABLE",
                        })
                    
                    jobs.extend(data["jobs"])
            except Exception:
                workers.append({
                    "worker_id": url,
                    "address": url,
                    "state": "UNREACHABLE",
                    "concurrency": 0,
                })
        return {
            "workers": workers,
            "jobs": sorted(jobs, key=lambda job: job["updated"]),
        }       


PAGE = """
<!doctype html>
<html>
<head>
    <meta charset="utf-8">
    <title>MediaForge Status</title>
    <style>
        body {
            font: 16px system-ui;
            margin: 40px;
            background: #f5f7fb;
            color: #182435;
        }

        table {
            border-collapse: collapse;
            width: 100%;
            background: white;
        }

        th, td {
            padding: 12px;
            text-align: left;
            border-bottom: 1px solid #ddd;
        }

        #message { color: #a21; }
    </style>
</head>
<body>
    <h1>MediaForge Status</h1>
    <p id="workers"></p>
    <p id="message"></p>

    <table>
        <thead>
            <tr>
                <th>Job</th>
                <th>Worker</th>
                <th>File</th>
                <th>Task</th>
                <th>State</th>
                <th>Detail</th>
            </tr>
        </thead>
        <tbody id="jobs"></tbody>
    </table>

    <script>
        async function refresh() {
            try {
                const response = await fetch(
                    "/api/status",
                    {cache: "no-store"}
                );

                if (!response.ok) {
                    throw new Error("Status unavailable");
                }

                const data = await response.json();

                document.getElementById("workers").textContent =
                    data.workers.map(worker =>
                        `${worker.worker_id} | ` +
                        `${worker.address} | ` +
                        `${worker.state || "CONNECTED"} | ` +
                        `${worker.concurrency} processing slots`
                    ).join(" / ");

                document.getElementById("message").textContent = "";

                const body = document.getElementById("jobs");
                body.replaceChildren();

                for (const job of data.jobs.slice().reverse()) {
                    const row = document.createElement("tr");

                    for (const key of [
                        "job_id", "worker_id", "filename",
                        "task", "state", "detail"
                    ]) {
                        const cell = document.createElement("td");
                        cell.textContent = job[key];
                        row.appendChild(cell);
                    }

                    body.appendChild(row);
                }
            } catch (error) {
                document.getElementById("message").textContent =
                    "Status unavailable: connection lost";
            } finally {
                setTimeout(refresh, 1000);
            }
        }

        refresh();
    </script>
</body>
</html>
"""
def start_status_server(status, port):
    class Handler(BaseHTTPRequestHandler):
        def do_GET(self):
            if self.path == "/api/status":
                content = json.dumps(status.snapshot()).encode()
                content_type = "application/json"
            elif self.path == "/":
                content = PAGE.encode()
                content_type = "text/html; charset=utf-8"
            else:
                self.send_error(404)
                return

            self.send_response(200)
            self.send_header("Content-Type", content_type)
            self.send_header("Cache-Control", "no-store")        
            self.send_header("Content-Length", str(len(content)))
            self.end_headers()
            self.wfile.write(content)

        def log_message(self, *args):
            pass    

    server = ThreadingHTTPServer(("0.0.0.0", port), Handler)

    threading.Thread(
        target=server.serve_forever,
        daemon=True,
    ).start()

    return server

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--port", type=int, default=8081)

    parser.add_argument(
        "--workers",
        nargs="+",
        default=["http://localhost:8080"],
    )

    args = parser.parse_args()
    server = start_status_server(
        ClusterStatus(args.workers),
        args.port,
    )

    print(f"Dashboard: http://localhost:{args.port}")

    try:
        threading.Event().wait()
    except KeyboardInterrupt:
        server.shutdown()
        server.server_close()

if __name__ == "__main__":
    main()


        
    