"""Mock RAG server for e2e testing of the cozy-stack RabbitMQ indexing pipeline.

Implements the OpenRAG-compatible endpoints used by the rag-indexer consumer:
  GET  /partition/{partition}/file/{file_id}     - lookup
  POST /indexer/partition/{partition}/file/{file_id} - create
  PUT  /indexer/partition/{partition}/file/{file_id} - update
  DELETE /indexer/partition/{partition}/file/{file_id} - delete

Control endpoints (for test orchestration):
  POST   /mock/fail/{file_id}        - make next N requests for file_id return 500
  GET    /mock/files                  - list all indexed files
  GET    /mock/files/{file_id}        - get indexed file details
  DELETE /mock/reset                  - clear all state
  GET    /mock/requests               - list all received requests (for debugging)
"""

import json
import threading
from http.server import HTTPServer, BaseHTTPRequestHandler
from urllib.parse import urlparse, parse_qs
import re
import sys


class MockRAGState:
    def __init__(self):
        self.lock = threading.Lock()
        self.files = {}           # file_id -> {metadata, partition, ...}
        self.fail_map = {}        # file_id -> remaining fail count
        self.requests = []        # [{method, path, timestamp}, ...]

    def reset(self):
        with self.lock:
            self.files.clear()
            self.fail_map.clear()
            self.requests.clear()

    def set_fail(self, file_id, count):
        with self.lock:
            self.fail_map[file_id] = count

    def should_fail(self, file_id):
        with self.lock:
            count = self.fail_map.get(file_id, 0)
            if count > 0:
                self.fail_map[file_id] = count - 1
                return True
            return False

    def upsert_file(self, partition, file_id, metadata):
        with self.lock:
            self.files[file_id] = {
                "partition": partition,
                "file_id": file_id,
                "metadata": metadata,
            }

    def get_file(self, file_id):
        with self.lock:
            return self.files.get(file_id)

    def delete_file(self, file_id):
        with self.lock:
            return self.files.pop(file_id, None)

    def list_files(self):
        with self.lock:
            return dict(self.files)

    def log_request(self, method, path):
        with self.lock:
            self.requests.append({"method": method, "path": path})

    def get_requests(self):
        with self.lock:
            return list(self.requests)


state = MockRAGState()

# Route patterns
PARTITION_FILE_RE = re.compile(r"^/partition/([^/]+)/file/([^/]+)$")
INDEXER_FILE_RE = re.compile(r"^/indexer/partition/([^/]+)/file/([^/]+)$")


class MockRAGHandler(BaseHTTPRequestHandler):
    def log_message(self, format, *args):
        sys.stderr.write(f"[mock-rag] {format % args}\n")

    def _send_json(self, code, data):
        body = json.dumps(data).encode()
        self.send_response(code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _send_empty(self, code):
        self.send_response(code)
        self.end_headers()

    def _read_body(self):
        length = int(self.headers.get("Content-Length", 0))
        return self.rfile.read(length) if length > 0 else b""

    def _parse_multipart_metadata(self, body):
        """Extract metadata JSON from multipart form data."""
        content_type = self.headers.get("Content-Type", "")
        if "multipart/form-data" not in content_type:
            return {}
        try:
            text = body.decode("latin-1")
            idx = text.find('name="metadata"')
            if idx == -1:
                return {}
            start = text.find("\r\n\r\n", idx)
            if start == -1:
                return {}
            start += 4
            end = text.find("\r\n--", start)
            if end == -1:
                end = len(text)
            return json.loads(text[start:end].strip())
        except (json.JSONDecodeError, ValueError):
            return {}

    def _handle_upsert(self, body, status_code):
        """Shared handler for POST (create) and PUT (update) on /indexer/... endpoints."""
        path = urlparse(self.path).path
        m = INDEXER_FILE_RE.match(path)
        if not m:
            return False
        partition, file_id = m.group(1), m.group(2)
        if state.should_fail(file_id):
            self._send_json(500, {"error": "simulated failure"})
            return True
        metadata = self._parse_multipart_metadata(body)
        qs = parse_qs(urlparse(self.path).query)
        if "name" in qs:
            metadata["name"] = qs["name"][0]
        if "md5sum" in qs:
            metadata["md5sum"] = qs["md5sum"][0]
        state.upsert_file(partition, file_id, metadata)
        self._send_json(status_code, {"task_status_url": f"/indexer/task/mock-{file_id}"})
        return True

    # --- RAG API endpoints ---

    def do_GET(self):
        state.log_request("GET", self.path)
        path = urlparse(self.path).path

        # GET /partition/{partition}/file/{file_id} - lookup
        m = PARTITION_FILE_RE.match(path)
        if m:
            partition, file_id = m.group(1), m.group(2)
            if state.should_fail(file_id):
                self._send_json(500, {"error": "simulated failure"})
                return
            f = state.get_file(file_id)
            if f:
                self._send_json(200, {
                    "metadata": f["metadata"],
                    "documents": [],
                })
            else:
                self._send_json(404, {"detail": "Not Found"})
            return

        # GET /mock/files - list all indexed files
        if path == "/mock/files":
            self._send_json(200, state.list_files())
            return

        # GET /mock/files/{file_id} - get specific file
        if path.startswith("/mock/files/"):
            file_id = path[len("/mock/files/"):]
            f = state.get_file(file_id)
            if f:
                self._send_json(200, f)
            else:
                self._send_json(404, {"error": "not found"})
            return

        # GET /mock/requests - debug log
        if path == "/mock/requests":
            self._send_json(200, state.get_requests())
            return

        # GET /health
        if path == "/health":
            self._send_json(200, {"status": "healthy"})
            return

        self._send_json(404, {"error": "unknown endpoint"})

    def do_POST(self):
        state.log_request("POST", self.path)
        path = urlparse(self.path).path
        body = self._read_body()

        if self._handle_upsert(body, 201):
            return

        # POST /mock/fail/{file_id}?count=N - set failure count
        if path.startswith("/mock/fail/"):
            file_id = path[len("/mock/fail/"):]
            qs = parse_qs(urlparse(self.path).query)
            count = int(qs.get("count", ["1"])[0])
            state.set_fail(file_id, count)
            self._send_json(200, {"file_id": file_id, "fail_count": count})
            return

        self._send_json(404, {"error": "unknown endpoint"})

    def do_PUT(self):
        state.log_request("PUT", self.path)
        body = self._read_body()

        if self._handle_upsert(body, 202):
            return

        self._send_json(404, {"error": "unknown endpoint"})

    def do_DELETE(self):
        state.log_request("DELETE", self.path)
        path = urlparse(self.path).path

        # DELETE /indexer/partition/{partition}/file/{file_id}
        m = INDEXER_FILE_RE.match(path)
        if m:
            partition, file_id = m.group(1), m.group(2)
            if state.should_fail(file_id):
                self._send_json(500, {"error": "simulated failure"})
                return
            state.delete_file(file_id)
            self._send_empty(204)
            return

        # DELETE /mock/reset - clear all state
        if path == "/mock/reset":
            state.reset()
            self._send_json(200, {"status": "reset"})
            return

        self._send_json(404, {"error": "unknown endpoint"})


def main():
    port = int(sys.argv[1]) if len(sys.argv) > 1 else 8000
    server = HTTPServer(("0.0.0.0", port), MockRAGHandler)
    print(f"Mock RAG server listening on :{port}", flush=True)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    server.server_close()


if __name__ == "__main__":
    main()
