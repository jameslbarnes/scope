import json
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

from fastapi.testclient import TestClient


class _CueProxyHandler(BaseHTTPRequestHandler):
    observations = []

    def do_GET(self):
        if self.path == "/sessions/demo/state":
            self._write_json({"sessionId": "demo", "state": {"transcript": "hello"}})
            return
        if self.path == "/sessions/demo/agent":
            self._write_json({"sessionId": "demo", "endpoints": {}})
            return
        self.send_response(404)
        self.end_headers()

    def do_POST(self):
        if self.path != "/sessions/demo/observations":
            self.send_response(404)
            self.end_headers()
            return
        length = int(self.headers.get("Content-Length", "0"))
        body = self.rfile.read(length).decode("utf-8")
        decoded = json.loads(body) if body else {}
        type(self).observations.append(decoded)
        self._write_json({"sessionId": "demo", "results": []})

    def _write_json(self, payload):
        body = json.dumps(payload).encode("utf-8")
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, format, *args):
        return


def _serve_cue():
    _CueProxyHandler.observations = []
    server = ThreadingHTTPServer(("127.0.0.1", 0), _CueProxyHandler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    return server


def test_cue_state_proxy_returns_session_state():
    from scope.server.app import app

    server = _serve_cue()
    try:
        client = TestClient(app, raise_server_exceptions=False)
        response = client.get(
            "/api/v1/cue/sessions/demo/state",
            params={"base_url": f"http://127.0.0.1:{server.server_port}"},
        )

        assert response.status_code == 200
        assert response.json()["state"]["transcript"] == "hello"
    finally:
        server.shutdown()
        server.server_close()


def test_cue_observation_proxy_posts_observation():
    from scope.server.app import app

    server = _serve_cue()
    try:
        client = TestClient(app, raise_server_exceptions=False)
        response = client.post(
            "/api/v1/cue/sessions/demo/observations",
            json={
                "base_url": f"http://127.0.0.1:{server.server_port}",
                "observation": {
                    "type": "transcript.segment",
                    "payload": {"text": "hello"},
                },
            },
        )

        assert response.status_code == 200
        assert _CueProxyHandler.observations == [
            {"type": "transcript.segment", "payload": {"text": "hello"}}
        ]
    finally:
        server.shutdown()
        server.server_close()
