import json
import sys
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

PLUGIN_SRC = Path(__file__).resolve().parents[1] / "plugins" / "scope-cue" / "src"
if str(PLUGIN_SRC) not in sys.path:
    sys.path.insert(0, str(PLUGIN_SRC))

from scope_cue_plugin.nodes.cue_session import CueSessionNode  # noqa: E402


class _CueStateHandler(BaseHTTPRequestHandler):
    state = {
        "state": {
            "transcript": "The room turns blue.",
            "decisionCount": 3,
            "observationCount": 8,
        },
        "decisionHistory": [
            {
                "kind": "tool_call",
                "result": {
                    "actions": [
                        {
                            "type": "video.update_prompt",
                            "payload": {
                                "prompt": "Blue projected room, camera holds close.",
                                "reset": True,
                            },
                        }
                    ]
                },
            }
        ],
    }

    def do_GET(self):
        if self.path != "/sessions/demo/state":
            self.send_response(404)
            self.end_headers()
            return
        body = json.dumps(type(self).state).encode("utf-8")
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, format, *args):
        return


def _serve_cue_state():
    server = ThreadingHTTPServer(("127.0.0.1", 0), _CueStateHandler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    return server


def test_cue_session_node_definition_exposes_expected_ports():
    definition = CueSessionNode.get_definition()

    assert definition.node_type_id == "cue.session"
    assert definition.continuous is True
    assert {port.name for port in definition.outputs} >= {
        "prompt",
        "reset",
        "action_json",
        "transcript",
        "status",
        "tick",
    }


def test_cue_session_node_polls_state_and_emits_prompt_action():
    server = _serve_cue_state()
    try:
        node = CueSessionNode("cue")
        outputs = node.execute(
            {},
            cue_base_url=f"http://127.0.0.1:{server.server_port}",
            session_id="demo",
            poll_interval_ms=0,
        )

        assert outputs["prompt"] == "Blue projected room, camera holds close."
        assert outputs["reset"] is True
        assert outputs["transcript"] == "The room turns blue."
        assert outputs["decision_count"] == 3
        assert outputs["observation_count"] == 8
        assert outputs["status"] == "ok"
        assert outputs["tick"] == 1
        assert json.loads(outputs["action_json"])["type"] == "video.update_prompt"
    finally:
        server.shutdown()
        server.server_close()


def test_cue_session_node_suppresses_unchanged_outputs():
    server = _serve_cue_state()
    try:
        node = CueSessionNode("cue")
        kwargs = {
            "cue_base_url": f"http://127.0.0.1:{server.server_port}",
            "session_id": "demo",
            "poll_interval_ms": 0,
        }

        first = node.execute({}, **kwargs)
        second = node.execute({}, **kwargs)

        assert first["tick"] == 1
        assert second == {}
    finally:
        server.shutdown()
        server.server_close()


def test_cue_session_node_reports_connection_errors_once():
    node = CueSessionNode("cue")

    outputs = node.execute(
        {},
        cue_base_url="http://127.0.0.1:1",
        session_id="demo",
        poll_interval_ms=0,
        timeout_ms=50,
    )
    repeat = node.execute(
        {},
        cue_base_url="http://127.0.0.1:1",
        session_id="demo",
        poll_interval_ms=0,
        timeout_ms=50,
    )

    assert outputs["status"].startswith("error:")
    assert outputs["tick"] == 1
    assert repeat == {}
