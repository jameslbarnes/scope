import base64
import json
import sys
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from io import BytesIO
from pathlib import Path

import pytest
import torch
from PIL import Image

PLUGIN_SRC = (
    Path(__file__).resolve().parents[1] / "plugins" / "scope-shaderclaw" / "src"
)
if str(PLUGIN_SRC) not in sys.path:
    sys.path.insert(0, str(PLUGIN_SRC))

from scope_shaderclaw_plugin.client import ShaderClawClient  # noqa: E402
from scope_shaderclaw_plugin.native_renderer import ShaderLibrary  # noqa: E402
from scope_shaderclaw_plugin.pipelines.pipeline import (  # noqa: E402
    ShaderClawPipeline,
    data_url_to_tensor,
    parse_parameters_json,
)


def _png_data_url(color=(255, 0, 0), size=(1, 1)):
    image = Image.new("RGB", size, color)
    out = BytesIO()
    image.save(out, format="PNG")
    encoded = base64.b64encode(out.getvalue()).decode("ascii")
    return f"data:image/png;base64,{encoded}"


class _ShaderClawHandler(BaseHTTPRequestHandler):
    actions = []

    def do_GET(self):
        if self.path != "/shaders/manifest.json":
            self.send_response(404)
            self.end_headers()
            return
        body = json.dumps(
            [
                {
                    "id": 1,
                    "title": "Gradient",
                    "file": "no_mans_sky_gradients.fs",
                }
            ]
        ).encode("utf-8")
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_POST(self):
        if self.path != "/api/rc":
            self.send_response(404)
            self.end_headers()
            return
        length = int(self.headers.get("Content-Length", "0"))
        payload = json.loads(self.rfile.read(length).decode("utf-8"))
        type(self).actions.append(payload)
        action = payload["action"]
        result = {"ok": True}
        if action == "screenshot":
            result = {"dataUrl": _png_data_url()}
        body = json.dumps({"ok": True, "result": result}).encode("utf-8")
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, format, *args):
        return


def _serve_shaderclaw():
    _ShaderClawHandler.actions = []
    server = ThreadingHTTPServer(("127.0.0.1", 0), _ShaderClawHandler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    return server


def test_parse_parameters_json_requires_object():
    assert parse_parameters_json('{"speed": 0.5}') == {"speed": 0.5}
    assert parse_parameters_json("") == {}

    try:
        parse_parameters_json("[1, 2]")
    except Exception as e:
        assert "JSON object" in str(e)
    else:
        raise AssertionError("Expected non-object parameter JSON to fail")


def test_data_url_to_tensor_resizes_to_scope_output_size():
    tensor = data_url_to_tensor(
        _png_data_url(size=(1, 1)),
        width=2,
        height=3,
        device=torch.device("cpu"),
    )

    assert tuple(tensor.shape) == (1, 3, 2, 3)
    assert float(tensor[0, 0, 0, 0]) == 1.0


def test_shaderclaw_client_resolves_manifest_and_sends_rc_actions():
    server = _serve_shaderclaw()
    try:
        client = ShaderClawClient(
            f"http://127.0.0.1:{server.server_port}",
            timeout_s=1,
        )

        entry = client.load_shader("Gradient", timeout_ms=1000)
        client.set_parameters({"speed": 0.25}, timeout_ms=1000)

        assert entry.file == "no_mans_sky_gradients.fs"
        assert _ShaderClawHandler.actions[0]["action"] == "load_shader_file"
        assert _ShaderClawHandler.actions[0]["params"]["file"] == entry.file
        assert _ShaderClawHandler.actions[1]["action"] == "set_parameter"
        assert _ShaderClawHandler.actions[1]["params"] == {
            "name": "speed",
            "value": 0.25,
        }
    finally:
        server.shutdown()
        server.server_close()


def _write_test_shader_dir(tmp_path: Path):
    shaders = tmp_path / "shaders"
    shaders.mkdir()
    (shaders / "manifest.json").write_text(
        json.dumps(
            [
                {
                    "id": 1,
                    "title": "Test Native",
                    "file": "test_native.fs",
                }
            ]
        ),
        encoding="utf-8",
    )
    (shaders / "test_native.fs").write_text(
        """
/*{
  "INPUTS": [
    { "NAME": "speed", "TYPE": "float", "DEFAULT": 0.25, "MIN": 0.0, "MAX": 1.0 },
    { "NAME": "enabled", "TYPE": "bool", "DEFAULT": true }
  ]
}*/
void main() {
    gl_FragColor = vec4(speed, enabled ? 0.5 : 0.0, 0.0, 1.0);
}
""",
        encoding="utf-8",
    )
    return shaders


def test_shader_library_reads_local_manifest_and_inputs(tmp_path: Path):
    shaders = _write_test_shader_dir(tmp_path)
    library = ShaderLibrary(shaders)

    manifest = library.manifest()
    inputs = library.inputs("Test Native", {"speed": 0.75})

    assert manifest[0]["title"] == "Test Native"
    assert inputs[0]["name"] == "speed"
    assert inputs[0]["value"] == 0.75


def test_shaderclaw_pipeline_outputs_video_from_native_renderer(tmp_path: Path):
    shaders = _write_test_shader_dir(tmp_path)
    try:
        pipeline = ShaderClawPipeline(
            height=4,
            width=4,
            shaders_dir=str(shaders),
        )
        output = pipeline(
            shader="Test Native",
            parameters_json='{"speed": 0.5, "enabled": true}',
        )
    except Exception as exc:
        pytest.skip(f"Native OpenGL context unavailable: {exc}")
    if pipeline._last_error:
        pytest.skip(f"Native OpenGL context unavailable: {pipeline._last_error}")

    assert tuple(output["video"].shape) == (1, 4, 4, 3)
    assert output["video"].dtype.is_floating_point
    assert float(output["video"][0, 0, 0, 0]) > 0.45
    assert float(output["video"][0, 0, 0, 1]) > 0.45
