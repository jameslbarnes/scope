"""Native offscreen renderer for ShaderClaw ISF shaders."""

from __future__ import annotations

import base64
import json
import os
import re
import time
from dataclasses import dataclass
from io import BytesIO
from pathlib import Path
from typing import Any

import numpy as np
import torch
from PIL import Image, ImageDraw, ImageFont

DEFAULT_SHADERCLAW_REPO = Path.home() / "shader-claw3"
GL_NEAREST = 9728
GL_LINEAR = 9729


class ShaderClawNativeError(RuntimeError):
    """Raised when the native ShaderClaw renderer cannot complete a request."""


@dataclass(frozen=True)
class ShaderEntry:
    id: int | str | None
    title: str
    file: str
    description: str = ""
    hidden: bool = False
    type: str = "generator"


@dataclass
class ShaderInput:
    name: str
    type: str
    label: str | None = None
    value: Any = None
    default: Any = None
    min: float | list[float] | None = None
    max: float | list[float] | None = None
    values: list[Any] | None = None
    labels: list[str] | None = None
    max_length: int | None = None

    def to_dict(self) -> dict[str, Any]:
        data = {
            "name": self.name,
            "type": self.type,
            "value": self.value,
            "default": self.default,
        }
        if self.label:
            data["label"] = self.label
        if self.min is not None:
            data["min"] = self.min
        if self.max is not None:
            data["max"] = self.max
        if self.values is not None:
            data["values"] = self.values
        if self.labels is not None:
            data["labels"] = self.labels
        if self.max_length is not None:
            data["max_length"] = self.max_length
        return data


def _candidate_shader_dirs(explicit: str | None = None) -> list[Path]:
    candidates: list[Path] = []
    if explicit:
        candidates.append(Path(explicit))
    for env_name in (
        "SCOPE_SHADERCLAW_SHADERS_DIR",
        "SHADERCLAW_SHADERS_DIR",
        "SCOPE_SHADERCLAW_REPO",
        "SHADERCLAW_REPO",
    ):
        if value := os.getenv(env_name):
            candidates.append(Path(value))
    candidates.extend(
        [
            Path.cwd().parent / "shader-claw3",
            DEFAULT_SHADERCLAW_REPO,
        ]
    )
    return candidates


def resolve_shaders_dir(explicit: str | None = None) -> Path:
    """Resolve a ShaderClaw repo or shaders directory."""

    for candidate in _candidate_shader_dirs(explicit):
        shaders_dir = candidate / "shaders" if candidate.name != "shaders" else candidate
        manifest = shaders_dir / "manifest.json"
        if shaders_dir.is_dir() and manifest.exists():
            return shaders_dir.resolve()
    searched = ", ".join(str(path) for path in _candidate_shader_dirs(explicit))
    raise ShaderClawNativeError(
        "Could not find ShaderClaw shaders/manifest.json. "
        f"Set SCOPE_SHADERCLAW_REPO or SCOPE_SHADERCLAW_SHADERS_DIR. Searched: {searched}"
    )


def parse_parameters_json(raw: str | None) -> dict[str, Any]:
    """Parse ShaderClaw parameter JSON from a Scope text field."""

    if raw is None or raw.strip() == "":
        return {}
    try:
        decoded = json.loads(raw)
    except json.JSONDecodeError as e:
        raise ShaderClawNativeError(f"Invalid parameters_json: {e.msg}") from e
    if not isinstance(decoded, dict):
        raise ShaderClawNativeError("parameters_json must be a JSON object")
    return decoded


def _json_number(value: Any, fallback: float = 0.0) -> float:
    if isinstance(value, int | float):
        return float(value)
    if isinstance(value, str):
        try:
            return float(value)
        except ValueError:
            return fallback
    return fallback


def _json_bool(value: Any, fallback: bool = False) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, int | float):
        return value != 0
    if isinstance(value, str):
        return value.lower() in {"1", "true", "yes", "on"}
    return fallback


def _extract_isf_json(source: str) -> dict[str, Any]:
    start = source.find("/*")
    if start == -1:
        return {}
    comment_end = source.find("*/", start)
    if comment_end == -1:
        return {}
    json_start = source.find("{", start, comment_end)
    if json_start == -1:
        return {}

    depth = 0
    json_end: int | None = None
    for index in range(json_start, comment_end):
        char = source[index]
        if char == "{":
            depth += 1
        elif char == "}":
            depth -= 1
            if depth == 0:
                json_end = index
                break
    if json_end is None:
        return {}

    try:
        payload = json.loads(source[json_start : json_end + 1])
    except json.JSONDecodeError as e:
        raise ShaderClawNativeError(f"ISF metadata parse failed: {e.msg}") from e
    return payload if isinstance(payload, dict) else {}


def _strip_isf_header(source: str) -> str:
    start = source.find("/*")
    if start == -1:
        return source
    comment_end = source.find("*/", start)
    if comment_end == -1:
        return source
    if "{" not in source[start:comment_end]:
        return source
    return source[comment_end + 2 :]


def _entry_from_manifest_item(item: dict[str, Any], shaders_dir: Path) -> ShaderEntry | None:
    file_name = str(item.get("file") or "").strip()
    shader_type = str(item.get("type") or "generator")
    if shader_type == "scene" or not file_name.endswith(".fs"):
        return None
    folder = str(item.get("folder") or "").strip()
    file_path = shaders_dir / folder / file_name if folder else shaders_dir / file_name
    if not file_path.exists():
        return None
    return ShaderEntry(
        id=item.get("id"),
        title=str(item.get("title") or file_path.stem),
        file=str(Path(folder) / file_name) if folder else file_name,
        description=str(item.get("description") or ""),
        hidden=bool(item.get("hidden", False)),
        type=shader_type,
    )


class ShaderLibrary:
    """Reads ShaderClaw shader manifest and ISF metadata from disk."""

    def __init__(self, shaders_dir: str | Path | None = None):
        self.shaders_dir = resolve_shaders_dir(str(shaders_dir) if shaders_dir else None)

    def manifest(self) -> list[dict[str, Any]]:
        manifest_path = self.shaders_dir / "manifest.json"
        try:
            raw = json.loads(manifest_path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError) as e:
            raise ShaderClawNativeError(f"Could not read ShaderClaw manifest: {e}") from e

        entries: list[ShaderEntry] = []
        if isinstance(raw, list):
            for item in raw:
                if isinstance(item, dict):
                    entry = _entry_from_manifest_item(item, self.shaders_dir)
                    if entry:
                        entries.append(entry)
        if not entries:
            for path in sorted(self.shaders_dir.glob("*.fs")):
                entries.append(
                    ShaderEntry(
                        id=None,
                        title=path.stem,
                        file=path.name,
                    )
                )
        return [entry.__dict__ for entry in entries]

    def resolve_shader(self, shader: str | int) -> ShaderEntry:
        value = str(shader).strip()
        value_lower = value.lower()
        entries = [ShaderEntry(**item) for item in self.manifest()]

        entry: ShaderEntry | None = None
        if value.isdigit():
            numeric = int(value)
            entry = next((item for item in entries if item.id == numeric), None)
        if entry is None:
            entry = next((item for item in entries if item.file == value), None)
        if entry is None:
            entry = next((item for item in entries if item.title.lower() == value_lower), None)
        if entry is None:
            entry = next((item for item in entries if value_lower in item.title.lower()), None)
        if entry is None and value.endswith(".fs"):
            path = self.shaders_dir / value
            if path.exists():
                entry = ShaderEntry(id=None, title=path.stem, file=value)
        if entry is None:
            available = ", ".join(item.title for item in entries[:8])
            raise ShaderClawNativeError(
                f"Shader not found: {value}. Try one of: {available}"
            )
        return entry

    def shader_path(self, shader: str | int) -> Path:
        entry = self.resolve_shader(shader)
        path = self.shaders_dir / entry.file
        if not path.exists():
            raise ShaderClawNativeError(f"Shader file not found: {path}")
        return path

    def source(self, shader: str | int) -> str:
        path = self.shader_path(shader)
        try:
            return path.read_text(encoding="utf-8")
        except OSError as e:
            raise ShaderClawNativeError(f"Could not read shader file: {path}") from e

    def inputs(self, shader: str | int, parameters: dict[str, Any] | None = None) -> list[dict[str, Any]]:
        source = self.source(shader)
        inputs = parse_isf_inputs(source, parameters or {})
        return [input_.to_dict() for input_ in inputs]


def parse_isf_inputs(source: str, parameters: dict[str, Any] | None = None) -> list[ShaderInput]:
    metadata = _extract_isf_json(source)
    raw_inputs = metadata.get("INPUTS")
    if not isinstance(raw_inputs, list):
        return []

    parsed: list[ShaderInput] = []
    parameters = parameters or {}
    for raw in raw_inputs:
        if not isinstance(raw, dict):
            continue
        name = str(raw.get("NAME") or "").strip()
        if not name:
            continue
        input_type = str(raw.get("TYPE") or "float")
        default = raw.get("DEFAULT")
        value = parameters.get(name, default)
        item = ShaderInput(
            name=name,
            type=input_type,
            label=str(raw.get("LABEL") or "") or None,
            default=default,
            value=value,
        )
        if input_type == "float":
            item.min = _json_number(raw.get("MIN"), 0.0) if "MIN" in raw else None
            item.max = _json_number(raw.get("MAX"), 1.0) if "MAX" in raw else None
            item.default = _json_number(default, 0.5)
            item.value = _json_number(value, item.default)
        elif input_type == "long":
            item.values = raw.get("VALUES") if isinstance(raw.get("VALUES"), list) else None
            item.labels = raw.get("LABELS") if isinstance(raw.get("LABELS"), list) else None
            item.default = int(_json_number(default, 0.0))
            item.value = int(_json_number(value, item.default))
            if item.values:
                item.min = min(int(_json_number(v, 0)) for v in item.values)
                item.max = max(int(_json_number(v, 0)) for v in item.values)
        elif input_type == "bool" or input_type == "event":
            item.default = _json_bool(default, False)
            item.value = _json_bool(value, item.default)
        elif input_type == "color":
            if not isinstance(default, list):
                default = [1.0, 1.0, 1.0, 1.0]
            if not isinstance(value, list):
                value = default
            item.default = [float(_json_number(v, 1.0)) for v in default[:4]]
            while len(item.default) < 4:
                item.default.append(1.0)
            item.value = [float(_json_number(v, 1.0)) for v in value[:4]]
            while len(item.value) < 4:
                item.value.append(1.0)
        elif input_type == "point2D":
            default_vec = default if isinstance(default, list) else [0.5, 0.5]
            value_vec = value if isinstance(value, list) else default_vec
            item.default = [float(_json_number(v, 0.5)) for v in default_vec[:2]]
            item.value = [float(_json_number(v, 0.5)) for v in value_vec[:2]]
            item.min = raw.get("MIN") if isinstance(raw.get("MIN"), list) else [0.0, 0.0]
            item.max = raw.get("MAX") if isinstance(raw.get("MAX"), list) else [1.0, 1.0]
        elif input_type == "text":
            item.max_length = int(_json_number(raw.get("MAX_LENGTH"), 48))
            item.default = str(default or "")
            item.value = str(value if value is not None else item.default)
        parsed.append(item)
    return parsed


def _pass_targets(source: str) -> list[dict[str, Any]]:
    metadata = _extract_isf_json(source)
    raw_passes = metadata.get("PASSES")
    if not isinstance(raw_passes, list):
        return []
    passes: list[dict[str, Any]] = []
    for raw in raw_passes:
        if isinstance(raw, dict):
            target = str(raw.get("TARGET") or "")
            passes.append(
                {
                    "target": target,
                    "persistent": _json_bool(raw.get("PERSISTENT"), False),
                }
            )
    return passes


def _translate_fragment(source: str, inputs: list[ShaderInput], pass_names: list[str]) -> str:
    body = _strip_isf_header(source)
    body = re.sub(r"#version\s+\d+[^\n]*", "// version stripped by Scope", body)
    body = re.sub(r"precision\s+(highp|mediump|lowp)\s+\w+\s*;", "// precision stripped", body)
    body = re.sub(r"\bvarying\b", "in", body)
    body = re.sub(r"\btexture2D\b", "texture", body)

    lines: list[str] = [
        "#version 330 core",
        "out vec4 FragColor;",
        "in vec2 isf_FragNormCoord;",
        "#define vv_FragNormCoord isf_FragNormCoord",
        "uniform float TIME;",
        "uniform float TIMEDELTA;",
        "uniform vec2 RENDERSIZE;",
        "uniform int PASSINDEX;",
        "uniform int FRAMEINDEX;",
        "uniform vec2 mousePos;",
        "uniform vec2 mouseDelta;",
        "uniform float mouseDown;",
        "uniform float pinchHold;",
        "uniform sampler2D audioFFT;",
        "uniform sampler2D fontAtlasTex;",
        "uniform float audioLevel;",
        "uniform float audioBass;",
        "uniform float audioMid;",
        "uniform float audioHigh;",
        "uniform float _voiceLevel;",
        "uniform float _voiceGlitch;",
    ]
    for input_ in inputs:
        if input_.type == "image":
            lines.extend(
                [
                    f"uniform sampler2D {input_.name};",
                    f"uniform vec2 IMG_SIZE_{input_.name};",
                    f"uniform bool _flip_{input_.name};",
                ]
            )
    for pass_name in pass_names:
        lines.append(f"uniform sampler2D {pass_name};")
    lines.extend(
        [
            "#define IMG_NORM_PIXEL(img, coord) texture(img, coord)",
            "#define IMG_THIS_NORM_PIXEL(img) texture(img, isf_FragNormCoord)",
            "#define IMG_THIS_PIXEL(img) texture(img, gl_FragCoord.xy / RENDERSIZE)",
            "#define IMG_SIZE(img) RENDERSIZE",
            "#define IMG_PIXEL(img, coord) texture(img, (coord) / RENDERSIZE)",
        ]
    )
    for input_ in inputs:
        if input_.type == "float":
            lines.append(f"uniform float {input_.name};")
        elif input_.type == "color":
            lines.append(f"uniform vec4 {input_.name};")
        elif input_.type in {"bool", "event"}:
            lines.append(f"uniform bool {input_.name};")
        elif input_.type == "point2D":
            lines.append(f"uniform vec2 {input_.name};")
        elif input_.type == "long":
            lines.append(f"uniform int {input_.name};")
        elif input_.type == "text":
            max_len = input_.max_length or 48
            lines.append(f"uniform int {input_.name}_len;")
            for index in range(max_len):
                lines.append(f"uniform int {input_.name}_{index};")
    lines.extend(["#define gl_FragColor FragColor", body])
    return "\n".join(lines)


VERTEX_SHADER = """
#version 330 core
in vec2 in_pos;
in vec2 in_uv;
out vec2 isf_FragNormCoord;
#define position in_pos
void isf_vertShaderInit() {
    gl_Position = vec4(in_pos, 0.0, 1.0);
    isf_FragNormCoord = in_uv;
}
#define vv_vertShaderInit isf_vertShaderInit
void main() {
    isf_vertShaderInit();
}
"""


def _char_code(char: str) -> int:
    if "A" <= char <= "Z":
        return ord(char) - ord("A")
    if "a" <= char <= "z":
        return ord(char) - ord("a")
    if "0" <= char <= "9":
        return 27 + ord(char) - ord("0")
    return 26


class _RenderTarget:
    def __init__(self, ctx: Any, width: int, height: int, persistent: bool):
        self.ctx = ctx
        self.width = width
        self.height = height
        self.persistent = persistent
        self.read_texture = ctx.texture((width, height), 4, dtype="f1")
        self.write_texture = ctx.texture((width, height), 4, dtype="f1")
        for texture in (self.read_texture, self.write_texture):
            texture.filter = (GL_LINEAR, GL_LINEAR)
            texture.repeat_x = False
            texture.repeat_y = False
        self.write_fbo = ctx.framebuffer(color_attachments=[self.write_texture])

    def current_texture(self) -> Any:
        return self.read_texture if self.persistent else self.write_texture

    def swap_after_write(self) -> None:
        if self.persistent:
            self.read_texture, self.write_texture = self.write_texture, self.read_texture
            self.write_fbo = self.ctx.framebuffer(color_attachments=[self.write_texture])


class NativeShaderClawRenderer:
    """Compiles and renders ShaderClaw ISF fragment shaders in a native GL context."""

    def __init__(
        self,
        *,
        width: int = 1280,
        height: int = 720,
        shaders_dir: str | Path | None = None,
        device: torch.device | None = None,
    ):
        self.width = int(width)
        self.height = int(height)
        self.device = device or torch.device("cpu")
        self.library = ShaderLibrary(shaders_dir)
        self._ctx: Any | None = None
        self._program: Any | None = None
        self._vao: Any | None = None
        self._fbo: Any | None = None
        self._output_texture: Any | None = None
        self._blank_texture: Any | None = None
        self._font_texture: Any | None = None
        self._targets: dict[str, _RenderTarget] = {}
        self._passes: list[dict[str, Any]] = []
        self._inputs: list[ShaderInput] = []
        self._shader_key: str | None = None
        self._shader_selector: str | None = None
        self._configured_parameters: dict[str, Any] = {}
        self._started_at = time.monotonic()
        self._last_time = self._started_at
        self._frame_index = 0

    @property
    def shaders_dir(self) -> Path:
        return self.library.shaders_dir

    def manifest(self) -> list[dict[str, Any]]:
        return self.library.manifest()

    def load_shader(
        self,
        shader: str | int,
        parameters: dict[str, Any] | None = None,
    ) -> list[dict[str, Any]]:
        parameters = parameters or {}
        shader_selector = str(shader)
        if self._shader_key is not None and self._shader_selector == shader_selector:
            self.set_parameters(parameters)
            return [input_.to_dict() for input_ in self._inputs]

        entry = self.library.resolve_shader(shader)
        source = self.library.source(entry.file)
        shader_key = f"{self.library.shaders_dir}:{entry.file}"
        if shader_key != self._shader_key:
            self._compile(shader_key, source)
        self._shader_selector = shader_selector
        self.set_parameters(parameters)
        return [input_.to_dict() for input_ in self._inputs]

    def set_parameters(self, parameters: dict[str, Any]) -> None:
        self._configured_parameters = dict(parameters)
        for input_ in self._inputs:
            if input_.name in self._configured_parameters:
                input_.value = self._configured_parameters[input_.name]

    def render_array(
        self,
        shader: str | int,
        parameters: dict[str, Any] | None = None,
    ) -> np.ndarray:
        self.load_shader(shader, parameters)
        self._render()
        assert self._fbo is not None
        raw = self._fbo.read(components=3, alignment=1)
        array = np.frombuffer(raw, dtype=np.uint8).reshape((self.height, self.width, 3))
        return np.flipud(array).copy()

    def render_rgba_bytes(
        self,
        shader: str | int,
        parameters: dict[str, Any] | None = None,
    ) -> bytes:
        self.load_shader(shader, parameters)
        self._render()
        assert self._fbo is not None
        raw = self._fbo.read(components=4, alignment=1)
        array = np.frombuffer(raw, dtype=np.uint8).reshape((self.height, self.width, 4))
        return np.flipud(array).copy().tobytes()

    def render_tensor(
        self,
        shader: str | int,
        parameters: dict[str, Any] | None = None,
    ) -> torch.Tensor:
        array = self.render_array(shader, parameters).astype(np.float32) / 255.0
        tensor = torch.from_numpy(array).unsqueeze(0).contiguous()
        if self.device.type != "cpu":
            tensor = tensor.to(device=self.device, non_blocking=True)
        return tensor

    def render_data_url(
        self,
        shader: str | int,
        parameters: dict[str, Any] | None = None,
    ) -> str:
        encoded = base64.b64encode(
            self.render_jpeg(shader, parameters, quality=82)
        ).decode("ascii")
        return f"data:image/jpeg;base64,{encoded}"

    def render_jpeg(
        self,
        shader: str | int,
        parameters: dict[str, Any] | None = None,
        *,
        quality: int = 82,
    ) -> bytes:
        image = Image.fromarray(self.render_array(shader, parameters))
        out = BytesIO()
        image.save(out, format="JPEG", quality=quality)
        return out.getvalue()

    def _ensure_context(self) -> None:
        if self._ctx is not None:
            return
        try:
            import moderngl
        except Exception as e:
            raise ShaderClawNativeError(
                "Native ShaderClaw rendering requires moderngl and glcontext"
            ) from e

        try:
            self._ctx = moderngl.create_standalone_context(require=330)
        except Exception as e:
            raise ShaderClawNativeError(f"Could not create native GL context: {e}") from e

        self._output_texture = self._ctx.texture((self.width, self.height), 4, dtype="f1")
        self._output_texture.filter = (GL_LINEAR, GL_LINEAR)
        self._output_texture.repeat_x = False
        self._output_texture.repeat_y = False
        self._fbo = self._ctx.framebuffer(color_attachments=[self._output_texture])
        self._blank_texture = self._ctx.texture((1, 1), 4, data=b"\x00\x00\x00\xff")
        self._blank_texture.filter = (GL_NEAREST, GL_NEAREST)
        self._font_texture = self._create_font_texture()

    def _compile(self, shader_key: str, source: str) -> None:
        self._ensure_context()
        assert self._ctx is not None

        self._inputs = parse_isf_inputs(source, {})
        self._passes = _pass_targets(source)
        pass_names = [item["target"] for item in self._passes if item.get("target")]
        frag = _translate_fragment(source, self._inputs, pass_names)

        try:
            self._program = self._ctx.program(
                vertex_shader=VERTEX_SHADER,
                fragment_shader=frag,
            )
        except Exception as e:
            raise ShaderClawNativeError(f"Shader compile failed: {e}") from e

        quad = np.array(
            [
                -1.0,
                -1.0,
                0.0,
                0.0,
                1.0,
                -1.0,
                1.0,
                0.0,
                -1.0,
                1.0,
                0.0,
                1.0,
                1.0,
                1.0,
                1.0,
                1.0,
            ],
            dtype="f4",
        )
        buffer = self._ctx.buffer(quad.tobytes())
        self._vao = self._ctx.vertex_array(
            self._program,
            [(buffer, "2f 2f", "in_pos", "in_uv")],
        )

        self._targets = {}
        for pass_ in self._passes:
            target = pass_.get("target")
            if target:
                self._targets[target] = _RenderTarget(
                    self._ctx,
                    self.width,
                    self.height,
                    bool(pass_.get("persistent", False)),
                )
        self._shader_key = shader_key
        self._frame_index = 0
        self._started_at = time.monotonic()
        self._last_time = self._started_at

    def _render(self) -> None:
        assert self._ctx is not None
        assert self._program is not None
        assert self._vao is not None
        assert self._fbo is not None

        passes = self._passes or [{"target": "", "persistent": False}]
        for pass_index, pass_ in enumerate(passes):
            target = pass_.get("target") or ""
            if target:
                fbo = self._targets[target].write_fbo
            else:
                fbo = self._fbo
            fbo.use()
            self._ctx.viewport = (0, 0, fbo.width, fbo.height)
            if not pass_.get("persistent") or not target:
                self._ctx.clear(0.0, 0.0, 0.0, 0.0)
            self._upload_uniforms(pass_index, fbo.width, fbo.height)
            self._vao.render(mode=5)
            if target:
                self._targets[target].swap_after_write()
        self._frame_index += 1

    def _upload_uniforms(self, pass_index: int, pass_width: int, pass_height: int) -> None:
        assert self._program is not None
        assert self._blank_texture is not None
        assert self._font_texture is not None

        now = time.monotonic()
        values: dict[str, Any] = {
            "TIME": now - self._started_at,
            "TIMEDELTA": now - self._last_time,
            "RENDERSIZE": (float(pass_width), float(pass_height)),
            "PASSINDEX": pass_index,
            "FRAMEINDEX": self._frame_index,
            "mousePos": (0.5, 0.5),
            "mouseDelta": (0.0, 0.0),
            "mouseDown": 0.0,
            "pinchHold": 0.0,
            "audioLevel": 0.0,
            "audioBass": 0.0,
            "audioMid": 0.0,
            "audioHigh": 0.0,
            "_voiceLevel": 0.0,
            "_voiceGlitch": 0.0,
        }
        for name, value in values.items():
            self._set_uniform(name, value)
        self._last_time = now

        texture_unit = 0
        for input_ in self._inputs:
            if input_.type == "image":
                self._blank_texture.use(location=texture_unit)
                self._set_uniform(input_.name, texture_unit)
                self._set_uniform(f"IMG_SIZE_{input_.name}", (float(self.width), float(self.height)))
                self._set_uniform(f"_flip_{input_.name}", False)
                texture_unit += 1

        for pass_name, target in self._targets.items():
            target.current_texture().use(location=texture_unit)
            self._set_uniform(pass_name, texture_unit)
            texture_unit += 1

        self._blank_texture.use(location=texture_unit)
        self._set_uniform("audioFFT", texture_unit)
        texture_unit += 1

        self._font_texture.use(location=texture_unit)
        self._set_uniform("fontAtlasTex", texture_unit)

        for input_ in self._inputs:
            value = input_.value
            if input_.type == "float":
                self._set_uniform(input_.name, float(_json_number(value, _json_number(input_.default, 0.0))))
            elif input_.type == "color":
                color = value if isinstance(value, list) else input_.default
                color_values = [float(_json_number(v, 1.0)) for v in (color or [1, 1, 1, 1])[:4]]
                while len(color_values) < 4:
                    color_values.append(1.0)
                self._set_uniform(input_.name, tuple(color_values))
            elif input_.type in {"bool", "event"}:
                self._set_uniform(input_.name, bool(_json_bool(value, False)))
            elif input_.type == "point2D":
                point = value if isinstance(value, list) else input_.default
                point_values = [float(_json_number(v, 0.5)) for v in (point or [0.5, 0.5])[:2]]
                while len(point_values) < 2:
                    point_values.append(0.5)
                self._set_uniform(input_.name, tuple(point_values))
            elif input_.type == "long":
                self._set_uniform(input_.name, int(_json_number(value, _json_number(input_.default, 0))))
            elif input_.type == "text":
                text = str(value if value is not None else input_.default or "").upper()
                max_len = input_.max_length or 48
                if len(text) > max_len:
                    text = text[-max_len:]
                self._set_uniform(f"{input_.name}_len", len(text))
                for index in range(max_len):
                    code = _char_code(text[index]) if index < len(text) else 26
                    self._set_uniform(f"{input_.name}_{index}", code)

    def _set_uniform(self, name: str, value: Any) -> None:
        assert self._program is not None
        if name in self._program:
            self._program[name].value = value

    def _create_font_texture(self) -> Any:
        assert self._ctx is not None
        chars = "ABCDEFGHIJKLMNOPQRSTUVWXYZ 0123456789"
        cell_w = 128
        cell_h = 180
        atlas = Image.new("L", (len(chars) * cell_w, cell_h), 0)
        draw = ImageDraw.Draw(atlas)
        font = _load_font(int(cell_h * 0.72))
        for index, char in enumerate(chars):
            if char == " ":
                continue
            bbox = draw.textbbox((0, 0), char, font=font)
            text_w = bbox[2] - bbox[0]
            text_h = bbox[3] - bbox[1]
            x = index * cell_w + (cell_w - text_w) / 2 - bbox[0]
            y = (cell_h - text_h) / 2 - bbox[1]
            draw.text((x, y), char, fill=255, font=font)
        flipped = atlas.transpose(Image.Transpose.FLIP_TOP_BOTTOM)
        texture = self._ctx.texture(flipped.size, 1, data=flipped.tobytes())
        texture.filter = (GL_LINEAR, GL_LINEAR)
        texture.repeat_x = False
        texture.repeat_y = False
        return texture


def _load_font(size: int) -> ImageFont.FreeTypeFont | ImageFont.ImageFont:
    candidates = [
        "/System/Library/Fonts/Supplemental/Arial.ttf",
        "/System/Library/Fonts/Helvetica.ttc",
        "/System/Library/Fonts/SFNS.ttf",
        "/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf",
        "/usr/share/fonts/truetype/liberation2/LiberationSans-Regular.ttf",
        "C:/Windows/Fonts/arial.ttf",
    ]
    for path in candidates:
        try:
            return ImageFont.truetype(path, size=size)
        except OSError:
            continue
    return ImageFont.load_default()
