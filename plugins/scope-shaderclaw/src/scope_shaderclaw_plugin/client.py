"""HTTP client for the local ShaderClaw3 remote-control API."""

from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.parse import urljoin
from urllib.request import Request, urlopen

DEFAULT_SHADERCLAW_URL = "http://127.0.0.1:7778"


class ShaderClawError(RuntimeError):
    """Raised when ShaderClaw3 cannot complete a requested operation."""


@dataclass(frozen=True)
class ShaderEntry:
    """Manifest entry resolved to a concrete shader file."""

    id: int | str | None
    title: str
    file: str


class ShaderClawClient:
    """Small synchronous client for ShaderClaw3's HTTP bridge."""

    def __init__(self, base_url: str = DEFAULT_SHADERCLAW_URL, timeout_s: float = 5.0):
        self.base_url = (base_url or DEFAULT_SHADERCLAW_URL).rstrip("/") + "/"
        self.timeout_s = timeout_s

    def _url(self, path: str) -> str:
        return urljoin(self.base_url, path.lstrip("/"))

    def _json_request(
        self,
        path: str,
        *,
        method: str = "GET",
        body: dict[str, Any] | None = None,
    ) -> Any:
        data = None
        headers = {"Accept": "application/json"}
        if body is not None:
            data = json.dumps(body).encode("utf-8")
            headers["Content-Type"] = "application/json"

        request = Request(self._url(path), data=data, headers=headers, method=method)
        try:
            with urlopen(request, timeout=self.timeout_s) as response:
                raw = response.read()
        except HTTPError as e:
            raw_error = e.read().decode("utf-8", errors="replace")
            try:
                payload = json.loads(raw_error)
                message = payload.get("error") or raw_error
            except json.JSONDecodeError:
                message = raw_error or e.reason
            raise ShaderClawError(f"ShaderClaw HTTP {e.code}: {message}") from e
        except URLError as e:
            raise ShaderClawError(
                f"Cannot reach ShaderClaw at {self.base_url}: {e}"
            ) from e

        try:
            return json.loads(raw.decode("utf-8"))
        except json.JSONDecodeError as e:
            raise ShaderClawError("ShaderClaw returned non-JSON response") from e

    def remote_control(
        self,
        action: str,
        params: dict[str, Any] | None = None,
        timeout_ms: int | None = None,
    ) -> Any:
        """Send one `/api/rc` action to the connected ShaderClaw browser."""

        body: dict[str, Any] = {"action": action, "params": params or {}}
        if timeout_ms is not None:
            body["timeout"] = timeout_ms

        payload = self._json_request("/api/rc", method="POST", body=body)
        if not isinstance(payload, dict) or not payload.get("ok"):
            error = payload.get("error") if isinstance(payload, dict) else payload
            raise ShaderClawError(str(error or "ShaderClaw command failed"))
        return payload.get("result")

    def manifest(self) -> list[dict[str, Any]]:
        """Load the built-in shader manifest from the ShaderClaw static server."""

        payload = self._json_request("/shaders/manifest.json")
        if not isinstance(payload, list):
            raise ShaderClawError("ShaderClaw shader manifest is not a list")
        return [entry for entry in payload if isinstance(entry, dict)]

    def resolve_shader(self, shader: str | int) -> ShaderEntry:
        """Resolve a title, numeric id, or `.fs` file name to a manifest entry."""

        manifest = self.manifest()
        value = str(shader).strip()
        value_lower = value.lower()

        entry: dict[str, Any] | None = None
        if value.isdigit():
            shader_id = int(value)
            entry = next(
                (item for item in manifest if item.get("id") == shader_id), None
            )
        if entry is None:
            entry = next((item for item in manifest if item.get("file") == value), None)
        if entry is None:
            entry = next(
                (
                    item
                    for item in manifest
                    if str(item.get("title", "")).lower() == value_lower
                ),
                None,
            )
        if entry is None:
            entry = next(
                (
                    item
                    for item in manifest
                    if value_lower in str(item.get("title", "")).lower()
                ),
                None,
            )

        if entry is None:
            if value.endswith(".fs"):
                return ShaderEntry(id=None, title=value, file=value)
            available = ", ".join(
                str(item.get("title") or item.get("file"))
                for item in manifest[:8]
                if item.get("title") or item.get("file")
            )
            raise ShaderClawError(
                f"Shader not found: {value}. Try a manifest title, id, or file. "
                f"Examples: {available}"
            )

        file_name = str(entry.get("file") or "").strip()
        if not file_name:
            raise ShaderClawError(f"Shader manifest entry has no file: {entry}")
        return ShaderEntry(
            id=entry.get("id"),
            title=str(entry.get("title") or file_name),
            file=file_name,
        )

    def load_shader(
        self,
        shader: str | int,
        *,
        layer_id: str = "shader",
        timeout_ms: int | None = None,
    ) -> ShaderEntry:
        """Load a built-in ShaderClaw shader file into the browser renderer."""

        entry = self.resolve_shader(shader)
        result = self.remote_control(
            "load_shader_file",
            {"folder": "shaders", "file": entry.file, "layerId": layer_id},
            timeout_ms,
        )
        if isinstance(result, dict) and result.get("ok") is False:
            raise ShaderClawError(str(result.get("errors") or result.get("error")))
        return entry

    def set_parameters(
        self,
        parameters: dict[str, Any],
        *,
        timeout_ms: int | None = None,
    ) -> None:
        """Apply ShaderClaw parameter values to the focused browser layer."""

        for name, value in parameters.items():
            result = self.remote_control(
                "set_parameter",
                {"name": name, "value": value},
                timeout_ms,
            )
            if isinstance(result, dict) and result.get("ok") is False:
                raise ShaderClawError(str(result.get("error") or result))

    def get_parameters(self, *, timeout_ms: int | None = None) -> list[dict[str, Any]]:
        """Return ShaderClaw's current ISF parameter metadata."""

        result = self.remote_control("get_parameters", timeout_ms=timeout_ms)
        if isinstance(result, dict) and isinstance(result.get("inputs"), list):
            return result["inputs"]
        return []

    def screenshot_data_url(self, *, timeout_ms: int | None = None) -> str:
        """Capture the current WebGL canvas as a PNG data URL."""

        result = self.remote_control("screenshot", timeout_ms=timeout_ms)
        if isinstance(result, dict) and isinstance(result.get("dataUrl"), str):
            return result["dataUrl"]
        raise ShaderClawError("ShaderClaw screenshot did not return a dataUrl")
