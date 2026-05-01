"""Cue session polling node for Scope graphs."""

from __future__ import annotations

import json
import os
import time
import urllib.error
import urllib.parse
import urllib.request
from typing import Any, ClassVar

from scope.core.nodes.base import BaseNode, NodeDefinition, NodeParam, NodePort

DEFAULT_CUE_BASE_URL = "http://127.0.0.1:8792"
DEFAULT_SESSION_ID = "demo"
DEFAULT_ACTION_TYPE = "video.update_prompt"


class CueSessionNode(BaseNode):
    """Expose a Cue session's latest transcript and prompt action to Scope."""

    node_type_id: ClassVar[str] = "cue.session"

    def __init__(self, node_id: str = "", config: dict[str, Any] | None = None):
        super().__init__(node_id, config)
        self._last_poll_at = 0.0
        self._last_signature: tuple[Any, ...] | None = None
        self._last_outputs: dict[str, Any] = {}
        self._tick = 0

    @classmethod
    def get_definition(cls) -> NodeDefinition:
        return NodeDefinition(
            node_type_id=cls.node_type_id,
            display_name="Cue Session",
            category="cue",
            description="Poll a Cue session and expose transcript and prompt actions.",
            continuous=True,
            inputs=[
                NodePort(
                    name="refresh",
                    port_type="trigger",
                    required=False,
                    description="Force an immediate Cue state poll",
                )
            ],
            outputs=[
                NodePort(
                    name="prompt",
                    port_type="string",
                    required=False,
                    description="Latest matching Cue prompt action payload",
                ),
                NodePort(
                    name="reset",
                    port_type="boolean",
                    required=False,
                    description="Latest matching Cue prompt reset flag",
                ),
                NodePort(
                    name="action_json",
                    port_type="string",
                    required=False,
                    description="Latest matching Cue action serialized as JSON",
                ),
                NodePort(
                    name="transcript",
                    port_type="string",
                    required=False,
                    description="Current Cue transcript snapshot",
                ),
                NodePort(
                    name="status",
                    port_type="string",
                    required=False,
                    description="Cue polling status",
                ),
                NodePort(
                    name="tick",
                    port_type="trigger",
                    required=False,
                    description="Increments when observed Cue state changes",
                ),
                NodePort(
                    name="decision_count",
                    port_type="number",
                    required=False,
                    description="Cue decision count snapshot",
                ),
                NodePort(
                    name="observation_count",
                    port_type="number",
                    required=False,
                    description="Cue observation count snapshot",
                ),
            ],
            params=[
                NodeParam(
                    name="cue_base_url",
                    param_type="string",
                    default=os.environ.get("CUE_BASE_URL", DEFAULT_CUE_BASE_URL),
                    description="Cue server base URL",
                ),
                NodeParam(
                    name="session_id",
                    param_type="string",
                    default=os.environ.get("CUE_SESSION_ID", DEFAULT_SESSION_ID),
                    description="Cue session id",
                ),
                NodeParam(
                    name="action_type",
                    param_type="string",
                    default=DEFAULT_ACTION_TYPE,
                    description="Cue action type to expose",
                ),
                NodeParam(
                    name="poll_interval_ms",
                    param_type="number",
                    default=250,
                    description="Polling interval",
                    ui={"min": 50, "max": 5000, "step": 50},
                ),
                NodeParam(
                    name="timeout_ms",
                    param_type="number",
                    default=500,
                    description="HTTP timeout",
                    ui={"min": 50, "max": 5000, "step": 50},
                ),
                NodeParam(
                    name="enabled",
                    param_type="boolean",
                    default=True,
                    description="Enable Cue polling",
                ),
            ],
        )

    def execute(self, inputs: dict[str, Any], **kwargs) -> dict[str, Any]:
        params = {**self.config, **kwargs}
        enabled = _as_bool(params.get("enabled"), True)
        if not enabled:
            return self._changed_outputs({"status": "disabled"})

        forced = "refresh" in inputs
        interval_seconds = max(_as_float(params.get("poll_interval_ms"), 250), 0) / 1000
        now = time.monotonic()
        if not forced and now - self._last_poll_at < interval_seconds:
            return {}

        self._last_poll_at = now
        try:
            state = _fetch_cue_state(
                base_url=str(
                    params.get("cue_base_url")
                    or os.environ.get("CUE_BASE_URL")
                    or DEFAULT_CUE_BASE_URL
                ),
                session_id=str(
                    params.get("session_id")
                    or os.environ.get("CUE_SESSION_ID")
                    or DEFAULT_SESSION_ID
                ),
                timeout_seconds=max(_as_float(params.get("timeout_ms"), 500), 50)
                / 1000,
            )
            outputs = _outputs_from_state(
                state,
                action_type=str(params.get("action_type") or DEFAULT_ACTION_TYPE),
            )
            return self._changed_outputs(outputs)
        except Exception as exc:
            return self._changed_outputs({"status": f"error: {exc}"})

    def _changed_outputs(self, outputs: dict[str, Any]) -> dict[str, Any]:
        signature = tuple(sorted(outputs.items(), key=lambda item: item[0]))
        if signature == self._last_signature:
            return {}
        self._last_signature = signature
        self._tick += 1
        next_outputs = {**self._last_outputs, **outputs, "tick": self._tick}
        self._last_outputs = next_outputs
        return next_outputs


def _fetch_cue_state(
    *, base_url: str, session_id: str, timeout_seconds: float
) -> dict[str, Any]:
    clean_base = base_url.rstrip("/")
    quoted_session = urllib.parse.quote(session_id, safe="")
    url = f"{clean_base}/sessions/{quoted_session}/state"
    request = urllib.request.Request(url, headers={"Accept": "application/json"})
    try:
        with urllib.request.urlopen(request, timeout=timeout_seconds) as response:
            payload = response.read().decode("utf-8")
    except urllib.error.HTTPError as exc:
        raise RuntimeError(f"Cue state request failed with HTTP {exc.code}") from exc
    except urllib.error.URLError as exc:
        raise RuntimeError(f"Cue state request failed: {exc.reason}") from exc

    decoded = json.loads(payload)
    if not isinstance(decoded, dict):
        raise RuntimeError("Cue state response was not an object")
    return decoded


def _outputs_from_state(state: dict[str, Any], *, action_type: str) -> dict[str, Any]:
    snapshot = _object(state.get("state"))
    latest_action = _latest_matching_action(state, action_type)
    action_payload = _object(latest_action.get("payload")) if latest_action else {}
    prompt = action_payload.get("prompt")
    reset = action_payload.get("reset")

    return {
        "prompt": prompt if isinstance(prompt, str) else "",
        "reset": bool(reset) if reset is not None else False,
        "action_json": _compact_json(latest_action) if latest_action else "",
        "transcript": _string(snapshot.get("transcript")),
        "status": "ok",
        "decision_count": _as_float(snapshot.get("decisionCount"), 0),
        "observation_count": _as_float(snapshot.get("observationCount"), 0),
    }


def _latest_matching_action(
    state: dict[str, Any], action_type: str
) -> dict[str, Any] | None:
    for collection_name in ("decisionHistory", "decisionTrace"):
        collection = state.get(collection_name)
        if not isinstance(collection, list):
            continue
        for item in reversed(collection):
            candidate = _object(item)
            result = _object(candidate.get("result"))
            actions = result.get("actions")
            if not isinstance(actions, list):
                continue
            for action in reversed(actions):
                action_object = _object(action)
                if action_object.get("type") == action_type:
                    return action_object
    return None


def _object(value: Any) -> dict[str, Any]:
    return value if isinstance(value, dict) else {}


def _string(value: Any) -> str:
    return value if isinstance(value, str) else ""


def _as_bool(value: Any, default: bool) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        return value.strip().lower() not in {"", "0", "false", "no", "off"}
    return default


def _as_float(value: Any, default: float) -> float:
    if isinstance(value, bool):
        return default
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def _compact_json(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"))
