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
DEFAULT_INPUT_MAPPING = {
    "chat": "transcript.segment",
    "chat_in": "transcript.segment",
    "context_json": "scope.context",
}
DEFAULT_OUTPUT_MAPPING = {
    "prompt": "longlive.prompt",
    "transcript": "chat.transcript",
    "action_json": "cue.action",
}


class CueSessionNode(BaseNode):
    """Expose a Cue session's latest transcript and prompt action to Scope."""

    node_type_id: ClassVar[str] = "cue.session"

    def __init__(self, node_id: str = "", config: dict[str, Any] | None = None):
        super().__init__(node_id, config)
        self._last_poll_at = 0.0
        self._last_signature: tuple[Any, ...] | None = None
        self._last_outputs: dict[str, Any] = {}
        self._tick = 0
        self._last_chat_submit_count = 0.0
        self._last_input_signatures: dict[str, str] = {}

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
                ),
                NodePort(
                    name="chat_in",
                    port_type="string",
                    required=False,
                    description="Forward upstream text into Cue as a transcript segment",
                ),
                NodePort(
                    name="context_json",
                    port_type="string",
                    required=False,
                    description="Forward upstream structured context into Cue",
                ),
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
                NodePort(
                    name="chat_status",
                    port_type="string",
                    required=False,
                    description="Status of chat and mapped input submissions",
                ),
                NodePort(
                    name="mapping_json",
                    port_type="string",
                    required=False,
                    description="Current Cue input/output mapping serialized as JSON",
                ),
            ],
            params=[
                NodeParam(
                    name="cue_base_url",
                    param_type="string",
                    default=os.environ.get("CUE_BASE_URL", DEFAULT_CUE_BASE_URL),
                    description="Cue server base URL",
                    ui={"section": "connection"},
                ),
                NodeParam(
                    name="session_id",
                    param_type="string",
                    default=os.environ.get("CUE_SESSION_ID", DEFAULT_SESSION_ID),
                    description="Cue session id",
                    ui={"section": "connection"},
                ),
                NodeParam(
                    name="action_type",
                    param_type="string",
                    default=DEFAULT_ACTION_TYPE,
                    description="Cue action type to expose",
                    ui={"section": "connection"},
                ),
                NodeParam(
                    name="poll_interval_ms",
                    param_type="number",
                    default=250,
                    description="Polling interval",
                    ui={"min": 50, "max": 5000, "step": 50, "section": "connection"},
                ),
                NodeParam(
                    name="timeout_ms",
                    param_type="number",
                    default=500,
                    description="HTTP timeout",
                    ui={"min": 50, "max": 5000, "step": 50, "section": "connection"},
                ),
                NodeParam(
                    name="enabled",
                    param_type="boolean",
                    default=True,
                    description="Enable Cue polling",
                    ui={"section": "connection"},
                ),
                NodeParam(
                    name="cue_file_path",
                    param_type="string",
                    default="",
                    description="Loaded .cue file name or path",
                    ui={"widget": "cue_hidden", "section": "cue_ui"},
                    convertible_to_input=False,
                ),
                NodeParam(
                    name="cue_file_json",
                    param_type="string",
                    default="",
                    description="Loaded .cue file contents",
                    ui={"widget": "cue_hidden", "section": "cue_ui"},
                    convertible_to_input=False,
                ),
                NodeParam(
                    name="input_mapping_json",
                    param_type="string",
                    default=_compact_json(DEFAULT_INPUT_MAPPING),
                    description="Cue input mapping JSON",
                    ui={"widget": "cue_hidden", "section": "cue_ui"},
                    convertible_to_input=False,
                ),
                NodeParam(
                    name="output_mapping_json",
                    param_type="string",
                    default=_compact_json(DEFAULT_OUTPUT_MAPPING),
                    description="Cue output mapping JSON",
                    ui={"widget": "cue_hidden", "section": "cue_ui"},
                    convertible_to_input=False,
                ),
                NodeParam(
                    name="chat_text",
                    param_type="string",
                    default="",
                    description="Pending chat text from the Cue chat surface",
                    ui={"widget": "cue_hidden", "section": "cue_ui"},
                    convertible_to_input=False,
                ),
                NodeParam(
                    name="chat_submit_count",
                    param_type="number",
                    default=0,
                    description="Monotonic chat submit counter",
                    ui={"widget": "cue_hidden", "section": "cue_ui"},
                    convertible_to_input=False,
                ),
            ],
        )

    def execute(self, inputs: dict[str, Any], **kwargs) -> dict[str, Any]:
        params = {**self.config, **kwargs}
        enabled = _as_bool(params.get("enabled"), True)
        if not enabled:
            return self._changed_outputs({"status": "disabled"})

        connection = _connection_from_params(params)
        chat_status = self._submit_pending_observations(
            inputs=inputs,
            params=params,
            connection=connection,
        )

        forced = "refresh" in inputs
        interval_seconds = max(_as_float(params.get("poll_interval_ms"), 250), 0) / 1000
        now = time.monotonic()
        if not forced and now - self._last_poll_at < interval_seconds:
            return (
                self._changed_outputs({"chat_status": chat_status})
                if chat_status
                else {}
            )

        self._last_poll_at = now
        try:
            state = _fetch_cue_state(
                base_url=connection["base_url"],
                session_id=connection["session_id"],
                timeout_seconds=connection["timeout_seconds"],
            )
            outputs = _outputs_from_state(
                state,
                action_type=str(params.get("action_type") or DEFAULT_ACTION_TYPE),
            )
            if chat_status:
                outputs["chat_status"] = chat_status
            outputs["mapping_json"] = _mapping_json_from_params(params)
            return self._changed_outputs(outputs)
        except Exception as exc:
            outputs = {"status": f"error: {exc}"}
            if chat_status:
                outputs["chat_status"] = chat_status
            return self._changed_outputs(outputs)

    def _submit_pending_observations(
        self,
        *,
        inputs: dict[str, Any],
        params: dict[str, Any],
        connection: dict[str, Any],
    ) -> str:
        statuses: list[str] = []

        submit_count = _as_float(params.get("chat_submit_count"), 0)
        chat_text = _string(params.get("chat_text")).strip()
        if chat_text and submit_count > self._last_chat_submit_count:
            self._last_chat_submit_count = submit_count
            try:
                _post_cue_observation(
                    connection=connection,
                    observation=_transcript_observation(
                        chat_text,
                        source="scope.chat",
                    ),
                )
                statuses.append("chat sent")
            except Exception as exc:
                statuses.append(f"chat error: {exc}")

        input_mapping = _json_object(
            params.get("input_mapping_json"), DEFAULT_INPUT_MAPPING
        )
        if "chat_in" in inputs:
            text = _string(inputs.get("chat_in")).strip()
            if text:
                statuses.append(
                    self._post_deduped_input(
                        "chat_in",
                        text,
                        input_mapping,
                        connection,
                    )
                )
        if "context_json" in inputs:
            text = _string(inputs.get("context_json")).strip()
            if text:
                statuses.append(
                    self._post_deduped_input(
                        "context_json",
                        text,
                        input_mapping,
                        connection,
                    )
                )

        return "; ".join(status for status in statuses if status)

    def _post_deduped_input(
        self,
        port_name: str,
        value: str,
        input_mapping: dict[str, Any],
        connection: dict[str, Any],
    ) -> str:
        if self._last_input_signatures.get(port_name) == value:
            return ""
        self._last_input_signatures[port_name] = value
        cue_type = _string(input_mapping.get(port_name)) or DEFAULT_INPUT_MAPPING.get(
            port_name, "scope.context"
        )
        try:
            if cue_type == "transcript.segment":
                observation = _transcript_observation(
                    value,
                    source=f"scope.input.{port_name}",
                )
            else:
                observation = {
                    "type": cue_type,
                    "source": f"scope.input.{port_name}",
                    "timestamp": time.time(),
                    "payload": _maybe_json_payload(value),
                }
            _post_cue_observation(connection=connection, observation=observation)
            return f"{port_name} sent"
        except Exception as exc:
            return f"{port_name} error: {exc}"

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


def _post_cue_observation(
    *, connection: dict[str, Any], observation: dict[str, Any]
) -> dict[str, Any]:
    clean_base = str(connection["base_url"]).rstrip("/")
    quoted_session = urllib.parse.quote(str(connection["session_id"]), safe="")
    url = f"{clean_base}/sessions/{quoted_session}/observations"
    body = json.dumps(observation).encode("utf-8")
    request = urllib.request.Request(
        url,
        data=body,
        method="POST",
        headers={
            "Accept": "application/json",
            "Content-Type": "application/json",
        },
    )
    try:
        with urllib.request.urlopen(
            request,
            timeout=float(connection["timeout_seconds"]),
        ) as response:
            payload = response.read().decode("utf-8")
    except urllib.error.HTTPError as exc:
        raise RuntimeError(f"Cue observation failed with HTTP {exc.code}") from exc
    except urllib.error.URLError as exc:
        raise RuntimeError(f"Cue observation failed: {exc.reason}") from exc

    decoded = json.loads(payload) if payload else {}
    return decoded if isinstance(decoded, dict) else {}


def _connection_from_params(params: dict[str, Any]) -> dict[str, Any]:
    return {
        "base_url": str(
            params.get("cue_base_url")
            or os.environ.get("CUE_BASE_URL")
            or DEFAULT_CUE_BASE_URL
        ),
        "session_id": str(
            params.get("session_id")
            or os.environ.get("CUE_SESSION_ID")
            or DEFAULT_SESSION_ID
        ),
        "timeout_seconds": max(_as_float(params.get("timeout_ms"), 500), 50) / 1000,
    }


def _transcript_observation(text: str, *, source: str) -> dict[str, Any]:
    return {
        "type": "transcript.segment",
        "source": source,
        "timestamp": time.time(),
        "payload": {
            "text": text,
            "isFinal": True,
        },
    }


def _maybe_json_payload(value: str) -> Any:
    try:
        return json.loads(value)
    except json.JSONDecodeError:
        return {"text": value}


def _mapping_json_from_params(params: dict[str, Any]) -> str:
    return _compact_json(
        {
            "inputs": _json_object(
                params.get("input_mapping_json"),
                DEFAULT_INPUT_MAPPING,
            ),
            "outputs": _json_object(
                params.get("output_mapping_json"),
                DEFAULT_OUTPUT_MAPPING,
            ),
            "cue_file_path": _string(params.get("cue_file_path")),
        }
    )


def _json_object(value: Any, default: dict[str, Any]) -> dict[str, Any]:
    if isinstance(value, dict):
        return value
    if isinstance(value, str) and value.strip():
        try:
            decoded = json.loads(value)
        except json.JSONDecodeError:
            return dict(default)
        if isinstance(decoded, dict):
            return decoded
    return dict(default)


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
