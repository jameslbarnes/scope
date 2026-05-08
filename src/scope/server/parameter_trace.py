"""Trace helpers for prompt/reset parameter updates."""

from __future__ import annotations

import hashlib
import time
from typing import Any

PARAMETER_TRACE_PREFIX = "[PARAM-TRACE]"


def should_trace_parameters(parameters: dict[str, Any] | None) -> bool:
    """Return whether a parameter update is useful for prompt latency tracing."""
    if not isinstance(parameters, dict):
        return False
    return any(key in parameters for key in ("prompts", "transition", "reset_cache"))


def parameter_trace_summary(parameters: dict[str, Any] | None) -> dict[str, Any]:
    """Build a compact, stable summary that can correlate logs across processes."""
    params = parameters or {}
    prompts = _trace_prompts(params)
    prompt_signature = "|".join(
        f"{item.get('weight', '')}:{item.get('text', '')}" for item in prompts
    )
    prompt_hash = (
        hashlib.sha1(prompt_signature.encode("utf-8")).hexdigest()[:10]
        if prompt_signature
        else None
    )
    prompt_preview = " | ".join(
        text
        for text in (item.get("text") for item in prompts)
        if isinstance(text, str) and text
    )
    if len(prompt_preview) > 120:
        prompt_preview = f"{prompt_preview[:117]}..."

    return {
        "prompt_hash": prompt_hash,
        "prompt_preview": prompt_preview or None,
        "prompt_count": len(prompts),
        "node_id": params.get("node_id"),
        "reset_cache": params.get("reset_cache"),
        "has_transition": "transition" in params,
        "keys": sorted(str(key) for key in params),
        "ts": round(time.time(), 3),
    }


def _trace_prompts(parameters: dict[str, Any]) -> list[dict[str, Any]]:
    transition = parameters.get("transition")
    if isinstance(transition, dict):
        transition_prompts = _normalize_prompt_list(transition.get("target_prompts"))
        if transition_prompts:
            return transition_prompts
    return _normalize_prompt_list(parameters.get("prompts"))


def _normalize_prompt_list(value: Any) -> list[dict[str, Any]]:
    if isinstance(value, str):
        return [{"text": value, "weight": None}]
    if not isinstance(value, list):
        return []

    prompts: list[dict[str, Any]] = []
    for item in value:
        if isinstance(item, str):
            prompts.append({"text": item, "weight": None})
            continue
        if isinstance(item, dict):
            text = item.get("text")
            weight = item.get("weight")
        else:
            text = getattr(item, "text", None)
            weight = getattr(item, "weight", None)
        if isinstance(text, str):
            prompts.append({"text": text, "weight": weight})
    return prompts
