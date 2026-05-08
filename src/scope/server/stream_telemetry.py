"""Shared stream telemetry helpers for Scope diagnostics."""

from __future__ import annotations

import os
import time
from typing import Any

STREAM_TELEMETRY_PREFIX = "[STREAM-TELEMETRY]"


def telemetry_interval_seconds() -> float:
    """Return the interval for periodic stream telemetry log lines."""
    raw = os.getenv("SCOPE_STREAM_TELEMETRY_INTERVAL_SECONDS", "5")
    try:
        interval = float(raw)
    except ValueError:
        return 5.0
    return max(1.0, interval)


def should_log_telemetry(last_logged_at: float | None, now: float | None = None) -> bool:
    """Return whether a periodic telemetry line should be emitted now."""
    now = time.time() if now is None else now
    if last_logged_at is None:
        return True
    return now - last_logged_at >= telemetry_interval_seconds()


def compact_rtp_stats(stats: dict[str, Any] | None) -> dict[str, Any]:
    """Extract the high-signal RTP fields from a full aiortc stats sample."""
    if not stats:
        return {}
    aggregate = stats.get("aggregate") or {}
    compact: dict[str, Any] = {}
    for direction, by_kind in aggregate.items():
        if not isinstance(by_kind, dict):
            continue
        video = by_kind.get("video")
        if not isinstance(video, dict):
            continue
        compact[f"{direction}_video"] = {
            "bitrate_mbps": _mbps(video.get("recent_bitrate_bps")),
            "recent_loss_pct": video.get("recent_loss_pct"),
            "max_jitter_ms": video.get("max_jitter_ms"),
            "packets_per_second": video.get("recent_packets_per_second"),
            "frames_sent_fps": video.get("recent_frames_sent_per_second"),
            "frames_encoded_fps": video.get("recent_frames_encoded_per_second"),
            "frames_received_fps": video.get("recent_frames_received_per_second"),
            "frames_decoded_fps": video.get("recent_frames_decoded_per_second"),
            "frames_dropped": video.get("frames_dropped"),
            "recent_frames_dropped": video.get("recent_frames_dropped"),
        }
    return compact


def compact_frame_processor_stats(stats: dict[str, Any] | None) -> dict[str, Any]:
    """Extract a small frame-processor summary for telemetry logs."""
    if not stats:
        return {}
    return {
        "frames_in": stats.get("frames_in"),
        "frames_out": stats.get("frames_out"),
        "fps_in": _round(stats.get("fps_in")),
        "fps_out": _round(stats.get("fps_out")),
        "pipeline_fps": _round(stats.get("pipeline_fps")),
        "relay_mode": stats.get("relay_mode"),
        "frames_to_cloud": stats.get("frames_to_cloud"),
        "frames_from_cloud": stats.get("frames_from_cloud"),
        "cloud_relay": stats.get("cloud_relay"),
    }


def _mbps(value: Any) -> float | None:
    if value is None:
        return None
    try:
        return round(float(value) / 1_000_000, 3)
    except (TypeError, ValueError):
        return None


def _round(value: Any) -> float | None:
    if value is None:
        return None
    try:
        return round(float(value), 1)
    except (TypeError, ValueError):
        return None
