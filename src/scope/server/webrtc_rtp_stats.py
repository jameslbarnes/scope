"""Summarise aiortc RTP stats for Scope diagnostics."""

from __future__ import annotations

import time
from typing import Any

RTP_STAT_TYPES = {
    "inbound-rtp": "inbound",
    "outbound-rtp": "outbound",
    "remote-inbound-rtp": "remote_inbound",
    "remote-outbound-rtp": "remote_outbound",
}

PACKET_RECEIVED_TYPES = {"inbound", "remote_inbound"}
PACKET_SENT_TYPES = {"outbound", "remote_outbound"}
FRAME_COUNTER_ATTRS = {
    "framesSent": "frames_sent",
    "framesEncoded": "frames_encoded",
    "framesReceived": "frames_received",
    "framesDecoded": "frames_decoded",
    "framesDropped": "frames_dropped",
}


class RtpStatsSampler:
    """Keep previous RTP counters so each sample can report recent deltas."""

    def __init__(self) -> None:
        self._previous: dict[str, dict[str, float | int | str | None]] = {}

    def sample(self, report: Any, now: float | None = None) -> dict[str, Any]:
        now = time.time() if now is None else now
        streams = []
        current_by_key: dict[str, dict[str, float | int | str | None]] = {}

        for stat in _iter_stats(report):
            stat_type = getattr(stat, "type", None)
            direction = RTP_STAT_TYPES.get(stat_type)
            if direction is None:
                continue

            kind = getattr(stat, "kind", None) or "unknown"
            key = _stream_key(direction, stat)
            current = _stream_counters(direction, kind, stat, now)
            previous = self._previous.get(key)
            recent = _recent_counters(direction, current, previous, now)
            current_by_key[key] = current
            streams.append(
                {
                    **_stream_snapshot(direction, kind, stat, current),
                    "recent": recent,
                }
            )

        self._previous = current_by_key
        return {
            "sampled_at": now,
            "aggregate": _aggregate_streams(streams),
            "streams": streams,
        }


def _iter_stats(report: Any):
    if hasattr(report, "values"):
        return report.values()
    return report


def _stream_key(direction: str, stat: Any) -> str:
    stat_id = getattr(stat, "id", None)
    ssrc = getattr(stat, "ssrc", None)
    kind = getattr(stat, "kind", None)
    return f"{direction}:{kind}:{ssrc}:{stat_id}"


def _stream_counters(
    direction: str,
    kind: str,
    stat: Any,
    now: float,
) -> dict[str, float | int | str | None]:
    counters: dict[str, float | int | str | None] = {
        "direction": direction,
        "kind": kind,
        "time": now,
        "packets_received": _int_attr(stat, "packetsReceived"),
        "packets_lost": _int_attr(stat, "packetsLost"),
        "packets_sent": _int_attr(stat, "packetsSent"),
        "bytes_sent": _int_attr(stat, "bytesSent"),
    }
    for attr, key in FRAME_COUNTER_ATTRS.items():
        counters[key] = _int_attr(stat, attr)
    return counters


def _stream_snapshot(
    direction: str,
    kind: str,
    stat: Any,
    current: dict[str, float | int | str | None],
) -> dict[str, Any]:
    packets_received = current["packets_received"]
    packets_lost = current["packets_lost"]
    jitter = _int_attr(stat, "jitter")
    snapshot = {
        "direction": direction,
        "id": getattr(stat, "id", None),
        "ssrc": getattr(stat, "ssrc", None),
        "kind": kind,
        "packets_received": packets_received,
        "packets_lost": packets_lost,
        "packets_sent": current["packets_sent"],
        "bytes_sent": current["bytes_sent"],
        "loss_pct_total": _loss_pct(packets_received, packets_lost),
        "jitter": jitter,
        "jitter_ms": _jitter_ms(kind, jitter),
        "frames_per_second": _float_attr(stat, "framesPerSecond"),
    }
    for key in FRAME_COUNTER_ATTRS.values():
        snapshot[key] = current.get(key)
    round_trip_time = getattr(stat, "roundTripTime", None)
    if round_trip_time is not None:
        snapshot["round_trip_time_ms"] = round(float(round_trip_time) * 1000, 1)
    fraction_lost = getattr(stat, "fractionLost", None)
    if fraction_lost is not None:
        snapshot["fraction_lost_pct"] = round(float(fraction_lost) * 100 / 256, 3)
    return snapshot


def _recent_counters(
    direction: str,
    current: dict[str, float | int | str | None],
    previous: dict[str, float | int | str | None] | None,
    now: float,
) -> dict[str, float | int | None]:
    if previous is None:
        return {}
    elapsed = now - float(previous["time"] or now)
    if elapsed <= 0:
        return {}

    recent: dict[str, float | int | None] = {"elapsed_seconds": round(elapsed, 3)}
    if direction in PACKET_RECEIVED_TYPES:
        received_delta = _delta(current, previous, "packets_received")
        lost_delta = _delta(current, previous, "packets_lost")
        recent.update(
            {
                "packets_received": received_delta,
                "packets_lost": lost_delta,
                "packets_per_second": round(received_delta / elapsed, 1),
                "loss_pct": _loss_pct(received_delta, lost_delta),
            }
        )
    if direction in PACKET_SENT_TYPES:
        sent_delta = _delta(current, previous, "packets_sent")
        bytes_delta = _delta(current, previous, "bytes_sent")
        recent.update(
            {
                "packets_sent": sent_delta,
                "bytes_sent": bytes_delta,
                "packets_per_second": round(sent_delta / elapsed, 1),
                "bitrate_bps": round((bytes_delta * 8) / elapsed, 1),
            }
        )
    for field in FRAME_COUNTER_ATTRS.values():
        frame_delta = _delta(current, previous, field)
        if frame_delta:
            recent[field] = frame_delta
            recent[f"{field}_per_second"] = round(frame_delta / elapsed, 1)
    return recent


def _aggregate_streams(streams: list[dict[str, Any]]) -> dict[str, Any]:
    aggregate: dict[str, Any] = {}
    for stream in streams:
        direction = stream["direction"]
        kind = stream["kind"]
        bucket = aggregate.setdefault(direction, {}).setdefault(
            kind,
            {
                "streams": 0,
                "packets_received": 0,
                "packets_lost": 0,
                "packets_sent": 0,
                "bytes_sent": 0,
                "recent_packets_received": 0,
                "recent_packets_lost": 0,
                "recent_packets_sent": 0,
                "recent_bytes_sent": 0,
                "recent_bitrate_bps": 0.0,
                "_recent_elapsed_seconds": None,
                "max_jitter_ms": None,
                **dict.fromkeys(FRAME_COUNTER_ATTRS.values(), 0),
                **{
                    f"recent_{field}": 0
                    for field in FRAME_COUNTER_ATTRS.values()
                },
            },
        )
        bucket["streams"] += 1
        for field in (
            "packets_received",
            "packets_lost",
            "packets_sent",
            "bytes_sent",
            *FRAME_COUNTER_ATTRS.values(),
        ):
            value = stream.get(field)
            if value is not None:
                bucket[field] += value
        recent = stream.get("recent", {})
        if recent.get("elapsed_seconds"):
            bucket["_recent_elapsed_seconds"] = recent["elapsed_seconds"]
        for field, recent_field in (
            ("packets_received", "recent_packets_received"),
            ("packets_lost", "recent_packets_lost"),
            ("packets_sent", "recent_packets_sent"),
            ("bytes_sent", "recent_bytes_sent"),
            ("frames_sent", "recent_frames_sent"),
            ("frames_encoded", "recent_frames_encoded"),
            ("frames_received", "recent_frames_received"),
            ("frames_decoded", "recent_frames_decoded"),
            ("frames_dropped", "recent_frames_dropped"),
        ):
            value = recent.get(field)
            if value is not None:
                bucket[recent_field] += value
        bitrate_bps = recent.get("bitrate_bps")
        if bitrate_bps is not None:
            bucket["recent_bitrate_bps"] += bitrate_bps
        for field in FRAME_COUNTER_ATTRS.values():
            rate = recent.get(f"{field}_per_second")
            if rate is not None:
                bucket[f"recent_{field}_per_second"] = (
                    bucket.get(f"recent_{field}_per_second", 0.0) + rate
                )
        jitter_ms = stream.get("jitter_ms")
        if jitter_ms is not None:
            bucket["max_jitter_ms"] = (
                jitter_ms
                if bucket["max_jitter_ms"] is None
                else max(bucket["max_jitter_ms"], jitter_ms)
            )

    for by_kind in aggregate.values():
        for bucket in by_kind.values():
            bucket["loss_pct_total"] = _loss_pct(
                bucket["packets_received"],
                bucket["packets_lost"],
            )
            bucket["recent_loss_pct"] = _loss_pct(
                bucket["recent_packets_received"],
                bucket["recent_packets_lost"],
            )
            elapsed = bucket.pop("_recent_elapsed_seconds", None)
            if elapsed and elapsed > 0:
                bucket["recent_packets_per_second"] = round(
                    (
                        bucket["recent_packets_received"]
                        or bucket["recent_packets_sent"]
                    )
                    / elapsed,
                    1,
                )
    return aggregate


def _delta(
    current: dict[str, float | int | str | None],
    previous: dict[str, float | int | str | None],
    field: str,
) -> int:
    current_value = current.get(field)
    previous_value = previous.get(field)
    if current_value is None or previous_value is None:
        return 0
    return max(0, int(current_value) - int(previous_value))


def _int_attr(stat: Any, name: str) -> int | None:
    value = getattr(stat, name, None)
    if value is None:
        return None
    return int(value)


def _float_attr(stat: Any, name: str) -> float | None:
    value = getattr(stat, name, None)
    if value is None:
        return None
    return float(value)


def _loss_pct(received: int | None, lost: int | None) -> float | None:
    if received is None or lost is None:
        return None
    denominator = received + lost
    if denominator <= 0:
        return None
    return round((lost / denominator) * 100, 3)


def _jitter_ms(kind: str, jitter: int | None) -> float | None:
    if jitter is None:
        return None
    if kind == "video":
        return round(jitter / 90, 3)
    if kind == "audio":
        return round(jitter / 48, 3)
    return None
