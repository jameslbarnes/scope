"""VP8 recovery tuning for Scope WebRTC video streams."""

from __future__ import annotations

import logging
import os
from collections.abc import Callable
from typing import Any

from aiortc.codecs import vpx

logger = logging.getLogger(__name__)

VP8_PACKET_MAX_ENV = "SCOPE_WEBRTC_VP8_PACKET_MAX"
VP8_KEYFRAME_INTERVAL_FRAMES_ENV = "SCOPE_WEBRTC_VP8_KEYFRAME_INTERVAL_FRAMES"
DEFAULT_VP8_PACKET_MAX = 1100
DEFAULT_VP8_KEYFRAME_INTERVAL_FRAMES = 30

_ENCODE_PATCH_ATTR = "_scope_vp8_recovery_wrapped"
_FRAMES_SINCE_KEYFRAME_ATTR = "_scope_vp8_frames_since_keyframe"


def env_int(name: str, default: int) -> int:
    value = os.getenv(name)
    if value is None:
        return default
    try:
        return int(value)
    except ValueError:
        logger.warning("Ignoring invalid integer for %s=%r", name, value)
        return default


def should_force_periodic_vp8_keyframe(
    encoder: Any,
    interval_frames: int,
    requested_keyframe: bool,
) -> bool:
    """Return whether this VP8 frame should be forced to a keyframe."""
    if interval_frames <= 0:
        return requested_keyframe

    frames_since_keyframe = getattr(
        encoder,
        _FRAMES_SINCE_KEYFRAME_ATTR,
        interval_frames,
    )
    force_keyframe = requested_keyframe or frames_since_keyframe >= interval_frames
    setattr(
        encoder,
        _FRAMES_SINCE_KEYFRAME_ATTR,
        1 if force_keyframe else frames_since_keyframe + 1,
    )
    return force_keyframe


def configure_vp8_recovery() -> None:
    """Tune aiortc VP8 packetization and keyframe cadence for lossy relay links."""
    packet_max = env_int(VP8_PACKET_MAX_ENV, DEFAULT_VP8_PACKET_MAX)
    if packet_max > 0:
        vpx.PACKET_MAX = packet_max

    interval_frames = env_int(
        VP8_KEYFRAME_INTERVAL_FRAMES_ENV,
        DEFAULT_VP8_KEYFRAME_INTERVAL_FRAMES,
    )
    original_encode: Callable = vpx.Vp8Encoder.encode
    if getattr(original_encode, _ENCODE_PATCH_ATTR, False):
        logger.info(
            "VP8 recovery already configured: packet_max=%s keyframe_interval=%s",
            vpx.PACKET_MAX,
            interval_frames,
        )
        return

    def encode_with_periodic_keyframes(self, frame, force_keyframe: bool = False):
        return original_encode(
            self,
            frame,
            should_force_periodic_vp8_keyframe(
                self,
                interval_frames,
                force_keyframe,
            ),
        )

    setattr(encode_with_periodic_keyframes, _ENCODE_PATCH_ATTR, True)
    vpx.Vp8Encoder.encode = encode_with_periodic_keyframes
    logger.info(
        "Configured VP8 recovery: packet_max=%s keyframe_interval_frames=%s",
        vpx.PACKET_MAX,
        interval_frames,
    )
