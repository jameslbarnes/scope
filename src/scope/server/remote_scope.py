"""Direct remote Scope backend.

This backend keeps the local Scope desktop/server as the host for local
devices, native outputs, and the UI, while using another Scope server as the
GPU compute runtime.
"""

from __future__ import annotations

import asyncio
import base64
import copy
import json
import logging
import os
import time
import uuid
from collections import deque
from dataclasses import dataclass
from typing import Any
from urllib.parse import parse_qs, urljoin, urlparse

import aiohttp
import numpy as np
from aiortc import (
    RTCConfiguration,
    RTCIceServer,
    RTCPeerConnection,
    RTCRtpReceiver,
    RTCSessionDescription,
    VideoStreamTrack,
)
from aiortc.mediastreams import VIDEO_CLOCK_RATE, VIDEO_TIME_BASE, MediaStreamError
from av import AudioFrame, VideoFrame

from .cloud_relay import AudioOutputHandler, FrameOutputHandler
from .logs_config import set_connection_id

logger = logging.getLogger(__name__)

REMOTE_SCOPE_URL_ENV = "SCOPE_REMOTE_SCOPE_URL"
REMOTE_SCOPE_API_KEY_ENV = "SCOPE_REMOTE_SCOPE_API_KEY"
REMOTE_SCOPE_REQUEST_TIMEOUT_S = 30.0
REMOTE_SCOPE_VIDEO_NOISE_SCALE_DEFAULT = 0.7
REMOTE_SCOPE_LOCAL_ONLY_NODE_TYPES = {"cue.session"}
REMOTE_SCOPE_LOCAL_ONLY_PLUGIN_KIND = "source"
REMOTE_SCOPE_PREFER_H264 = os.getenv("SCOPE_WEBRTC_PREFER_H264", "0").strip().lower() not in {
    "0",
    "false",
    "no",
    "off",
}


@dataclass(slots=True)
class RemoteScopeGraphInfo:
    source_node_to_track_index: dict[str, int]
    sink_node_ids: list[str]
    record_node_ids: list[str]
    remote_record_node_ids: list[str]
    sink_teed_records: list[tuple[str, int]]
    sink_teed_sinks: list[tuple[int, int]]

    @property
    def source_track_order(self) -> list[str]:
        return list(self.source_node_to_track_index)


@dataclass(slots=True)
class RemoteScopeOutputMapping:
    num_output_tracks: int
    num_local_handlers: int
    remote_to_local: list[int]
    sink_tee_pairs: list[tuple[int, int]]


@dataclass(slots=True)
class RemoteScopeSessionParameters:
    local_params: dict[str, Any]
    runner_params: dict[str, Any]
    graph_info: RemoteScopeGraphInfo


@dataclass(slots=True)
class RemoteScopePipelineLoad:
    pipeline_ids: list[str]
    load_params: Any


class RemoteScopeInputTrack(VideoStreamTrack):
    """Queue-backed video track sent from local Scope to the remote pod."""

    def __init__(self, loop: asyncio.AbstractEventLoop, fps: float = 30.0):
        super().__init__()
        self._loop = loop
        self._queue: asyncio.Queue[VideoFrame] = asyncio.Queue(maxsize=2)
        self._frame_ptime = 1.0 / fps if fps > 0 else 1.0 / 30.0
        self._pts = 0

    def put_frame(self, frame: VideoFrame | np.ndarray) -> bool:
        if self.readyState != "live":
            return False
        if isinstance(frame, np.ndarray):
            frame = VideoFrame.from_ndarray(frame, format="rgb24")

        def enqueue() -> None:
            try:
                self._queue.put_nowait(frame)
            except asyncio.QueueFull:
                try:
                    self._queue.get_nowait()
                    self._queue.put_nowait(frame)
                except asyncio.QueueEmpty:
                    pass

        try:
            self._loop.call_soon_threadsafe(enqueue)
            return True
        except RuntimeError:
            return False

    async def recv(self) -> VideoFrame:
        if self.readyState != "live":
            raise MediaStreamError
        frame = await self._queue.get()
        self._pts += int(self._frame_ptime * VIDEO_CLOCK_RATE)
        frame.pts = self._pts
        frame.time_base = VIDEO_TIME_BASE
        return frame


def _prefer_h264_for_video_transceivers(pc: RTCPeerConnection, label: str) -> None:
    """Prefer H264 over VP8 for remote Scope video relay quality."""
    if not REMOTE_SCOPE_PREFER_H264:
        return
    codecs = RTCRtpReceiver.getCapabilities("video").codecs
    h264_codecs = [
        codec for codec in codecs if codec.mimeType.lower() == "video/h264"
    ]
    if not h264_codecs:
        return
    codec_preferences = h264_codecs + [
        codec for codec in codecs if codec.mimeType.lower() != "video/h264"
    ]

    applied = 0
    for transceiver in pc.getTransceivers():
        if transceiver.kind != "video":
            continue
        try:
            transceiver.setCodecPreferences(codec_preferences)
            applied += 1
        except Exception as exc:
            logger.debug("Could not prefer H264 for %s: %s", label, exc)
    if applied:
        logger.info("Preferred H264 for %s video transceiver(s): %s", applied, label)


class RemoteScopeConnection:
    """Remote backend that talks to a self-hosted Scope server/pod."""

    def __init__(self):
        self._base_url: str | None = None
        self._api_key: str | None = None
        self._connected = False
        self._connecting = False
        self._connect_error: str | None = None
        self._connect_task: asyncio.Task | None = None
        self._pc: RTCPeerConnection | None = None
        self._data_channel = None
        self._loop: asyncio.AbstractEventLoop | None = None
        self._remote_session_id: str | None = None
        self._connection_id: str | None = None
        self._last_close_code: int | None = None
        self._last_close_reason: str | None = None
        self._media_connected = False
        self._media_tasks: list[asyncio.Task] = []
        self._pending_parameters: list[dict[str, Any]] = []
        self._stats = {
            "connected_at": None,
            "api_requests_sent": 0,
            "api_requests_successful": 0,
            "webrtc_offers_sent": 0,
            "webrtc_offers_successful": 0,
            "webrtc_ice_candidates_sent": 0,
            "frames_sent_to_cloud": 0,
            "frames_received_from_cloud": 0,
        }
        self._sent_frame_times: deque[float] = deque(maxlen=180)
        self._received_frame_times: deque[float] = deque(maxlen=180)

        self.input_tracks: list[RemoteScopeInputTrack] = []
        self.output_handlers: list[FrameOutputHandler] = [FrameOutputHandler()]
        self.audio_output_handler = AudioOutputHandler()
        self.source_node_to_track_index: dict[str, int] = {}

    @staticmethod
    def _recent_fps(frame_times: deque[float], window_seconds: float = 5.0) -> float:
        now = time.time()
        while frame_times and now - frame_times[0] > window_seconds:
            frame_times.popleft()
        if len(frame_times) < 2:
            return 0.0
        elapsed = frame_times[-1] - frame_times[0]
        if elapsed <= 0:
            return 0.0
        return (len(frame_times) - 1) / elapsed

    @property
    def is_connected(self) -> bool:
        return self._connected

    @property
    def webrtc_connected(self) -> bool:
        return self._media_connected and self._pc is not None

    async def connect(
        self,
        remote_url: str | None = None,
        api_key: str | None = None,
        user_id: str | None = None,
    ) -> None:
        del user_id
        normalized_url = normalize_remote_scope_url(remote_url)
        if self.is_connected and self._base_url == normalized_url:
            self._connecting = False
            return

        await self.disconnect()
        self._connecting = True
        self._connect_error = None
        self._last_close_code = None
        self._last_close_reason = None
        self._base_url = normalized_url
        self._api_key = api_key or remote_scope_api_key()
        self._connection_id = f"remote-scope-{uuid.uuid4().hex[:12]}"
        set_connection_id(self._connection_id)

        try:
            await self._health_check()
            self._connected = True
            self._stats["connected_at"] = time.time()
            logger.info("Connected to remote Scope server at %s", self._base_url)
        except Exception as e:
            self._connect_error = str(e)
            self._last_close_reason = str(e)
            self._connected = False
            logger.error("Failed to connect to remote Scope server: %s", e)
            raise
        finally:
            self._connecting = False

    async def connect_background(
        self,
        app_id: str | None = None,
        api_key: str | None = None,
        user_id: str | None = None,
        remote_url: str | None = None,
    ) -> None:
        del app_id
        if self._connect_task is not None and not self._connect_task.done():
            self._connect_task.cancel()
            try:
                await self._connect_task
            except (asyncio.CancelledError, Exception):
                pass

        async def do_connect() -> None:
            try:
                await self.connect(
                    remote_url=remote_url,
                    api_key=api_key,
                    user_id=user_id,
                )
            except Exception as e:
                self._connecting = False
                self._connect_error = str(e)
                self._last_close_reason = str(e)
                logger.exception("Remote Scope background connect failed: %s", e)

        self._connecting = True
        self._connect_error = None
        self._connect_task = asyncio.create_task(do_connect())

    async def start_webrtc(self, initial_parameters: dict | None = None) -> None:
        if not self.is_connected:
            raise RuntimeError("Remote Scope backend is not connected")
        if self.webrtc_connected:
            return

        await self.stop_webrtc()
        self._loop = asyncio.get_running_loop()
        session_params = build_remote_scope_session_parameters(initial_parameters)
        params = session_params.runner_params
        graph_info = session_params.graph_info
        produces_video = bool(params.get("produces_video", True))
        mapping = (
            build_remote_scope_output_mapping(graph_info)
            if produces_video
            else RemoteScopeOutputMapping(
                num_output_tracks=0,
                num_local_handlers=0,
                remote_to_local=[],
                sink_tee_pairs=[],
            )
        )
        self.source_node_to_track_index = graph_info.source_node_to_track_index
        self.output_handlers = [
            FrameOutputHandler() for _ in range(max(mapping.num_local_handlers, 1))
        ]
        self.audio_output_handler = AudioOutputHandler()
        bootstrap_parameters = remote_scope_runtime_bootstrap_parameters(params)
        for sink_handler_idx, rec_handler_idx in mapping.sink_tee_pairs:
            if sink_handler_idx < len(self.output_handlers) and rec_handler_idx < len(
                self.output_handlers
            ):
                self.output_handlers[sink_handler_idx].add_callback(
                    self.output_handlers[rec_handler_idx].handle_frame
                )

        ice_servers = await self._ice_servers()
        self._pc = RTCPeerConnection(RTCConfiguration(iceServers=ice_servers))
        self._data_channel = self._pc.createDataChannel("parameters", ordered=True)
        self.input_tracks = []
        video_input_tracks = remote_scope_input_track_count(params, graph_info)
        fps = float(params.get("fps", 30.0) or 30.0)
        for _ in range(video_input_tracks):
            track = RemoteScopeInputTrack(self._loop, fps=fps)
            self.input_tracks.append(track)
            self._pc.addTrack(track)

        for _ in range(mapping.num_output_tracks):
            self._pc.addTransceiver("video", direction="recvonly")
        if params.get("produces_audio"):
            self._pc.addTransceiver("audio", direction="recvonly")

        _prefer_h264_for_video_transceivers(self._pc, "remote Scope")

        video_output_index = 0

        @self._pc.on("track")
        def on_track(track):
            nonlocal video_output_index
            if track.kind == "video":
                local_handler_index = (
                    mapping.remote_to_local[video_output_index]
                    if video_output_index < len(mapping.remote_to_local)
                    else video_output_index
                )
                video_output_index += 1
                self._media_tasks.append(
                    asyncio.create_task(
                        self._receive_video_loop(track, local_handler_index)
                    )
                )
            elif track.kind == "audio":
                self._media_tasks.append(
                    asyncio.create_task(self._receive_audio_loop(track))
                )

        @self._pc.on("connectionstatechange")
        async def on_connectionstatechange():
            state = self._pc.connectionState if self._pc is not None else "closed"
            logger.info("Remote Scope WebRTC connection state: %s", state)
            if state in {"failed", "closed"}:
                self._media_connected = False
                self._last_close_reason = f"WebRTC {state}"

        @self._data_channel.on("open")
        def on_data_channel_open():
            if bootstrap_parameters:
                self._send_parameters_now(bootstrap_parameters)
            for params in self._pending_parameters:
                self._send_parameters_now(params)
            self._pending_parameters.clear()

        @self._data_channel.on("message")
        def on_data_channel_message(message):
            logger.debug("Remote Scope data channel message: %s", message)

        offer = await self._pc.createOffer()
        await self._pc.setLocalDescription(offer)
        await wait_for_ice_gathering_complete(self._pc)
        self._stats["webrtc_offers_sent"] += 1
        response = await self._post_webrtc_offer(params)
        await self._pc.setRemoteDescription(
            RTCSessionDescription(sdp=response["sdp"], type=response["type"])
        )
        self._remote_session_id = response.get("sessionId")
        self._stats["webrtc_offers_successful"] += 1
        self._media_connected = True
        logger.info(
            "Remote Scope WebRTC session started (session=%s, inputs=%s, outputs=%s)",
            self._remote_session_id,
            len(self.input_tracks),
            mapping.num_output_tracks,
        )

    async def stop_webrtc(self) -> None:
        tasks = list(self._media_tasks)
        self._media_tasks.clear()
        for task in tasks:
            task.cancel()
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)
        for track in self.input_tracks:
            track.stop()
        self.input_tracks = []

        if self._pc is not None:
            try:
                await self._pc.close()
            except Exception as e:
                logger.debug("Remote Scope peer close failed: %s", e)
        self._pc = None
        self._data_channel = None
        self._remote_session_id = None
        self._media_connected = False
        self.source_node_to_track_index = {}
        self.output_handlers = [FrameOutputHandler()]
        self.audio_output_handler = AudioOutputHandler()

    async def disconnect(self) -> None:
        if self._connect_task is not None and not self._connect_task.done():
            self._connect_task.cancel()
            try:
                await self._connect_task
            except (asyncio.CancelledError, Exception):
                pass
        self._connect_task = None
        await self.stop_webrtc()
        self._connected = False
        self._connecting = False
        self._base_url = None
        self._api_key = None
        set_connection_id(None)

    def send_frame(self, frame: VideoFrame | np.ndarray) -> bool:
        return self.send_frame_to_track(frame, 0)

    def send_frame_to_track(
        self, frame: VideoFrame | np.ndarray, track_index: int
    ) -> bool:
        if not self.webrtc_connected:
            return False
        if track_index < 0 or track_index >= len(self.input_tracks):
            return False
        sent = self.input_tracks[track_index].put_frame(frame)
        if sent:
            self._stats["frames_sent_to_cloud"] += 1
            self._sent_frame_times.append(time.time())
        return sent

    def get_webrtc_client(self):
        return self

    def get_source_track_index(self, node_id: str) -> int | None:
        return self.source_node_to_track_index.get(node_id)

    def send_parameters(self, params: dict[str, Any]) -> None:
        if not self.is_connected:
            return
        params = filter_remote_scope_parameter_update(params)
        if self._data_channel is None or self._data_channel.readyState != "open":
            self._pending_parameters.append(copy.deepcopy(params))
            return
        if self._loop is None:
            return
        try:
            self._loop.call_soon_threadsafe(self._send_parameters_now, params)
        except RuntimeError:
            logger.debug("Remote Scope event loop closed before parameter send")

    async def api_request(
        self,
        method: str,
        path: str,
        body: dict | None = None,
        timeout: float = REMOTE_SCOPE_REQUEST_TIMEOUT_S,
    ) -> dict[str, Any]:
        if not self._base_url:
            raise RuntimeError("Remote Scope backend is not connected")
        route_path = urlparse(path).path
        if method.upper() == "POST" and route_path == "/api/v1/pipeline/load":
            body = filter_remote_scope_pipeline_load_body(body)
            if body is None:
                self._stats["api_requests_successful"] += 1
                return {
                    "status": 200,
                    "data": {"message": "No remote pipelines to load"},
                }
        if method.upper() == "GET" and route_path == "/api/v1/models/status":
            query = urlparse(path).query
            if is_remote_scope_local_only_models_status_request(query):
                self._stats["api_requests_successful"] += 1
                return {
                    "status": 200,
                    "data": {"downloaded": True, "progress": None},
                }
        url = remote_scope_url_join(self._base_url, path)
        self._stats["api_requests_sent"] += 1
        async with aiohttp.ClientSession(headers=self._headers()) as session:
            if method.upper() == "POST" and route_path == "/api/v1/pipeline/load":
                skipped_response = await self._skip_pipeline_load_if_current(
                    session=session,
                    body=body,
                    timeout=timeout,
                )
                if skipped_response is not None:
                    self._stats["api_requests_successful"] += 1
                    return skipped_response

            response = await self._api_request_once(
                session=session,
                method=method,
                url=url,
                body=body,
                timeout=timeout,
            )
            legacy_body = legacy_pipeline_load_request(body)
            if (
                response.get("status") == 422
                and legacy_body is not None
                and route_path == "/api/v1/pipeline/load"
            ):
                logger.info(
                    "Remote Scope rejected pipeline load shape; retrying legacy pipeline_ids payload"
                )
                self._stats["api_requests_sent"] += 1
                response = await self._api_request_once(
                    session=session,
                    method=method,
                    url=url,
                    body=legacy_body,
                    timeout=timeout,
                )
            if response.get("status", 500) < 400:
                self._stats["api_requests_successful"] += 1
            return response

    def add_frame_callback(self, callback) -> None:
        self.output_handlers[0].add_callback(callback)

    def remove_frame_callback(self, callback) -> None:
        self.output_handlers[0].remove_callback(callback)

    def add_audio_callback(self, callback) -> None:
        self.audio_output_handler.add_callback(callback)

    def remove_audio_callback(self, callback) -> None:
        self.audio_output_handler.remove_callback(callback)

    def get_status(self) -> dict[str, Any]:
        stats = copy.deepcopy(self._stats)
        if stats["connected_at"] is not None:
            stats["uptime_seconds"] = time.time() - stats["connected_at"]
        stats["frames_sent_to_cloud_fps"] = round(
            self._recent_fps(self._sent_frame_times), 1
        )
        stats["frames_received_from_cloud_fps"] = round(
            self._recent_fps(self._received_frame_times), 1
        )
        return {
            "connected": self.is_connected,
            "connecting": self._connecting,
            "error": self._connect_error,
            "connect_stage": "Connecting to remote Scope pod..."
            if self._connecting
            else None,
            "webrtc_connected": self.webrtc_connected,
            "app_id": "remote-scope" if self.is_connected else None,
            "backend": "remote_scope",
            "remote_url": self._base_url,
            "connection_id": self._connection_id,
            "last_close_code": self._last_close_code,
            "last_close_reason": self._last_close_reason,
            "stats": stats if self.is_connected else None,
        }

    async def _health_check(self) -> None:
        if not self._base_url:
            raise RuntimeError("Remote Scope URL is not configured")
        response = await self.api_request("GET", "/health", timeout=10.0)
        if response.get("status", 500) >= 400:
            raise RuntimeError(f"remote health check failed: {response}")

    async def _ice_servers(self) -> list[RTCIceServer]:
        try:
            response = await self.api_request(
                "GET", "/api/v1/webrtc/ice-servers", timeout=10.0
            )
            data = response.get("data", {})
            ice_servers = []
            for server in data.get("iceServers", []):
                urls = server.get("urls")
                if not urls:
                    continue
                ice_servers.append(
                    RTCIceServer(
                        urls=urls,
                        username=server.get("username"),
                        credential=server.get("credential"),
                    )
                )
            if ice_servers:
                return ice_servers
        except Exception as e:
            logger.warning("Remote ICE server fetch failed, using STUN fallback: %s", e)
        return [RTCIceServer(urls=["stun:stun.l.google.com:19302"])]

    async def _post_webrtc_offer(self, initial_parameters: dict[str, Any]) -> dict:
        if self._pc is None or self._pc.localDescription is None:
            raise RuntimeError("Cannot post WebRTC offer before local description")
        response = await self.api_request(
            "POST",
            "/api/v1/webrtc/offer",
            body={
                "sdp": self._pc.localDescription.sdp,
                "type": self._pc.localDescription.type,
                "initialParameters": initial_parameters,
                "connection_id": self._connection_id,
            },
            timeout=30.0,
        )
        status = response.get("status", 500)
        if status >= 400:
            raise RuntimeError(f"remote WebRTC offer failed: {response}")
        data = response.get("data", {})
        if not data.get("sdp") or not data.get("type"):
            raise RuntimeError(f"remote WebRTC answer missing SDP/type: {data}")
        return data

    async def _receive_video_loop(self, track, local_handler_index: int) -> None:
        try:
            while True:
                frame = await track.recv()
                self._stats["frames_received_from_cloud"] += 1
                self._received_frame_times.append(time.time())
                if 0 <= local_handler_index < len(self.output_handlers):
                    self.output_handlers[local_handler_index].handle_frame(frame)
        except asyncio.CancelledError:
            pass
        except Exception as e:
            logger.warning("Remote Scope video receive loop ended: %s", e)

    async def _receive_audio_loop(self, track) -> None:
        try:
            while True:
                frame = await track.recv()
                if isinstance(frame, AudioFrame):
                    self.audio_output_handler.handle_frame(frame)
        except asyncio.CancelledError:
            pass
        except Exception as e:
            logger.warning("Remote Scope audio receive loop ended: %s", e)

    def _send_parameters_now(self, params: dict[str, Any]) -> None:
        if self._data_channel is None or self._data_channel.readyState != "open":
            return
        self._data_channel.send(json.dumps(params))

    def _headers(self) -> dict[str, str]:
        headers = {"User-Agent": "Scope RemoteScopeConnection"}
        if self._api_key:
            headers["Authorization"] = f"Bearer {self._api_key}"
            headers["X-Scope-Key"] = self._api_key
        return headers

    async def _api_request_once(
        self,
        session: aiohttp.ClientSession,
        method: str,
        url: str,
        body: dict | None,
        timeout: float,
    ) -> dict[str, Any]:
        async with session.request(
            method.upper(),
            url,
            json=body,
            timeout=aiohttp.ClientTimeout(total=timeout),
        ) as response:
            payload = await response_payload(response)
            return {
                "status": response.status,
                **payload,
            }

    async def _skip_pipeline_load_if_current(
        self,
        session: aiohttp.ClientSession,
        body: dict | None,
        timeout: float,
    ) -> dict[str, Any] | None:
        if not self._base_url:
            return None
        requested = pipeline_load_request_identity(body)
        if requested is None:
            return None

        try:
            self._stats["api_requests_sent"] += 1
            response = await self._api_request_once(
                session=session,
                method="GET",
                url=remote_scope_url_join(self._base_url, "/api/v1/pipeline/status"),
                body=None,
                timeout=min(timeout, 10.0),
            )
        except Exception as e:
            logger.debug("Remote Scope status check before pipeline load failed: %s", e)
            return None

        if response.get("status", 500) >= 400:
            return None

        self._stats["api_requests_successful"] += 1
        if not remote_pipeline_status_matches_load(response.get("data"), requested):
            return None

        logger.info(
            "Remote Scope pipeline already loaded (%s); skipping pipeline/load",
            ", ".join(requested.pipeline_ids),
        )
        return {
            "status": 200,
            "data": {"message": "Pipeline already loaded on remote Scope"},
        }


def normalize_remote_scope_url(value: str | None) -> str:
    raw = (value or remote_scope_url_from_env() or "").strip()
    if not raw:
        raise ValueError(
            f"Remote Scope URL is required. Pass remote_url or set {REMOTE_SCOPE_URL_ENV}."
        )
    if "://" not in raw:
        raw = f"https://{raw}"
    parsed = urlparse(raw)
    if parsed.scheme not in {"http", "https"} or not parsed.netloc:
        raise ValueError("Remote Scope URL must be an http(s) URL")
    return raw.rstrip("/")


def remote_scope_url_from_env() -> str | None:
    import os

    return os.getenv(REMOTE_SCOPE_URL_ENV)


def remote_scope_api_key() -> str | None:
    import os

    return os.getenv(REMOTE_SCOPE_API_KEY_ENV)


def remote_scope_url_join(base_url: str, path: str) -> str:
    if path.startswith("http://") or path.startswith("https://"):
        return path
    return urljoin(f"{base_url.rstrip('/')}/", path.lstrip("/"))


def _remote_scope_source_plugin_names() -> set[str]:
    try:
        from scope.core.plugins import get_plugin_manager

        plugin_manager = get_plugin_manager()
        return {
            plugin.get("name")
            for plugin in plugin_manager.list_plugins_sync(skip_update_check=True)
            if plugin.get("kind") == REMOTE_SCOPE_LOCAL_ONLY_PLUGIN_KIND
        }
    except Exception as e:
        logger.debug("Failed to list local source plugins for remote filtering: %s", e)
        return set()


def is_remote_scope_local_only_node_type(node_type_id: Any) -> bool:
    if not isinstance(node_type_id, str):
        return False
    if node_type_id in REMOTE_SCOPE_LOCAL_ONLY_NODE_TYPES:
        return True

    try:
        from scope.core.plugins import get_plugin_manager

        plugin_manager = get_plugin_manager()
        plugin_name = plugin_manager.get_plugin_for_type_id(node_type_id)
        if plugin_name is None:
            import importlib

            importlib.import_module("scope.core.pipelines.registry")
            plugin_name = plugin_manager.get_plugin_for_type_id(node_type_id)
    except Exception as e:
        logger.debug(
            "Failed to resolve local plugin for remote-filtered type %s: %s",
            node_type_id,
            e,
        )
        return False

    return bool(
        plugin_name and plugin_name in _remote_scope_source_plugin_names()
    )


def is_remote_scope_local_only_models_status_request(query: str) -> bool:
    pipeline_ids = parse_qs(query).get("pipeline_id", [])
    return any(is_remote_scope_local_only_node_type(value) for value in pipeline_ids)


def filter_remote_scope_pipeline_load_body(body: dict | None) -> dict | None:
    """Remove local-only node loads before forwarding to a remote Scope pod.

    Scope desktop can host local control nodes such as Cue while remote Scope
    owns GPU pipelines. The remote pod should never be asked to load local-only
    nodes because it may not have the plugin installed and may misinterpret the
    mixed payload as the active LongLive load state.
    """
    if not isinstance(body, dict):
        return body

    pipelines = body.get("pipelines")
    if isinstance(pipelines, list):
        remote_pipelines = [
            copy.deepcopy(item)
            for item in pipelines
            if not (
                isinstance(item, dict)
                and is_remote_scope_local_only_node_type(item.get("pipeline_id"))
            )
        ]
        if len(remote_pipelines) == len(pipelines):
            return body
        if not remote_pipelines:
            return None
        filtered = copy.deepcopy(body)
        filtered["pipelines"] = remote_pipelines
        return filtered

    pipeline_ids = body.get("pipeline_ids")
    if isinstance(pipeline_ids, list):
        remote_pipeline_ids = [
            pipeline_id
            for pipeline_id in pipeline_ids
            if not is_remote_scope_local_only_node_type(pipeline_id)
        ]
        if len(remote_pipeline_ids) == len(pipeline_ids):
            return body
        if not remote_pipeline_ids:
            return None
        filtered = copy.deepcopy(body)
        filtered["pipeline_ids"] = remote_pipeline_ids
        return filtered

    return body


def legacy_pipeline_load_request(body: dict | None) -> dict[str, Any] | None:
    """Translate simple new-style pipeline loads for older remote Scope pods.

    Current local Scope sends ``pipelines`` so graph nodes can carry per-node
    load parameters. Older deployed Scope images only accept ``pipeline_ids`` +
    one shared ``load_params`` object. We can safely fall back when every
    requested pipeline has identical load parameters.
    """
    if not isinstance(body, dict) or body.get("pipeline_ids") is not None:
        return None
    pipelines = body.get("pipelines")
    if not isinstance(pipelines, list) or not pipelines:
        return None

    pipeline_ids: list[str] = []
    load_params: list[Any] = []
    for item in pipelines:
        if not isinstance(item, dict) or not isinstance(item.get("pipeline_id"), str):
            return None
        pipeline_ids.append(item["pipeline_id"])
        load_params.append(item.get("load_params"))

    first_load_params = load_params[0] if load_params else None
    if any(params != first_load_params for params in load_params[1:]):
        return None

    legacy = {
        key: copy.deepcopy(value) for key, value in body.items() if key != "pipelines"
    }
    legacy["pipeline_ids"] = pipeline_ids
    legacy["load_params"] = copy.deepcopy(first_load_params)
    return legacy


def pipeline_load_request_identity(
    body: dict | None,
) -> RemoteScopePipelineLoad | None:
    """Return comparable pipeline-load intent for current or legacy request bodies."""
    if not isinstance(body, dict):
        return None

    pipeline_ids = body.get("pipeline_ids")
    if isinstance(pipeline_ids, list) and pipeline_ids:
        if not all(isinstance(pipeline_id, str) for pipeline_id in pipeline_ids):
            return None
        return RemoteScopePipelineLoad(
            pipeline_ids=list(pipeline_ids),
            load_params=copy.deepcopy(body.get("load_params")),
        )

    legacy = legacy_pipeline_load_request(body)
    if legacy is None:
        return None
    return pipeline_load_request_identity(legacy)


PIPELINE_LOAD_PARAM_KEYS = {
    "height",
    "width",
    "quantization",
    "vace_enabled",
    "vae_type",
    "loras",
    "lora_merge_mode",
}


def remote_pipeline_status_matches_load(
    status: Any,
    requested: RemoteScopePipelineLoad,
) -> bool:
    """Return true when a remote status response already satisfies a load request."""
    if not isinstance(status, dict):
        return False
    if status.get("status") != "loaded":
        return False
    if len(requested.pipeline_ids) != 1:
        return False
    if status.get("pipeline_id") != requested.pipeline_ids[0]:
        return False

    requested_params = requested.load_params
    if not requested_params:
        return True
    current_params = status.get("load_params")
    if not isinstance(requested_params, dict) or not isinstance(current_params, dict):
        return False

    for key in PIPELINE_LOAD_PARAM_KEYS:
        if key not in requested_params:
            continue
        if key not in current_params:
            return False
        if normalize_load_param_value(
            requested_params[key]
        ) != normalize_load_param_value(current_params[key]):
            return False
    return True


def normalize_load_param_value(value: Any) -> Any:
    if hasattr(value, "value"):
        value = value.value
    if isinstance(value, str):
        return value.lower()
    if isinstance(value, list | tuple):
        return [normalize_load_param_value(item) for item in value]
    if isinstance(value, dict):
        return {key: normalize_load_param_value(value[key]) for key in sorted(value)}
    return value


def filter_remote_scope_initial_parameters(
    initial_parameters: dict[str, Any] | None,
) -> dict[str, Any]:
    params = copy.deepcopy(initial_parameters or {})
    graph = params.get("graph")
    if not isinstance(graph, dict):
        return params

    nodes = graph.get("nodes")
    if not isinstance(nodes, list):
        return params

    remote_nodes: list[Any] = []
    removed_node_ids: set[str] = set()
    for node in nodes:
        local_only_type_id = remote_scope_local_only_graph_node_type_id(node)
        if is_remote_scope_local_only_node_type(local_only_type_id):
            node_id = node.get("id")
            if isinstance(node_id, str):
                removed_node_ids.add(node_id)
            continue
        remote_nodes.append(node)

    if not removed_node_ids:
        return params

    graph["nodes"] = remote_nodes
    edges = graph.get("edges")
    if isinstance(edges, list):
        graph["edges"] = [
            edge
            for edge in edges
            if isinstance(edge, dict)
            and not remote_scope_edge_references_any_node(edge, removed_node_ids)
        ]

    ui_state = graph.get("ui_state")
    if isinstance(ui_state, dict):
        node_params = ui_state.get("node_params")
        if isinstance(node_params, dict):
            for node_id in removed_node_ids:
                node_params.pop(node_id, None)

    return params


def remote_scope_local_only_graph_node_type_id(node: Any) -> Any:
    if not isinstance(node, dict):
        return None
    node_type = node.get("type")
    if node_type == "node":
        return node.get("node_type_id")
    if node_type == "pipeline":
        return node.get("pipeline_id")
    return None


def ensure_remote_scope_graph_output_edges(params: dict[str, Any]) -> dict[str, Any]:
    """Add an obvious pipeline-to-sink edge for simple generated graphs.

    The desktop can produce a one-pipeline graph with a sink node but no stream
    edge when running in perform mode with plugin control nodes. Remote Scope
    accepts the WebRTC session in that shape, but the sink has no upstream video
    and the local preview stays black.
    """
    graph = params.get("graph")
    if not isinstance(graph, dict):
        return params

    nodes = graph.get("nodes")
    edges = graph.get("edges")
    if not isinstance(nodes, list):
        return params
    if not isinstance(edges, list):
        edges = []
        graph["edges"] = edges

    pipeline_nodes = [
        node
        for node in nodes
        if isinstance(node, dict)
        and node.get("type") == "pipeline"
        and isinstance(node.get("id"), str)
    ]
    sink_nodes = [
        node
        for node in nodes
        if isinstance(node, dict)
        and node.get("type") == "sink"
        and isinstance(node.get("id"), str)
    ]
    if len(pipeline_nodes) != 1 or not sink_nodes:
        return params

    pipeline_id = pipeline_nodes[0]["id"]
    sink_ids = {sink["id"] for sink in sink_nodes}

    for sink in sink_nodes:
        sink_id = sink["id"]
        has_inbound_stream = any(
            isinstance(edge, dict)
            and edge.get("kind") == "stream"
            and remote_scope_edge_to_id(edge) == sink_id
            and remote_scope_edge_from_id(edge) not in sink_ids
            for edge in edges
        )
        if has_inbound_stream:
            continue
        append_remote_scope_stream_edge_once(
            edges,
            from_id=pipeline_id,
            from_port="video",
            to_id=sink_id,
            to_port="video",
        )

    rewrite_remote_scope_sink_tee_edges(edges, sink_ids)

    return params


def remote_scope_edge_from_id(edge: dict[str, Any]) -> Any:
    return edge.get("from_node", edge.get("from"))


def remote_scope_edge_to_id(edge: dict[str, Any]) -> Any:
    return edge.get("to_node", edge.get("to"))


def append_remote_scope_stream_edge_once(
    edges: list[Any],
    *,
    from_id: str,
    from_port: str,
    to_id: str,
    to_port: str,
) -> None:
    exists = any(
        isinstance(edge, dict)
        and edge.get("kind") == "stream"
        and remote_scope_edge_from_id(edge) == from_id
        and remote_scope_edge_to_id(edge) == to_id
        and edge.get("from_port", "video") == from_port
        and edge.get("to_port", "video") == to_port
        for edge in edges
    )
    if exists:
        return
    edges.append(
        {
            "from": from_id,
            "from_port": from_port,
            "to_node": to_id,
            "to_port": to_port,
            "kind": "stream",
        }
    )


def rewrite_remote_scope_sink_tee_edges(
    edges: list[Any],
    sink_ids: set[str],
) -> None:
    """Materialize sink-to-sink UI tees as direct remote video edges.

    Local UI graphs can express "send the preview output to this native sink"
    as output-sink -> native-sink. The remote runner cannot execute a sink as a
    video producer, so keep the native sink wired to the same upstream producer
    and drop the tee edge before forwarding the graph.
    """
    rewritten_edges: list[Any] = []
    tee_edges: list[dict[str, Any]] = []
    for edge in edges:
        if (
            isinstance(edge, dict)
            and edge.get("kind") == "stream"
            and remote_scope_edge_from_id(edge) in sink_ids
            and remote_scope_edge_to_id(edge) in sink_ids
        ):
            tee_edges.append(edge)
            continue
        rewritten_edges.append(edge)

    if not tee_edges:
        return

    for tee_edge in tee_edges:
        from_sink_id = remote_scope_edge_from_id(tee_edge)
        to_sink_id = remote_scope_edge_to_id(tee_edge)
        resolved = False
        for upstream_edge in rewritten_edges:
            if (
                not isinstance(upstream_edge, dict)
                or upstream_edge.get("kind") != "stream"
                or remote_scope_edge_to_id(upstream_edge) != from_sink_id
            ):
                continue
            upstream_from = remote_scope_edge_from_id(upstream_edge)
            if not isinstance(upstream_from, str) or upstream_from in sink_ids:
                continue
            append_remote_scope_stream_edge_once(
                rewritten_edges,
                from_id=upstream_from,
                from_port=str(upstream_edge.get("from_port", "video")),
                to_id=str(to_sink_id),
                to_port=str(tee_edge.get("to_port", "video")),
            )
            resolved = True

        if not resolved:
            rewritten_edges.append(tee_edge)

    edges[:] = rewritten_edges


def remote_scope_edge_references_any_node(
    edge: dict[str, Any],
    node_ids: set[str],
) -> bool:
    return any(
        edge.get(key) in node_ids
        for key in ("from", "from_node", "to", "to_node")
    )


def parse_remote_scope_graph(params: dict[str, Any] | None) -> RemoteScopeGraphInfo:
    graph = (params or {}).get("graph")
    source_node_to_track_index: dict[str, int] = {}
    sink_node_ids: list[str] = []
    record_node_ids: list[str] = []
    remote_record_node_ids: list[str] = []
    sink_teed_records: list[tuple[str, int]] = []
    sink_teed_sinks: list[tuple[int, int]] = []

    if isinstance(graph, dict):
        node_by_id: dict[str, dict[str, Any]] = {}
        for node in graph.get("nodes", []):
            if not isinstance(node, dict):
                continue
            node_id = node.get("id")
            if not isinstance(node_id, str):
                continue
            node_by_id[node_id] = node
            node_type = node.get("type")
            if node_type == "source":
                source_node_to_track_index[node_id] = len(source_node_to_track_index)
            elif node_type == "sink":
                sink_node_ids.append(node_id)
            elif node_type == "record":
                record_node_ids.append(node_id)

        sink_upstreams: dict[int, tuple[str, str]] = {}
        upstream_to_primary_sink: dict[tuple[str, str], int] = {}
        for edge in graph.get("edges", []):
            if not isinstance(edge, dict) or edge.get("kind") != "stream":
                continue
            to_id = remote_scope_edge_to_id(edge)
            if to_id not in sink_node_ids:
                continue
            from_id = remote_scope_edge_from_id(edge)
            if not isinstance(from_id, str) or from_id in sink_node_ids:
                continue
            sink_idx = sink_node_ids.index(to_id)
            if sink_idx in sink_upstreams:
                continue
            upstream = (from_id, str(edge.get("from_port", "video")))
            sink_upstreams[sink_idx] = upstream
            primary_idx = upstream_to_primary_sink.setdefault(upstream, sink_idx)
            if primary_idx != sink_idx:
                sink_teed_sinks.append((primary_idx, sink_idx))

        for record_id in record_node_ids:
            inbound_from = None
            for edge in graph.get("edges", []):
                if not isinstance(edge, dict):
                    continue
                if edge.get("to_node") != record_id or edge.get("kind") != "stream":
                    continue
                from_id = edge.get("from")
                if isinstance(from_id, str):
                    inbound_from = from_id
                    break
            src_node = node_by_id.get(inbound_from) if inbound_from else None
            if (
                src_node is not None
                and src_node.get("type") == "sink"
                and inbound_from in sink_node_ids
            ):
                sink_teed_records.append((record_id, sink_node_ids.index(inbound_from)))
            else:
                remote_record_node_ids.append(record_id)

    return RemoteScopeGraphInfo(
        source_node_to_track_index=source_node_to_track_index,
        sink_node_ids=sink_node_ids,
        record_node_ids=record_node_ids,
        remote_record_node_ids=remote_record_node_ids,
        sink_teed_records=sink_teed_records,
        sink_teed_sinks=sink_teed_sinks,
    )


def build_remote_scope_output_mapping(
    graph_info: RemoteScopeGraphInfo,
) -> RemoteScopeOutputMapping:
    num_sink_slots = max(len(graph_info.sink_node_ids), 1)
    primary_sink_indices = list(range(num_sink_slots))
    for primary_idx, sink_idx in graph_info.sink_teed_sinks:
        if sink_idx < len(primary_sink_indices):
            primary_sink_indices[sink_idx] = primary_idx
    remote_sink_indices = list(dict.fromkeys(primary_sink_indices))
    num_output_tracks = len(remote_sink_indices) + len(graph_info.remote_record_node_ids)
    num_local_handlers = num_sink_slots + len(graph_info.record_node_ids)
    remote_to_local = remote_sink_indices.copy()
    for record_id in graph_info.remote_record_node_ids:
        rec_pos = graph_info.record_node_ids.index(record_id)
        remote_to_local.append(num_sink_slots + rec_pos)
    sink_tee_pairs = graph_info.sink_teed_sinks.copy()
    for record_id, sink_idx in graph_info.sink_teed_records:
        rec_pos = graph_info.record_node_ids.index(record_id)
        sink_tee_pairs.append((sink_idx, num_sink_slots + rec_pos))
    return RemoteScopeOutputMapping(
        num_output_tracks=num_output_tracks,
        num_local_handlers=num_local_handlers,
        remote_to_local=remote_to_local,
        sink_tee_pairs=sink_tee_pairs,
    )


def build_remote_scope_initial_parameters(
    initial_parameters: dict[str, Any] | None,
) -> dict[str, Any]:
    return build_remote_scope_session_parameters(initial_parameters).runner_params


def build_remote_scope_session_parameters(
    initial_parameters: dict[str, Any] | None,
) -> RemoteScopeSessionParameters:
    params = filter_remote_scope_initial_parameters(initial_parameters)
    params = dedupe_remote_scope_graph_edges(params)
    params = ensure_remote_scope_graph_output_edges(params)
    graph_info = parse_remote_scope_graph(params)
    if graph_info.source_track_order:
        params["source_track_order"] = graph_info.source_track_order
    runner_params = rewrite_remote_scope_graph_for_runner(copy.deepcopy(params), graph_info)
    return RemoteScopeSessionParameters(
        local_params=params,
        runner_params=runner_params,
        graph_info=graph_info,
    )


def filter_remote_scope_parameter_update(
    parameters: dict[str, Any] | None,
) -> dict[str, Any]:
    """Normalize runtime updates before forwarding them to the remote pod."""
    if not isinstance(parameters, dict):
        return {}

    params = copy.deepcopy(parameters)
    if not isinstance(params.get("graph"), dict):
        return params

    return build_remote_scope_session_parameters(params).runner_params


def dedupe_remote_scope_graph_edges(params: dict[str, Any]) -> dict[str, Any]:
    graph = params.get("graph")
    if not isinstance(graph, dict):
        return params
    edges = graph.get("edges")
    if not isinstance(edges, list):
        return params

    deduped_edges: list[Any] = []
    seen_edges: set[tuple[Any, Any, Any, Any, Any]] = set()
    for edge in edges:
        if not isinstance(edge, dict):
            deduped_edges.append(edge)
            continue
        key = (
            remote_scope_edge_from_id(edge),
            edge.get("from_port", "video"),
            remote_scope_edge_to_id(edge),
            edge.get("to_port", "video"),
            edge.get("kind"),
        )
        if key in seen_edges:
            continue
        seen_edges.add(key)
        deduped_edges.append(edge)
    graph["edges"] = deduped_edges
    return params


REMOTE_SCOPE_RUNTIME_BOOTSTRAP_KEYS = {
    "input_mode",
    "prompts",
    "prompt_interpolation_method",
    "transition",
    "noise_scale",
    "noise_controller",
    "denoising_step_list",
    "manage_cache",
    "kv_cache_attention_bias",
    "lora_scales",
    "vace_enabled",
    "vace_ref_images",
    "vace_use_input_video",
    "vace_context_scale",
    "first_frame_image",
    "last_frame_image",
    "images",
    "paused",
}


def remote_scope_runtime_bootstrap_parameters(
    params: dict[str, Any] | None,
) -> dict[str, Any]:
    """Build a flat runtime update for older remote Scope shared processors.

    Some deployed Scope pods run browser WebRTC sessions through the active RTMP
    FrameProcessor. In that mode the pod accepts ``initialParameters`` but the
    shared processor keeps its existing runtime state until a data-channel update
    arrives. Send a structural-key-free update on channel open so video-mode
    defaults such as ``noise_scale`` are present before LongLive processes frames.
    """
    if not isinstance(params, dict):
        return {}

    bootstrap = {
        key: copy.deepcopy(params[key])
        for key in REMOTE_SCOPE_RUNTIME_BOOTSTRAP_KEYS
        if key in params and params[key] is not None
    }

    if bootstrap.get("input_mode") == "video":
        bootstrap.setdefault("noise_scale", REMOTE_SCOPE_VIDEO_NOISE_SCALE_DEFAULT)
        bootstrap.setdefault("noise_controller", True)
        bootstrap["reset_cache"] = True

    return bootstrap


def rewrite_remote_scope_graph_for_runner(
    params: dict[str, Any],
    graph_info: RemoteScopeGraphInfo,
) -> dict[str, Any]:
    """Adapt local graph outputs to the remote Scope WebRTC channel contract.

    Scope's relay path treats graph outputs as ordered ``sinks + recorders``:
    sink-attached recorders are mirrored locally, while pipeline-attached
    recorders need a dedicated remote video output. A vanilla Scope WebRTC
    server only sends sink nodes as live tracks, so pipeline-attached record
    nodes are rewritten as sink nodes for the runner request.
    """
    graph = params.get("graph")
    if not isinstance(graph, dict):
        return params
    collapsed_records = {record_id for record_id, _ in graph_info.sink_teed_records}
    remote_records = set(graph_info.remote_record_node_ids)
    collapsed_sinks = {
        graph_info.sink_node_ids[sink_idx]
        for _, sink_idx in graph_info.sink_teed_sinks
        if sink_idx < len(graph_info.sink_node_ids)
    }
    if not collapsed_records and not remote_records and not collapsed_sinks:
        return params

    nodes = graph.get("nodes")
    if isinstance(nodes, list):
        rewritten_nodes = []
        for node in nodes:
            if not isinstance(node, dict):
                continue
            node_id = node.get("id")
            if node_id in collapsed_sinks:
                continue
            if node_id in collapsed_records:
                continue
            if node_id in remote_records:
                rewritten = copy.deepcopy(node)
                rewritten["type"] = "sink"
                rewritten.pop("sink_mode", None)
                rewritten.pop("sink_name", None)
                rewritten_nodes.append(rewritten)
            else:
                rewritten_nodes.append(node)
        graph["nodes"] = rewritten_nodes

    edges = graph.get("edges")
    collapsed_node_ids = collapsed_records | collapsed_sinks
    if isinstance(edges, list) and collapsed_node_ids:
        graph["edges"] = [
            edge
            for edge in edges
            if isinstance(edge, dict)
            and remote_scope_edge_to_id(edge) not in collapsed_node_ids
            and remote_scope_edge_from_id(edge) not in collapsed_node_ids
        ]
    return params


def remote_scope_input_track_count(
    params: dict[str, Any],
    graph_info: RemoteScopeGraphInfo,
) -> int:
    if params.get("input_mode") == "text":
        return 0
    if graph_info.source_node_to_track_index:
        return len(graph_info.source_node_to_track_index)
    if params.get("input_mode") == "video":
        return 1
    return 0


async def response_payload(response: aiohttp.ClientResponse) -> dict[str, Any]:
    content_type = response.headers.get("content-type", "")
    if "application/json" in content_type:
        return {"data": await response.json()}
    raw = await response.read()
    if content_type.startswith("video/") or content_type in {
        "application/octet-stream",
        "application/zip",
    }:
        return {
            "_base64_content": base64.b64encode(raw).decode("ascii"),
            "media_type": content_type or "application/octet-stream",
        }
    text = raw.decode("utf-8", errors="replace")
    try:
        return {"data": json.loads(text)}
    except Exception:
        return {"data": {"text": text}}


async def wait_for_ice_gathering_complete(
    pc: RTCPeerConnection,
    timeout: float = 5.0,
) -> None:
    if pc.iceGatheringState == "complete":
        return
    loop = asyncio.get_running_loop()
    done = loop.create_future()

    @pc.on("icegatheringstatechange")
    def on_icegatheringstatechange():
        if pc.iceGatheringState == "complete" and not done.done():
            done.set_result(None)

    try:
        await asyncio.wait_for(done, timeout=timeout)
    except TimeoutError:
        logger.debug("Timed out waiting for ICE gathering; continuing with current SDP")
