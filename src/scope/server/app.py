import asyncio
import contextlib
import io
import json
import logging
import os
import queue
import subprocess
import sys
import threading
import time
import warnings
import webbrowser
from contextlib import asynccontextmanager
from datetime import datetime
from functools import wraps
from importlib.metadata import version
from logging.handlers import RotatingFileHandler
from pathlib import Path
from typing import TYPE_CHECKING, Any
from urllib.parse import quote, urlparse

import click
import httpx
import uvicorn
from fastapi import (
    BackgroundTasks,
    Depends,
    FastAPI,
    HTTPException,
    Query,
    Request,
    WebSocket,
    WebSocketDisconnect,
)
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, Response, StreamingResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, Field

if TYPE_CHECKING:
    from .livepeer import LivepeerConnection
    from .pipeline_manager import PipelineManager
    from .schema import PluginInfo
    from .webrtc import WebRTCManager

from scope.core.config import get_base_dir
from scope.core.lora.manifest import (
    LoRAManifestEntry,
    LoRAProvenance,
    add_manifest_entry,
    compute_sha256,
    load_manifest,
    save_manifest,
)
from scope.core.workflows.resolve import (
    WorkflowRequest,
    WorkflowResolutionPlan,
    resolve_workflow,
)

from .cloud_proxy import (
    cloud_proxy,
    get_hardware_info_from_cloud,
    proxy_with_body,
    upload_asset_to_cloud,
)
from .download_models import download_models
from .download_progress_manager import download_progress_manager
from .file_utils import (
    AUDIO_EXTENSIONS,
    IMAGE_EXTENSIONS,
    LORA_EXTENSIONS,
    VIDEO_EXTENSIONS,
    iter_files,
)
from .kafka_publisher import (
    KafkaPublisher,
    is_kafka_enabled,
    set_kafka_publisher,
)
from .logs_config import (
    LOG_FORMAT,
    ScopeLogContextFilter,
    cleanup_old_logs,
    ensure_logs_dir,
    get_current_log_file,
    get_logs_dir,
    get_most_recent_log_file,
)
from .lora_downloader import LoRADownloadRequest, LoRADownloadResult
from .mcp_router import router as mcp_router
from .models_config import (
    ensure_models_dir,
    get_assets_dir,
    get_lora_dir,
    get_shared_lora_dir,
    models_are_downloaded,
)
from .pipeline_manager import PipelineManager
from .recording import (
    RecordingManager,
    cleanup_recording_files,
    cleanup_temp_file,
)
from .schema import (
    ApiKeyDeleteResponse,
    ApiKeyInfo,
    ApiKeySetRequest,
    ApiKeySetResponse,
    ApiKeysListResponse,
    AssetFileInfo,
    AssetsResponse,
    CloudConnectRequest,
    CloudStatusResponse,
    HardwareInfoResponse,
    HealthResponse,
    IceCandidateRequest,
    IceServerConfig,
    IceServersResponse,
    IntegrationAvailability,
    NodeDefinitionsResponse,
    PipelineLoadRequest,
    PipelineSchemasResponse,
    PipelineStatusResponse,
    WebRTCOfferRequest,
    WebRTCOfferResponse,
)
from .scope_cloud_types import ScopeCloudBackend
from .tempo_router import router as tempo_router

# Cached responses for pipeline schemas, node definitions, and plugin list.
# Invalidated by _invalidate_plugin_caches() on plugin install/uninstall and
# on cloud connect/disconnect — the latter matters because cloud mode proxies
# the schema/definition endpoints to the cloud backend, so the cached payload
# depends on cloud-mode state and must be rebuilt when it flips.
_pipeline_schemas_cache: PipelineSchemasResponse | None = None
_node_definitions_cache: NodeDefinitionsResponse | None = None
_plugins_list_cache: object | None = None
_local_only_pipeline_load_active = False
_local_only_pipeline_load_node_ids: set[str] = set()


def _invalidate_plugin_caches():
    """Reset plugin, pipeline schema, and node definition caches.

    Called on plugin install/uninstall and on cloud connect/disconnect so
    that the next fetch rebuilds the payload from whichever registry is
    authoritative for the current mode.
    """
    global _pipeline_schemas_cache, _node_definitions_cache, _plugins_list_cache
    _pipeline_schemas_cache = None
    _node_definitions_cache = None
    _plugins_list_cache = None

    # Also clear the plugin manager's per-plugin update check TTL cache
    try:
        from scope.core.plugins import get_plugin_manager

        get_plugin_manager().clear_update_check_cache()
    except Exception:
        pass


def _is_remote_scope_local_only_type(type_id: Any) -> bool:
    from .remote_scope import is_remote_scope_local_only_node_type

    return is_remote_scope_local_only_node_type(type_id)


def _pipeline_load_tuples_from_request(
    request: PipelineLoadRequest,
) -> list[tuple[str, str, dict[str, Any] | None]]:
    if request.pipelines:
        return [
            (pipeline.node_id, pipeline.pipeline_id, pipeline.load_params)
            for pipeline in request.pipelines
        ]
    if request.pipeline_ids:
        return [
            (pipeline_id, pipeline_id, request.load_params)
            for pipeline_id in request.pipeline_ids
        ]
    raise HTTPException(
        status_code=400,
        detail="Either 'pipelines' or 'pipeline_ids' must be provided",
    )


def _split_local_only_pipeline_tuples(
    pipelines: list[tuple[str, str, dict[str, Any] | None]],
) -> tuple[
    list[tuple[str, str, dict[str, Any] | None]],
    list[tuple[str, str, dict[str, Any] | None]],
]:
    local: list[tuple[str, str, dict[str, Any] | None]] = []
    remote: list[tuple[str, str, dict[str, Any] | None]] = []
    for item in pipelines:
        _node_id, pipeline_id, _load_params = item
        if _is_remote_scope_local_only_type(pipeline_id):
            local.append(item)
        else:
            remote.append(item)
    return local, remote


def _pipeline_load_request_body(request: PipelineLoadRequest) -> dict[str, Any]:
    return request.model_dump(exclude_none=True)


def _plain_dict(value: Any) -> dict[str, Any] | None:
    if isinstance(value, dict):
        return value
    if hasattr(value, "model_dump"):
        return value.model_dump(exclude_none=True)
    return None


def _initial_parameters_require_remote_scope(initial_parameters: Any) -> bool:
    """Return true when a WebRTC run contains at least one remote pipeline."""
    params = _plain_dict(initial_parameters)
    if params is None:
        return True

    pipeline_ids = params.get("pipeline_ids")
    if isinstance(pipeline_ids, list) and pipeline_ids:
        return any(
            not _is_remote_scope_local_only_type(pipeline_id)
            for pipeline_id in pipeline_ids
        )

    graph = _plain_dict(params.get("graph"))
    if graph is None:
        return True

    nodes = graph.get("nodes")
    if not isinstance(nodes, list):
        return True

    for node in nodes:
        node_data = _plain_dict(node)
        if not isinstance(node_data, dict) or node_data.get("type") != "pipeline":
            continue
        if not _is_remote_scope_local_only_type(node_data.get("pipeline_id")):
            return True

    return False


def _local_only_pipeline_status_response(
    status_info: dict[str, Any],
) -> PipelineStatusResponse:
    global _local_only_pipeline_load_active, _local_only_pipeline_load_node_ids

    expected_pipeline_id = next(iter(_local_only_pipeline_load_node_ids), None)
    if status_info.get("status") == "not_loaded" or (
        status_info.get("status") == "loaded"
        and expected_pipeline_id is not None
        and status_info.get("pipeline_id") not in _local_only_pipeline_load_node_ids
    ):
        status_info = {
            **status_info,
            "status": "loading",
            "pipeline_id": expected_pipeline_id,
            "loading_stage": status_info.get("loading_stage")
            or "Starting local pipeline...",
        }
    elif status_info.get("status") == "error":
        _local_only_pipeline_load_active = False
        _local_only_pipeline_load_node_ids = set()

    return PipelineStatusResponse(**status_info)


class STUNErrorFilter(logging.Filter):
    """Filter to suppress STUN/TURN connection errors that are not critical."""

    def filter(self, record):
        # Suppress STUN  exeception that occurrs always during the stream restart
        if "Task exception was never retrieved" in record.getMessage():
            return False
        return True


def _configure_logging():
    """Set up file and console logging for the main server process.

    Called from run_server() rather than at module import time so that the MCP
    subprocess (which also imports this module) doesn't create a competing log
    file that shadows the real server logs.
    """
    # Ensure logs directory exists and clean up old logs
    ensure_logs_dir()
    cleanup_old_logs(max_age_days=1)
    log_file = get_current_log_file()

    # Set root to WARNING to keep non-app libraries quiet by default
    logging.basicConfig(level=logging.WARNING, format=LOG_FORMAT)

    # Install filter on every handler so %(connection_id)s is always populated
    _fal_filter = ScopeLogContextFilter()
    root_logger = logging.getLogger()
    for handler in root_logger.handlers:
        handler.addFilter(_fal_filter)
        if isinstance(handler, logging.StreamHandler) and not isinstance(
            handler, RotatingFileHandler
        ):
            handler.setLevel(logging.INFO)

    # Add rotating file handler
    file_handler = RotatingFileHandler(
        log_file,
        maxBytes=5 * 1024 * 1024,  # 5 MB per file
        backupCount=5,  # Keep 5 backup files
    )
    file_handler.setLevel(logging.INFO)
    file_handler.setFormatter(logging.Formatter(LOG_FORMAT))
    file_handler.addFilter(_fal_filter)
    root_logger.addHandler(file_handler)

    # Add the filter to suppress STUN/TURN errors
    stun_filter = STUNErrorFilter()
    logging.getLogger("asyncio").addFilter(stun_filter)

    # Set INFO level for app modules
    logging.getLogger("scope.server").setLevel(logging.INFO)
    logging.getLogger("scope.core").setLevel(logging.INFO)

    # Set INFO level for uvicorn
    logging.getLogger("uvicorn.error").setLevel(logging.INFO)

    # Enable verbose logging for other libraries when needed
    if os.getenv("VERBOSE_LOGGING"):
        logging.getLogger("uvicorn.access").setLevel(logging.INFO)
        logging.getLogger("fastapi").setLevel(logging.INFO)
    logging.getLogger("aiortc").setLevel(logging.INFO)

    if os.getenv("LIVEPEER_DEBUG"):
        logging.getLogger("livepeer_gateway").setLevel(logging.DEBUG)
        logging.getLogger("scope.server.livepeer").setLevel(logging.DEBUG)
        logging.getLogger("scope.server.livepeer_client").setLevel(logging.DEBUG)
        for handler in logging.getLogger().handlers:
            if isinstance(handler, logging.StreamHandler) and not isinstance(
                handler, RotatingFileHandler
            ):
                handler.setLevel(logging.DEBUG)


# Set INFO for the cloud log re-emitter so cloud lines reach console and file
logging.getLogger("scope.cloud").setLevel(logging.INFO)

# Allow suppressing noisy loggers via env var (comma-separated logger names)
# e.g. SCOPE_LOG_QUIET_LOGGERS=scope.server.frame_processor,scope.core.pipelines.longlive
_quiet_loggers = os.getenv("SCOPE_LOG_QUIET_LOGGERS", "")
for _logger_name in _quiet_loggers.split(","):
    _logger_name = _logger_name.strip()
    if _logger_name:
        logging.getLogger(_logger_name).setLevel(logging.WARNING)

# Select pipeline depending on the "PIPELINE" environment variable
PIPELINE = os.getenv("PIPELINE", None)

logger = logging.getLogger(__name__)


def suppress_init_output(func):
    """Decorator to suppress all initialization output (logging, warnings, stdout/stderr)."""

    @wraps(func)
    def wrapper(*args, **kwargs):
        with (
            contextlib.redirect_stdout(io.StringIO()),
            contextlib.redirect_stderr(io.StringIO()),
            warnings.catch_warnings(),
        ):
            warnings.simplefilter("ignore")
            # Temporarily disable all logging
            logging.disable(logging.CRITICAL)
            try:
                return func(*args, **kwargs)
            finally:
                # Re-enable logging
                logging.disable(logging.NOTSET)

    return wrapper


def get_git_commit_hash() -> str:
    """
    Get the current git commit hash.

    Returns:
        Git commit hash if available, otherwise a fallback message.
    """
    # Packaged distributions (e.g. the Electron app) don't ship the .git
    # directory, so the build pipeline writes the short hash into a sibling
    # file that we read here.
    commit_file = Path(__file__).parent.parent / "_git_commit.txt"
    try:
        if commit_file.is_file():
            baked = commit_file.read_text().strip()
            if baked:
                return baked
    except OSError:
        pass

    try:
        result = subprocess.run(
            ["git", "rev-parse", "--short", "HEAD"],
            capture_output=True,
            text=True,
            timeout=5,  # 5 second timeout
            cwd=Path(__file__).parent,  # Run in the project directory
        )
        if result.returncode == 0:
            return result.stdout.strip()
        else:
            return "unknown (not a git repository)"
    except (subprocess.TimeoutExpired, subprocess.CalledProcessError):
        return "unknown (git error)"
    except FileNotFoundError:
        return "unknown (git not installed)"
    except Exception:
        return "unknown"


def print_version_info():
    """Print version information and exit."""
    try:
        pkg_version = version("daydream-scope")
    except Exception:
        pkg_version = "unknown"

    git_hash = get_git_commit_hash()

    print(f"daydream-scope: {pkg_version}")
    print(f"git commit: {git_hash}")


def configure_static_files():
    """Configure static file serving for production."""
    frontend_dist = Path(__file__).parent.parent.parent.parent / "frontend" / "dist"
    if frontend_dist.exists():
        app.mount(
            "/assets", StaticFiles(directory=frontend_dist / "assets"), name="assets"
        )
        logger.info(f"Serving static assets from {frontend_dist / 'assets'}")
    else:
        logger.info("Frontend dist directory not found - running in development mode")


# Global WebRTC manager instance
webrtc_manager = None
# Global pipeline manager instance
pipeline_manager = None
# Server startup timestamp for detecting restarts
server_start_time = time.time()
# Global Livepeer manager instance
livepeer = None
# Global self-hosted remote Scope manager instance
remote_scope = None
# Currently selected remote backend
scope_cloud = None
# Global Kafka publisher instance (optional, initialized if credentials are present)
kafka_publisher = None
# Global tempo sync manager instance
tempo_sync = None
# Global OSC server instance
osc_server = None
# Global DMX server instance
dmx_server = None


async def prewarm_pipeline(pipeline_id: str):
    """Background task to pre-warm the pipeline without blocking startup."""
    try:
        await asyncio.wait_for(
            pipeline_manager.load_pipelines([(pipeline_id, pipeline_id, None)]),
            timeout=300,  # 5 minute timeout for pipeline loading
        )
    except Exception as e:
        logger.error(f"Error pre-warming pipeline {pipeline_id} in background: {e}")


async def _prewarm_plugin_update_cache():
    """Background task to warm the plugin update check cache at startup."""
    try:
        from scope.core.plugins import get_plugin_manager

        pm = get_plugin_manager()
        loop = asyncio.get_event_loop()
        await loop.run_in_executor(None, pm.list_plugins_sync)
        logger.info("Plugin update check cache warmed")
    except Exception as e:
        logger.debug(f"Plugin update cache warm-up skipped: {e}")


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Lifespan handler for startup and shutdown events."""
    # Lazy imports to avoid loading torch at CLI startup (fixes Windows DLL locking)
    import torch

    from .livepeer import LivepeerConnection
    from .pipeline_manager import PipelineManager
    from .remote_scope import RemoteScopeConnection
    from .tempo_sync import TempoSync
    from .webrtc import WebRTCManager

    # Startup
    global \
        webrtc_manager, \
        pipeline_manager, \
        kafka_publisher, \
        livepeer, \
        remote_scope, \
        scope_cloud, \
        tempo_sync, \
        osc_server, \
        dmx_server

    # Check CUDA availability and warn if not available
    if not torch.cuda.is_available():
        warning_msg = (
            "CUDA is not available on this system. "
            "Some pipelines may not work without a CUDA-compatible GPU. "
            "The application will start, but pipeline functionality may be limited."
        )
        logger.warning(warning_msg)

    # Clean up recording files from previous sessions (in case of crashes)
    cleanup_recording_files()

    # Log logs directory
    logs_dir = get_logs_dir()
    logger.info(f"Logs directory: {logs_dir}")

    # Ensure models directory and subdirectories exist
    models_dir = ensure_models_dir()
    logger.info(f"Models directory: {models_dir}")

    # Ensure assets directory exists for VACE reference images and other media (at same level as models)
    assets_dir = get_assets_dir()
    assets_dir.mkdir(parents=True, exist_ok=True)
    logger.info(f"Assets directory: {assets_dir}")

    # Initialize pipeline manager (but don't load pipeline yet)
    pipeline_manager = PipelineManager()
    logger.info("Pipeline manager initialized")

    # Pre-warm the default pipeline
    if PIPELINE is not None:
        asyncio.create_task(prewarm_pipeline(PIPELINE))

    # Pre-warm the plugin update check cache in the background so the first
    # "Nodes" / resolve-workflow call doesn't block on PyPI lookups.
    asyncio.create_task(_prewarm_plugin_update_cache())

    webrtc_manager = WebRTCManager()
    logger.info("WebRTC manager initialized")

    tempo_sync = TempoSync()
    logger.info("Tempo sync manager initialized")

    livepeer = LivepeerConnection()
    livepeer.configure()
    logger.info("Livepeer configured")

    remote_scope = RemoteScopeConnection()
    scope_cloud = livepeer
    logger.info("Remote Scope backend configured")

    # Initialize Kafka publisher if credentials are configured
    if is_kafka_enabled():
        kafka_publisher = KafkaPublisher()
        if await kafka_publisher.start():
            set_kafka_publisher(kafka_publisher)
            logger.info("Kafka publisher initialized")
        else:
            kafka_publisher = None
            logger.warning("Kafka publisher failed to start")

    # Start OSC UDP server on the same port as the HTTP API
    from .osc_server import OSCServer

    osc_host = os.getenv("SCOPE_HOST", "0.0.0.0")
    osc_port = int(os.getenv("SCOPE_PORT", "8000"))
    osc_server = OSCServer(osc_host, osc_port)
    osc_server.set_managers(pipeline_manager, webrtc_manager)
    await osc_server.start()

    # Start DMX Art-Net server (loads config from disk for port + mappings)
    from .dmx_config import load_config as load_dmx_config
    from .dmx_config import mappings_to_dict
    from .dmx_server import DMXServer

    dmx_cfg = load_dmx_config()
    dmx_host = os.getenv("SCOPE_HOST", "0.0.0.0")
    dmx_server = DMXServer(dmx_host, dmx_cfg.get("preferred_port", 6454))
    dmx_server.set_managers(pipeline_manager, webrtc_manager)
    dmx_server.log_all_messages = dmx_cfg.get("log_all_messages", False)
    dmx_server.set_mappings(mappings_to_dict(dmx_cfg.get("mappings", [])))
    dmx_server.enabled = dmx_cfg.get("enabled", False)
    if dmx_server.enabled:
        await dmx_server.start()

    # Syphon server discovery (macOS only): create the ObjC singleton and do
    # an initial NSRunLoop pump so servers are available when the UI first loads.
    # Subsequent refreshes pump on demand in the list_input_sources endpoint.
    if sys.platform == "darwin":
        try:
            from .syphon.receiver import (
                drain_notifications,
                ensure_directory_initialized,
            )

            ensure_directory_initialized()
            drain_notifications(0.1)
            logger.info("Syphon directory initialized")
        except Exception:
            logger.debug("Syphon not available, skipping directory init")

    yield

    # Shutdown
    if dmx_server:
        logger.info("Shutting down DMX server...")
        await dmx_server.stop()
        logger.info("DMX server shutdown complete")

    if osc_server:
        logger.info("Shutting down OSC server...")
        await osc_server.stop()
        logger.info("OSC server shutdown complete")

    if webrtc_manager:
        logger.info("Shutting down WebRTC manager...")
        await webrtc_manager.stop()
        logger.info("WebRTC manager shutdown complete")

    if pipeline_manager:
        logger.info("Shutting down pipeline manager...")
        pipeline_manager.unload_all_pipelines()
        logger.info("Pipeline manager shutdown complete")

    if tempo_sync:
        logger.info("Shutting down tempo sync...")
        await tempo_sync.stop()
        logger.info("Tempo sync shutdown complete")

    if livepeer and livepeer.is_connected:
        logger.info("Shutting down Livepeer connection...")
        await livepeer.disconnect()
        logger.info("Livepeer connection shutdown complete")

    if remote_scope and remote_scope.is_connected:
        logger.info("Shutting down Remote Scope connection...")
        await remote_scope.disconnect()
        logger.info("Remote Scope connection shutdown complete")

    if kafka_publisher:
        logger.info("Shutting down Kafka publisher...")
        await kafka_publisher.stop()
        set_kafka_publisher(None)
        logger.info("Kafka publisher shutdown complete")


def get_webrtc_manager() -> "WebRTCManager":
    """Dependency to get WebRTC manager instance."""
    return webrtc_manager


def get_pipeline_manager() -> "PipelineManager":
    """Dependency to get pipeline manager instance."""
    return pipeline_manager


def get_osc_server():
    """Dependency to get OSC server instance."""

    return osc_server


def get_livepeer() -> "LivepeerConnection":
    """Dependency to get Livepeer manager instance."""
    return livepeer


def get_scope_cloud() -> ScopeCloudBackend:
    """Dependency to get the selected remote backend.

    Returns the backend object regardless of connection state so callers can
    inspect status, including connecting/error states.  Callers that need an
    *active* connection must check cloud_manager.is_connected themselves.
    """
    return scope_cloud or livepeer


def get_dmx_server():
    """Dependency to get DMX server instance."""

    return dmx_server


app = FastAPI(
    lifespan=lifespan,
    title="Scope",
    description="A tool for running and customizing real-time, interactive generative AI pipelines and models",
    version=version("daydream-scope"),
)

# MCP server endpoints (headless sessions, parameters, frame capture, etc.)
app.include_router(mcp_router)
# Tempo sync endpoints (enable/disable, status, sources, BPM control)
app.include_router(tempo_router)

# Add CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

CUE_DEFAULT_BASE_URL = "http://127.0.0.1:8792"


class CueObservationProxyRequest(BaseModel):
    base_url: str = Field(default=CUE_DEFAULT_BASE_URL)
    timeout_ms: int = Field(default=1000, ge=50, le=30000)
    observation: dict[str, Any] | None = None
    observations: list[dict[str, Any]] | None = None


class CueSessionProxyRequest(BaseModel):
    base_url: str = Field(default=CUE_DEFAULT_BASE_URL)
    timeout_ms: int = Field(default=1000, ge=50, le=30000)


SHADERCLAW_DEFAULT_SHADER = "Gradient"
SHADERCLAW_DEFAULT_WIDTH = 640
SHADERCLAW_DEFAULT_HEIGHT = 360


class ShaderClawBaseRequest(BaseModel):
    shaders_dir: str | None = Field(default=None)
    width: int = Field(default=SHADERCLAW_DEFAULT_WIDTH, ge=1, le=4096)
    height: int = Field(default=SHADERCLAW_DEFAULT_HEIGHT, ge=1, le=4096)


class ShaderClawLoadRequest(ShaderClawBaseRequest):
    shader: str = Field(default=SHADERCLAW_DEFAULT_SHADER)
    parameters: dict[str, Any] | None = None


class ShaderClawParameterRequest(ShaderClawBaseRequest):
    shader: str = Field(default=SHADERCLAW_DEFAULT_SHADER)
    parameters: dict[str, Any] | None = None
    name: str
    value: Any


def _normalize_cue_base_url(base_url: str) -> str:
    parsed = urlparse(base_url)
    if parsed.scheme not in {"http", "https"} or not parsed.netloc:
        raise HTTPException(status_code=400, detail="Cue base_url must be http(s).")
    return base_url.rstrip("/")


def _cue_session_url(base_url: str, session_id: str, route: str) -> str:
    base = _normalize_cue_base_url(base_url)
    return f"{base}/sessions/{quote(session_id, safe='')}/{route}"


def _cue_session_ws_url(base_url: str, session_id: str, route: str) -> str:
    url = _cue_session_url(base_url, session_id, route)
    if url.startswith("https://"):
        return "wss://" + url.removeprefix("https://")
    return "ws://" + url.removeprefix("http://")


def _cue_timeout(timeout_ms: int) -> httpx.Timeout:
    seconds = max(timeout_ms, 50) / 1000
    return httpx.Timeout(seconds, connect=min(seconds, 5.0))


async def _raise_cue_proxy_error(exc: Exception) -> None:
    if isinstance(exc, httpx.HTTPStatusError):
        detail = exc.response.text or exc.response.reason_phrase
        raise HTTPException(status_code=exc.response.status_code, detail=detail) from exc
    if isinstance(exc, httpx.RequestError):
        raise HTTPException(status_code=502, detail=f"Cue request failed: {exc}") from exc
    raise exc


@app.get("/api/v1/cue/sessions/{session_id}/state")
async def cue_session_state(
    session_id: str,
    base_url: str = Query(default=CUE_DEFAULT_BASE_URL),
    timeout_ms: int = Query(default=1000, ge=50, le=30000),
):
    """Proxy Cue session state so the desktop UI can avoid browser CORS issues."""
    try:
        async with httpx.AsyncClient(timeout=_cue_timeout(timeout_ms)) as client:
            response = await client.get(_cue_session_url(base_url, session_id, "state"))
            response.raise_for_status()
            return response.json()
    except Exception as exc:
        await _raise_cue_proxy_error(exc)


@app.get("/api/v1/cue/sessions/{session_id}/agent")
async def cue_session_agent(
    session_id: str,
    base_url: str = Query(default=CUE_DEFAULT_BASE_URL),
    timeout_ms: int = Query(default=1000, ge=50, le=30000),
):
    """Proxy Cue's machine-readable session manifest for node UI discovery."""
    try:
        async with httpx.AsyncClient(timeout=_cue_timeout(timeout_ms)) as client:
            response = await client.get(_cue_session_url(base_url, session_id, "agent"))
            response.raise_for_status()
            return response.json()
    except Exception as exc:
        await _raise_cue_proxy_error(exc)


@app.post("/api/v1/cue/sessions/{session_id}/observations")
async def cue_session_observations(
    session_id: str,
    request: CueObservationProxyRequest,
):
    """Proxy manual chat and mapped graph observations into Cue."""
    payload: dict[str, Any]
    if request.observations is not None:
        payload = {"observations": request.observations}
    elif request.observation is not None:
        payload = request.observation
    else:
        raise HTTPException(
            status_code=400,
            detail="Provide observation or observations.",
        )
    try:
        async with httpx.AsyncClient(timeout=_cue_timeout(request.timeout_ms)) as client:
            response = await client.post(
                _cue_session_url(request.base_url, session_id, "observations"),
                json=payload,
            )
            response.raise_for_status()
            return response.json()
    except Exception as exc:
        await _raise_cue_proxy_error(exc)


@app.post("/api/v1/cue/sessions/{session_id}/reset")
async def cue_session_reset(
    session_id: str,
    request: CueSessionProxyRequest,
):
    """Proxy Cue session reset so the node can match Etherea's reset control."""
    try:
        async with httpx.AsyncClient(timeout=_cue_timeout(request.timeout_ms)) as client:
            response = await client.post(
                _cue_session_url(request.base_url, session_id, "reset"),
            )
            response.raise_for_status()
            return response.json()
    except Exception as exc:
        await _raise_cue_proxy_error(exc)


@app.websocket("/api/v1/cue/sessions/{session_id}/events")
async def cue_session_events(
    websocket: WebSocket,
    session_id: str,
    base_url: str = Query(default=CUE_DEFAULT_BASE_URL),
    timeout_ms: int = Query(default=1000, ge=50, le=30000),
):
    """Proxy Cue's passive event websocket into the desktop UI origin."""
    await _proxy_cue_websocket(websocket, session_id, "events", base_url, timeout_ms)


@app.websocket("/api/v1/cue/sessions/{session_id}/transcription")
async def cue_session_transcription(
    websocket: WebSocket,
    session_id: str,
    base_url: str = Query(default=CUE_DEFAULT_BASE_URL),
    timeout_ms: int = Query(default=1000, ge=50, le=30000),
):
    """Proxy microphone/audio frames into Cue's transcription websocket."""
    await _proxy_cue_websocket(
        websocket,
        session_id,
        "transcription",
        base_url,
        timeout_ms,
    )


@app.websocket("/api/v1/cue/sessions/{session_id}/vlm")
async def cue_session_vlm(
    websocket: WebSocket,
    session_id: str,
    base_url: str = Query(default=CUE_DEFAULT_BASE_URL),
    timeout_ms: int = Query(default=1000, ge=50, le=30000),
):
    """Proxy sampled frames into Cue's VLM websocket."""
    await _proxy_cue_websocket(websocket, session_id, "vlm", base_url, timeout_ms)


async def _proxy_cue_websocket(
    websocket: WebSocket,
    session_id: str,
    route: str,
    base_url: str,
    timeout_ms: int,
):
    await websocket.accept()
    try:
        import aiohttp

        async with aiohttp.ClientSession() as session:
            async with session.ws_connect(
                _cue_session_ws_url(base_url, session_id, route),
                timeout=max(timeout_ms, 50) / 1000,
            ) as cue_ws:
                async def cue_to_client() -> None:
                    async for message in cue_ws:
                        if message.type == aiohttp.WSMsgType.TEXT:
                            await websocket.send_text(message.data)
                        elif message.type == aiohttp.WSMsgType.BINARY:
                            await websocket.send_bytes(message.data)
                        elif message.type in {
                            aiohttp.WSMsgType.CLOSE,
                            aiohttp.WSMsgType.CLOSED,
                            aiohttp.WSMsgType.ERROR,
                        }:
                            break

                async def client_to_cue() -> None:
                    try:
                        while True:
                            message = await websocket.receive()
                            if message["type"] == "websocket.disconnect":
                                break
                            text = message.get("text")
                            data = message.get("bytes")
                            if text is not None:
                                await cue_ws.send_str(text)
                            elif data is not None:
                                await cue_ws.send_bytes(data)
                    except WebSocketDisconnect:
                        pass

                done, pending = await asyncio.wait(
                    {
                        asyncio.create_task(cue_to_client()),
                        asyncio.create_task(client_to_cue()),
                    },
                    return_when=asyncio.FIRST_COMPLETED,
                )
                for task in pending:
                    task.cancel()
                for task in done:
                    task.result()
    except Exception as exc:
        with contextlib.suppress(Exception):
            await websocket.send_json(
                {"type": "error", "error": f"Cue {route} failed: {exc}"}
            )
    finally:
        with contextlib.suppress(Exception):
            await websocket.close()


_shaderclaw_preview_local = threading.local()


def _shaderclaw_library(shaders_dir: str | None = None):
    try:
        from scope_shaderclaw_plugin.native_renderer import ShaderLibrary
    except Exception as exc:
        raise HTTPException(
            status_code=404,
            detail="scope-shaderclaw plugin is not installed in this Scope server",
        ) from exc

    return ShaderLibrary(shaders_dir)


def _shaderclaw_preview_renderer(
    *,
    shaders_dir: str | None,
    width: int,
    height: int,
):
    try:
        from scope_shaderclaw_plugin.native_renderer import NativeShaderClawRenderer
    except Exception as exc:
        raise HTTPException(
            status_code=404,
            detail="scope-shaderclaw plugin is not installed in this Scope server",
        ) from exc

    key = (shaders_dir or "", width, height)
    renderer = getattr(_shaderclaw_preview_local, "renderer", None)
    renderer_key = getattr(_shaderclaw_preview_local, "renderer_key", None)
    if renderer is None or renderer_key != key:
        renderer = NativeShaderClawRenderer(
            shaders_dir=shaders_dir,
            width=width,
            height=height,
        )
        _shaderclaw_preview_local.renderer = renderer
        _shaderclaw_preview_local.renderer_key = key
    return renderer


def _shaderclaw_http_error(exc: Exception) -> HTTPException:
    status_code = (
        502
        if exc.__class__.__name__ in {"ShaderClawError", "ShaderClawNativeError"}
        else 500
    )
    return HTTPException(status_code=status_code, detail=str(exc))


def _shaderclaw_parameters_from_json(value: str) -> dict[str, Any]:
    if not value.strip():
        return {}
    try:
        parsed = json.loads(value)
    except json.JSONDecodeError as exc:
        raise HTTPException(
            status_code=400,
            detail=f"Invalid ShaderClaw parameters_json: {exc.msg}",
        ) from exc
    if not isinstance(parsed, dict):
        raise HTTPException(
            status_code=400,
            detail="ShaderClaw parameters_json must be a JSON object.",
        )
    return parsed


def _shaderclaw_mjpeg_frame(jpeg: bytes) -> bytes:
    return (
        b"--frame\r\n"
        b"Content-Type: image/jpeg\r\n"
        b"Content-Length: "
        + str(len(jpeg)).encode()
        + b"\r\n"
        b"\r\n"
        + jpeg
        + b"\r\n"
    )


@app.get("/api/v1/shaderclaw/manifest")
async def shaderclaw_manifest(
    shaders_dir: str | None = Query(default=None),
):
    """Return ShaderClaw's local shader manifest through the Scope origin."""

    def run():
        library = _shaderclaw_library(shaders_dir)
        return {"shaders": library.manifest(), "shaders_dir": str(library.shaders_dir)}

    try:
        return await asyncio.to_thread(run)
    except HTTPException:
        raise
    except Exception as exc:
        raise _shaderclaw_http_error(exc) from exc


@app.post("/api/v1/shaderclaw/load")
async def shaderclaw_load(request: ShaderClawLoadRequest):
    """Load ShaderClaw metadata and return its current parameter metadata."""

    def run():
        library = _shaderclaw_library(request.shaders_dir)
        entry = library.resolve_shader(request.shader)
        return {
            "entry": {"id": entry.id, "title": entry.title, "file": entry.file},
            "inputs": library.inputs(entry.file, request.parameters or {}),
            "shaders_dir": str(library.shaders_dir),
        }

    try:
        return await asyncio.to_thread(run)
    except HTTPException:
        raise
    except Exception as exc:
        raise _shaderclaw_http_error(exc) from exc


@app.post("/api/v1/shaderclaw/parameter")
async def shaderclaw_parameter(request: ShaderClawParameterRequest):
    """Return parameter metadata after changing one native ShaderClaw value."""

    def run():
        parameters = dict(request.parameters or {})
        parameters[request.name] = request.value
        library = _shaderclaw_library(request.shaders_dir)
        entry = library.resolve_shader(request.shader)
        return {
            "inputs": library.inputs(entry.file, parameters),
            "shaders_dir": str(library.shaders_dir),
        }

    try:
        return await asyncio.to_thread(run)
    except HTTPException:
        raise
    except Exception as exc:
        raise _shaderclaw_http_error(exc) from exc


@app.get("/api/v1/shaderclaw/stream")
async def shaderclaw_stream(
    shaders_dir: str | None = Query(default=None),
    shader: str = Query(default=SHADERCLAW_DEFAULT_SHADER),
    parameters_json: str = Query(default="{}"),
    width: int = Query(default=SHADERCLAW_DEFAULT_WIDTH, ge=1, le=4096),
    height: int = Query(default=SHADERCLAW_DEFAULT_HEIGHT, ge=1, le=4096),
    fps: int = Query(default=30, ge=1, le=60),
    quality: int = Query(default=82, ge=30, le=95),
):
    """Stream a native ShaderClaw preview as MJPEG without React frame polling."""

    parameters = _shaderclaw_parameters_from_json(parameters_json)

    try:
        # Validate paths and shader selection before the streaming response starts.
        _shaderclaw_library(shaders_dir).resolve_shader(shader)
    except HTTPException:
        raise
    except Exception as exc:
        raise _shaderclaw_http_error(exc) from exc

    frame_queue: queue.Queue[bytes | Exception | None] = queue.Queue(maxsize=2)
    stop_event = threading.Event()

    def publish(item: bytes | Exception | None) -> None:
        try:
            frame_queue.put_nowait(item)
            return
        except queue.Full:
            pass
        with contextlib.suppress(queue.Empty):
            frame_queue.get_nowait()
        with contextlib.suppress(queue.Full):
            frame_queue.put_nowait(item)

    def produce_frames() -> None:
        interval = 1.0 / fps
        try:
            renderer = _shaderclaw_preview_renderer(
                shaders_dir=shaders_dir,
                width=width,
                height=height,
            )
            while not stop_event.is_set():
                started_at = time.monotonic()
                jpeg = renderer.render_jpeg(shader, parameters, quality=quality)
                publish(_shaderclaw_mjpeg_frame(jpeg))
                elapsed = time.monotonic() - started_at
                if stop_event.wait(max(0.0, interval - elapsed)):
                    break
        except Exception as exc:
            publish(exc)
        finally:
            publish(None)

    producer = threading.Thread(
        target=produce_frames,
        name=f"shaderclaw-preview-{shader}",
        daemon=True,
    )

    async def stream_frames():
        producer.start()
        try:
            while True:
                item = await asyncio.to_thread(frame_queue.get)
                if item is None:
                    break
                if isinstance(item, Exception):
                    logger.warning("ShaderClaw preview stream failed: %s", item)
                    break
                yield item
        except asyncio.CancelledError:
            pass
        finally:
            stop_event.set()
            await asyncio.to_thread(producer.join, 1.0)

    return StreamingResponse(
        stream_frames(),
        media_type="multipart/x-mixed-replace; boundary=frame",
        headers={
            "Cache-Control": "no-store, no-cache, must-revalidate",
            "Pragma": "no-cache",
            "X-Accel-Buffering": "no",
        },
    )


@app.websocket("/api/v1/shaderclaw/ws")
async def shaderclaw_ws_preview(
    websocket: WebSocket,
    shaders_dir: str | None = Query(default=None),
    shader: str = Query(default=SHADERCLAW_DEFAULT_SHADER),
    parameters_json: str = Query(default="{}"),
    width: int = Query(default=SHADERCLAW_DEFAULT_WIDTH, ge=1, le=4096),
    height: int = Query(default=SHADERCLAW_DEFAULT_HEIGHT, ge=1, le=4096),
    fps: int = Query(default=30, ge=1, le=60),
):
    """Stream raw native ShaderClaw frames to a canvas over a local WebSocket."""

    await websocket.accept()
    try:
        parameters = _shaderclaw_parameters_from_json(parameters_json)
        _shaderclaw_library(shaders_dir).resolve_shader(shader)
    except Exception as exc:
        detail = exc.detail if isinstance(exc, HTTPException) else str(exc)
        await websocket.send_json({"type": "error", "error": detail})
        await websocket.close(code=1003)
        return

    frame_queue: queue.Queue[bytes | Exception | None] = queue.Queue(maxsize=2)
    command_queue: queue.Queue[dict[str, Any]] = queue.Queue(maxsize=8)
    stop_event = threading.Event()

    def publish(item: bytes | Exception | None) -> None:
        try:
            frame_queue.put_nowait(item)
            return
        except queue.Full:
            pass
        with contextlib.suppress(queue.Empty):
            frame_queue.get_nowait()
        with contextlib.suppress(queue.Full):
            frame_queue.put_nowait(item)

    def update_parameters(next_parameters: dict[str, Any]) -> None:
        try:
            command_queue.put_nowait(next_parameters)
            return
        except queue.Full:
            pass
        with contextlib.suppress(queue.Empty):
            command_queue.get_nowait()
        with contextlib.suppress(queue.Full):
            command_queue.put_nowait(next_parameters)

    def produce_frames() -> None:
        interval = 1.0 / fps
        current_parameters = dict(parameters)
        try:
            renderer = _shaderclaw_preview_renderer(
                shaders_dir=shaders_dir,
                width=width,
                height=height,
            )
            while not stop_event.is_set():
                with contextlib.suppress(queue.Empty):
                    while True:
                        current_parameters = command_queue.get_nowait()

                started_at = time.monotonic()
                publish(renderer.render_rgba_bytes(shader, current_parameters))
                elapsed = time.monotonic() - started_at
                if stop_event.wait(max(0.0, interval - elapsed)):
                    break
        except Exception as exc:
            publish(exc)
        finally:
            publish(None)

    producer = threading.Thread(
        target=produce_frames,
        name=f"shaderclaw-canvas-preview-{shader}",
        daemon=True,
    )

    async def send_frames() -> None:
        await websocket.send_json(
            {
                "type": "ready",
                "format": "rgba",
                "width": width,
                "height": height,
                "fps": fps,
            }
        )
        while True:
            item = await asyncio.to_thread(frame_queue.get)
            if item is None:
                break
            if isinstance(item, Exception):
                logger.warning("ShaderClaw canvas preview failed: %s", item)
                await websocket.send_json({"type": "error", "error": str(item)})
                break
            await websocket.send_bytes(item)

    async def receive_commands() -> None:
        while True:
            try:
                message = await websocket.receive_json()
            except WebSocketDisconnect:
                break
            if not isinstance(message, dict):
                continue
            if message.get("type") != "parameters":
                continue
            next_parameters = message.get("parameters")
            if isinstance(next_parameters, dict):
                update_parameters(next_parameters)

    producer.start()
    tasks = {
        asyncio.create_task(send_frames()),
        asyncio.create_task(receive_commands()),
    }
    try:
        done, pending = await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)
        for task in pending:
            task.cancel()
        for task in done:
            with contextlib.suppress(WebSocketDisconnect):
                task.result()
    finally:
        stop_event.set()
        await asyncio.to_thread(producer.join, 1.0)
        for task in tasks:
            task.cancel()
        with contextlib.suppress(Exception):
            await websocket.close()


@app.post("/api/v1/shaderclaw/screenshot")
async def shaderclaw_screenshot(request: ShaderClawLoadRequest):
    """Render a native ShaderClaw preview frame through the Scope origin."""

    def run():
        renderer = _shaderclaw_preview_renderer(
            shaders_dir=request.shaders_dir,
            width=request.width,
            height=request.height,
        )
        return {
            "dataUrl": renderer.render_data_url(
                request.shader,
                request.parameters or {},
            )
        }

    try:
        return await asyncio.to_thread(run)
    except HTTPException:
        raise
    except Exception as exc:
        raise _shaderclaw_http_error(exc) from exc


@app.get("/health", response_model=HealthResponse)
@cloud_proxy()
async def health_check(
    http_request: Request,
    cloud_manager: ScopeCloudBackend = Depends(get_scope_cloud),
):
    """Health check endpoint."""
    return HealthResponse(
        status="healthy",
        timestamp=datetime.now().isoformat(),
        server_start_time=server_start_time,
        version=version("daydream-scope"),
        git_commit=get_git_commit_hash(),
    )


@app.post("/api/v1/restart")
@cloud_proxy(timeout=30.0)
async def restart_server(
    http_request: Request,
    cloud_manager: ScopeCloudBackend = Depends(get_scope_cloud),
):
    """Restart the server process.

    This endpoint is called after plugin install/uninstall to ensure
    Python's module cache is refreshed. The server restarts by re-executing
    the entry point, which replaces the current process and keeps terminal
    output working.

    When running under the Electron app (DAYDREAM_SCOPE_MANAGED=1), the server
    exits with code 42 to signal the managing process to respawn it. This
    ensures proper PID tracking and prevents orphaned processes on Windows.

    Known limitation (Windows/Git Bash): After the server restarts and you
    press Ctrl+C, the terminal may appear to hang. The process exits correctly,
    but MinTTY's input buffer gets stuck. Press any key to get your prompt back.
    This is a quirk of how MinTTY handles process replacement and doesn't affect
    CMD, PowerShell, or the Electron app.
    """
    # If managed by an external process (e.g., Electron), exit and let it respawn us
    # This prevents orphaned processes on Windows where os.execv spawns a new PID
    if os.environ.get("DAYDREAM_SCOPE_MANAGED"):
        logger.info("Server restart requested (managed mode) - exiting for respawn...")

        def do_managed_exit():
            time.sleep(0.5)  # Give time for response to be sent
            logger.info("Exiting with code 42 for managed respawn...")
            os._exit(42)  # Use os._exit to terminate entire process from thread

        thread = threading.Thread(target=do_managed_exit, daemon=True)
        thread.start()
        return {"message": "Server exiting for respawn..."}

    # Standalone mode: self-restart via subprocess/os.execv
    def do_restart():
        time.sleep(0.5)  # Give time for response to be sent

        # Close all logging handlers to avoid file descriptor warnings
        for handler in logging.root.handlers[:]:
            handler.close()
            logging.root.removeHandler(handler)

        # On Windows, entry points are .exe files but sys.argv[0] may not have extension
        executable = sys.argv[0]
        if sys.platform == "win32" and not executable.endswith(".exe"):
            executable += ".exe"

        if sys.platform == "win32":
            # On Windows, we can't use os.execv() because it spawns a child process
            # instead of replacing in-place (unlike Unix). So we spawn with Popen
            # and exit. Known issue: In Git Bash/MinTTY, after Ctrl+C the terminal
            # may require an extra keypress due to MinTTY's input buffer handling.
            subprocess.Popen(
                [executable] + sys.argv[1:],
                stdin=subprocess.DEVNULL,
                stdout=None,  # Inherit parent's stdout
                stderr=None,  # Inherit parent's stderr
            )
            sys.stdout.flush()
            sys.stderr.flush()
            try:
                sys.stdin.close()
            except Exception:
                pass
            os._exit(0)
        else:
            # On Unix, execv works correctly (replaces process in-place)
            os.execv(executable, sys.argv)

    # Run in a thread to allow response to be sent first
    thread = threading.Thread(target=do_restart, daemon=True)
    thread.start()
    return {"message": "Server restarting..."}


@app.get("/")
async def root():
    """Serve the frontend at the root URL."""
    frontend_dist = Path(__file__).parent.parent.parent.parent / "frontend" / "dist"

    # Only serve SPA if frontend dist exists (production mode)
    if not frontend_dist.exists():
        return {"message": "Scope API - Frontend not built"}

    # Serve the frontend index.html with no-cache headers
    # This ensures clients like Electron alway fetch the latest HTML (which references hashed assets)
    index_file = frontend_dist / "index.html"
    if index_file.exists():
        return FileResponse(
            index_file,
            headers={
                "Cache-Control": "no-cache, no-store, must-revalidate",
                "Pragma": "no-cache",
                "Expires": "0",
            },
        )

    return {"message": "Scope API - Frontend index.html not found"}


@app.post("/api/v1/pipeline/load")
async def load_pipeline(
    request: PipelineLoadRequest,
    http_request: Request,
    pipeline_manager: "PipelineManager" = Depends(get_pipeline_manager),
    cloud_manager: ScopeCloudBackend = Depends(get_scope_cloud),
):
    """Load one or more pipelines.

    In cloud mode, remote pipelines are proxied to the cloud-hosted Scope
    backend while source-kind plugin pipelines stay on the desktop backend.
    """
    global _local_only_pipeline_load_active, _local_only_pipeline_load_node_ids
    del http_request
    try:
        # Normalize to list of (node_id, pipeline_id, load_params) tuples
        pipelines = _pipeline_load_tuples_from_request(request)

        # Pipeline active/available DMX path grouping can change after load/unload.
        # Mark the DMX known-path cache stale so it is rebuilt once on next packet.
        srv = get_dmx_server()
        if srv is not None:
            srv.invalidate_known_paths_cache()

        if getattr(cloud_manager, "is_connected", False):
            from .remote_scope import filter_remote_scope_pipeline_load_body

            local_pipelines, remote_pipelines = _split_local_only_pipeline_tuples(
                pipelines
            )
            _local_only_pipeline_load_active = bool(
                local_pipelines and not remote_pipelines
            )
            _local_only_pipeline_load_node_ids = (
                {node_id for node_id, _pipeline_id, _load_params in local_pipelines}
                if _local_only_pipeline_load_active
                else set()
            )

            if local_pipelines:
                asyncio.create_task(
                    pipeline_manager.load_pipelines(
                        local_pipelines,
                        connection_id=request.connection_id,
                        connection_info=request.connection_info,
                        user_id=request.user_id,
                    )
                )

            remote_body = filter_remote_scope_pipeline_load_body(
                _pipeline_load_request_body(request)
            )
            if remote_body is not None:
                await proxy_with_body(
                    cloud_manager,
                    method="POST",
                    path="/api/v1/pipeline/load",
                    body=remote_body,
                    timeout=60.0,
                )

            return {"message": "Pipeline loading initiated successfully"}

        _local_only_pipeline_load_active = False
        _local_only_pipeline_load_node_ids = set()

        # Local mode: start loading in background without blocking
        asyncio.create_task(
            pipeline_manager.load_pipelines(
                pipelines,
                connection_id=request.connection_id,
                connection_info=request.connection_info,
                user_id=request.user_id,
            )
        )
        return {"message": "Pipeline loading initiated successfully"}
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error loading pipeline: {e}")
        raise HTTPException(status_code=500, detail=str(e)) from e


@app.get("/api/v1/pipeline/status", response_model=PipelineStatusResponse)
async def get_pipeline_status(
    http_request: Request,
    pipeline_manager: "PipelineManager" = Depends(get_pipeline_manager),
    cloud_manager: ScopeCloudBackend = Depends(get_scope_cloud),
):
    """Get current pipeline status.

    In cloud mode, report local status for local-only source plugin loads and
    proxy remote pipeline status otherwise.
    """
    del http_request
    try:
        if (
            getattr(cloud_manager, "is_connected", False)
            and not _local_only_pipeline_load_active
        ):
            status_info = await proxy_with_body(
                cloud_manager,
                method="GET",
                path="/api/v1/pipeline/status",
            )
            return PipelineStatusResponse(**status_info)

        status_info = await pipeline_manager.get_status_info_async()
        if (
            getattr(cloud_manager, "is_connected", False)
            and _local_only_pipeline_load_active
        ):
            return _local_only_pipeline_status_response(status_info)

        return PipelineStatusResponse(**status_info)
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error getting pipeline status: {e}")
        raise HTTPException(status_code=500, detail=str(e)) from e


@app.get("/api/v1/pipelines/schemas", response_model=PipelineSchemasResponse)
@cloud_proxy(required_keys=("pipelines",))
async def get_pipeline_schemas(
    http_request: Request,
    cloud_manager: ScopeCloudBackend = Depends(get_scope_cloud),
):
    """Compat alias for the pipeline-rich subset of the unified node catalog.

    Derives its response from the unified :class:`NodeRegistry` —
    every entry whose :attr:`NodeDefinition.pipeline_meta` is set is a
    pipeline, and ``pipeline_meta`` already holds the full output of
    ``get_schema_with_metadata()`` (config_schema, mode_defaults,
    supports_lora, supports_vace, etc.). Kept so existing frontend
    callers in ``usePipelines.ts`` keep working without migration; new
    code should read from ``GET /api/v1/nodes/definitions`` instead.

    In cloud mode this proxies to the cloud-hosted scope backend.
    """
    global _pipeline_schemas_cache
    if _pipeline_schemas_cache is not None:
        return _pipeline_schemas_cache

    from scope.core.nodes.registry import NodeRegistry
    from scope.core.pipelines.registry import PipelineRegistry
    from scope.core.plugins import get_plugin_manager

    plugin_manager = get_plugin_manager()
    pipelines: dict = {}

    # Pipelines always appear — the set is ``PipelineRegistry.list_pipelines()``.
    # ``pipeline_meta`` is either the full ``get_schema_with_metadata()`` output
    # or a degraded identity-only stub with a ``schema_error`` field when that
    # call failed (see ``Pipeline.get_definition``). Either way the pipeline
    # stays visible in the UI; the legacy behaviour of 500-ing or silently
    # disappearing on a broken plugin is gone.
    pipeline_ids = set(PipelineRegistry.list_pipelines())
    for definition in NodeRegistry.get_all_definitions():
        if definition.node_type_id not in pipeline_ids:
            continue
        schema_data = dict(definition.pipeline_meta or {})
        schema_data["plugin_name"] = plugin_manager.get_plugin_for_pipeline(
            definition.node_type_id
        )
        pipelines[definition.node_type_id] = schema_data

    response = PipelineSchemasResponse(pipelines=pipelines)
    _pipeline_schemas_cache = response
    return response


# ---------------------------------------------------------------------------
# Node definitions
# ---------------------------------------------------------------------------


@app.get("/api/v1/nodes/definitions", response_model=NodeDefinitionsResponse)
@cloud_proxy(required_keys=("nodes",))
async def get_node_definitions(
    http_request: Request,
    cloud_manager: ScopeCloudBackend = Depends(get_scope_cloud),
):
    """Return definitions for every registered node — pipelines included.

    The unified discovery endpoint. Pipelines (Pipeline subclasses)
    appear with ``pipeline_meta`` populated; plain custom nodes leave
    ``pipeline_meta`` ``None``. Frontend consumers that only want plain
    nodes filter on ``pipeline_meta == null`` client-side; consumers
    that want rich pipeline data read from ``pipeline_meta`` directly.

    Memoized in ``_node_definitions_cache`` and invalidated by
    ``_invalidate_plugin_caches`` whenever a plugin is installed or
    uninstalled, so the modal open / graph hydrate hot paths don't
    rebuild the payload (including full ``pipeline_meta``) every call.

    In cloud mode this proxies to the cloud-hosted scope backend so the
    Add Node modal and graph hydrator see cloud-registered nodes (plain
    and pipeline) rather than just the local set.
    """
    global _node_definitions_cache
    if _node_definitions_cache is not None:
        return _node_definitions_cache

    from scope.core.nodes.registry import NodeRegistry
    from scope.core.plugins import get_plugin_manager

    plugin_manager = get_plugin_manager()

    definitions = []
    for d in NodeRegistry.get_all_definitions():
        payload = d.model_dump()
        payload["plugin_name"] = plugin_manager.get_plugin_for_type_id(d.node_type_id)
        definitions.append(payload)

    response = NodeDefinitionsResponse(nodes=definitions)
    _node_definitions_cache = response
    return response


# ---------------------------------------------------------------------------
# OSC endpoints
# ---------------------------------------------------------------------------


@app.get("/api/v1/osc/status")
async def osc_status():
    """Return current OSC server status (port, listening state, logging mode)."""

    srv = get_osc_server()
    if srv is None:
        return {
            "enabled": False,
            "listening": False,
            "port": None,
            "host": None,
            "log_all_messages": False,
        }
    return srv.status()


class OscSettingsRequest(BaseModel):
    log_all_messages: bool


@app.put("/api/v1/osc/settings")
async def update_osc_settings(request: OscSettingsRequest):
    """Update OSC server runtime settings (e.g. logging verbosity)."""

    srv = get_osc_server()
    if srv is None:
        raise HTTPException(status_code=503, detail="OSC server not running")
    srv.log_all_messages = request.log_all_messages
    return srv.status()


@app.get("/api/v1/osc/paths")
async def osc_paths(
    pm: "PipelineManager" = Depends(get_pipeline_manager),
):
    """Return all OSC paths split into active / available sections.

    Includes the most recently registered graph inventory (see
    ``POST /api/v1/osc/inventory``) so external clients can discover
    user-curated node param paths alongside the registry-derived ones.
    """
    from .osc_docs import get_osc_paths

    srv = get_osc_server()
    graph_inventory = srv.graph_inventory if srv else None
    return get_osc_paths(pm, graph_inventory)


class OscInventoryEntry(BaseModel):
    """One graph-supplied OSC path entry.

    Mirrors the dict shape ``osc_docs._normalize_graph_entry`` expects.
    Validated minimally: required fields are ``osc_address`` (must start
    with ``/scope/`` or be coerced to it) and ``type``. Anything else
    flows through as-is so the frontend can attach metadata (group,
    description, etc.) without a backend schema bump.
    """

    osc_address: str
    type: str
    description: str | None = None
    min: float | None = None
    max: float | None = None
    enum: list | None = None
    default: Any | None = None
    node_id: str | None = None
    param: str | None = None
    pipeline_id: str | None = None
    group: str | None = None


class OscInventoryRequest(BaseModel):
    paths: list[OscInventoryEntry]


@app.post("/api/v1/osc/inventory")
async def set_osc_inventory(request: OscInventoryRequest):
    """Replace the OSC server's per-graph inventory.

    The frontend computes this list from each node's ``oscConfig`` overlay
    and POSTs whenever the inventory changes (debounced). Empty list
    clears the inventory and reverts behavior to registry-derived paths
    only.
    """
    srv = get_osc_server()
    if srv is None:
        raise HTTPException(status_code=503, detail="OSC server not running")
    srv.set_graph_inventory([p.model_dump(exclude_none=True) for p in request.paths])
    return {"count": len(request.paths)}


@app.get("/api/v1/osc/stream")
async def osc_sse_stream():
    """Server-Sent Events stream that pushes OSC commands to the frontend in real time."""
    srv = get_osc_server()
    if srv is None:
        return Response(content="OSC server not running", status_code=503)

    q = srv.subscribe()

    async def event_generator():
        try:
            while True:
                try:
                    event = await asyncio.wait_for(q.get(), timeout=30.0)
                    yield f"data: {json.dumps(event)}\n\n"
                except TimeoutError:
                    # Keepalive comment — prevents proxy/browser timeout.
                    yield ": keepalive\n\n"
        except asyncio.CancelledError:
            pass
        finally:
            srv.unsubscribe(q)

    return StreamingResponse(
        event_generator(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            "X-Accel-Buffering": "no",
        },
    )


@app.get("/api/v1/osc/docs")
async def osc_docs_page(
    pm: "PipelineManager" = Depends(get_pipeline_manager),
):
    """Serve a self-contained HTML reference page for OSC control."""
    from .osc_docs import render_osc_docs_html

    srv = get_osc_server()
    port = srv.port if srv else 8000
    graph_inventory = srv.graph_inventory if srv else None
    html_content = render_osc_docs_html(pm, port, graph_inventory)
    return Response(content=html_content, media_type="text/html")


# ---------------------------------------------------------------------------
# DMX endpoints
# ---------------------------------------------------------------------------


@app.get("/api/v1/dmx/status")
async def dmx_status():
    """Return current DMX Art-Net server status."""
    srv = get_dmx_server()
    if srv is None:
        return {
            "enabled": False,
            "listening": False,
            "port": None,
            "preferred_port": 6454,
            "host": None,
            "log_all_messages": False,
            "mapping_count": 0,
        }
    return srv.status()


class DmxSettingsRequest(BaseModel):
    enabled: bool | None = None
    log_all_messages: bool | None = None
    preferred_port: int | None = Field(None, ge=1024, le=65535)


@app.put("/api/v1/dmx/settings")
async def update_dmx_settings(request: DmxSettingsRequest):
    """Update DMX server runtime settings (enabled, logging, preferred port)."""
    srv = get_dmx_server()
    if srv is None:
        raise HTTPException(status_code=503, detail="DMX server not initialized")

    from .dmx_config import load_config, save_config

    need_persist = False
    cfg = load_config()

    if request.enabled is not None:
        cfg["enabled"] = request.enabled
        srv.enabled = request.enabled
        need_persist = True
        if request.enabled and not srv.listening:
            await srv.start()
        elif not request.enabled and srv.listening:
            await srv.stop()

    if request.log_all_messages is not None:
        srv.log_all_messages = request.log_all_messages
        cfg["log_all_messages"] = request.log_all_messages
        need_persist = True

    if request.preferred_port is not None:
        srv.preferred_port = request.preferred_port

    if need_persist:
        save_config(cfg)

    return srv.status()


class DmxRestartRequest(BaseModel):
    preferred_port: int | None = Field(None, ge=1024, le=65535)


@app.post("/api/v1/dmx/restart")
async def dmx_restart(request: DmxRestartRequest):
    """Restart the DMX server to apply a new port. Persists preferred_port to config."""
    from .dmx_config import load_config, save_config

    srv = get_dmx_server()
    if srv is None:
        raise HTTPException(status_code=503, detail="DMX server not running")

    if request.preferred_port is not None:
        srv.preferred_port = request.preferred_port
        cfg = load_config()
        cfg["preferred_port"] = request.preferred_port
        save_config(cfg)

    await srv.stop()
    if srv.enabled:
        await srv.start()
    return srv.status()


@app.get("/api/v1/dmx/paths")
async def dmx_paths(
    pm: "PipelineManager" = Depends(get_pipeline_manager),
):
    """Return numeric DMX-mappable paths split into active / available."""
    from .dmx_paths import get_dmx_paths

    return get_dmx_paths(pm)


@app.get("/api/v1/dmx/config")
async def dmx_get_config():
    """Return the current persisted DMX mapping configuration."""
    from .dmx_config import load_config

    return load_config()


class DmxConfigRequest(BaseModel):
    enabled: bool | None = None
    preferred_port: int | None = Field(None, ge=1024, le=65535)
    log_all_messages: bool | None = None
    mappings: list[dict] | None = None


@app.put("/api/v1/dmx/config")
async def dmx_put_config(request: DmxConfigRequest):
    """Save / import a full DMX mapping configuration."""
    from .dmx_config import (
        load_config,
        mappings_to_dict,
        save_config,
    )

    cfg = load_config()
    if request.enabled is not None:
        cfg["enabled"] = request.enabled
    if request.preferred_port is not None:
        cfg["preferred_port"] = request.preferred_port
    if request.log_all_messages is not None:
        cfg["log_all_messages"] = request.log_all_messages
    if request.mappings is not None:
        normalized = mappings_to_dict(request.mappings)
        # Store only the validated/cleaned mappings list
        cfg["mappings"] = [
            {"universe": u, "channel": c, "key": k} for (u, c), k in normalized.items()
        ]
    save_config(cfg)

    # Hot-reload into the running server
    srv = get_dmx_server()
    if srv is not None:
        srv.log_all_messages = cfg.get("log_all_messages", False)
        srv.set_mappings(mappings_to_dict(cfg.get("mappings", [])))
        if request.enabled is not None:
            srv.enabled = cfg["enabled"]
            if srv.enabled and not srv.listening:
                await srv.start()
            elif not srv.enabled and srv.listening:
                await srv.stop()

    return cfg


@app.get("/api/v1/dmx/stream")
async def dmx_sse_stream():
    """Server-Sent Events stream pushing DMX commands to the frontend."""
    srv = get_dmx_server()
    if srv is None:
        return Response(content="DMX server not running", status_code=503)

    q = srv.subscribe()

    async def event_generator():
        try:
            while True:
                try:
                    event = await asyncio.wait_for(q.get(), timeout=30.0)
                    yield f"data: {json.dumps(event)}\n\n"
                except TimeoutError:
                    yield ": keepalive\n\n"
        except asyncio.CancelledError:
            pass
        finally:
            srv.unsubscribe(q)

    return StreamingResponse(
        event_generator(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            "X-Accel-Buffering": "no",
        },
    )


@app.get("/api/v1/webrtc/ice-servers", response_model=IceServersResponse)
async def get_ice_servers(
    webrtc_manager: "WebRTCManager" = Depends(get_webrtc_manager),
):
    """Return ICE server configuration for frontend WebRTC connection.

    Returns ICE configuration from the local backend.
    """
    ice_servers = []
    for server in webrtc_manager.rtc_config.iceServers:
        ice_servers.append(
            IceServerConfig(
                urls=server.urls,
                username=server.username if hasattr(server, "username") else None,
                credential=server.credential if hasattr(server, "credential") else None,
            )
        )

    return IceServersResponse(iceServers=ice_servers)


@app.post("/api/v1/webrtc/offer", response_model=WebRTCOfferResponse)
async def handle_webrtc_offer(
    request: WebRTCOfferRequest,
    webrtc_manager: "WebRTCManager" = Depends(get_webrtc_manager),
    pipeline_manager: "PipelineManager" = Depends(get_pipeline_manager),
    cloud_manager: ScopeCloudBackend = Depends(get_scope_cloud),
):
    """Handle WebRTC offer and return answer.

    In cloud mode, video flows through the backend to cloud:
        Browser → Backend (WebRTC) → cloud (WebRTC) → Backend → Browser

    This enables:
    - Spout input to be forwarded to cloud
    - Full control over the video pipeline on the backend
    - Local backend can record/manipulate frames
    """
    try:
        # If connected to cloud, use relay mode only when the graph contains
        # a remote pipeline. Local-only source plugin graphs run on desktop.
        if getattr(
            cloud_manager, "is_connected", False
        ) and _initial_parameters_require_remote_scope(request.initialParameters):
            logger.info("Using relay mode - video will flow through backend to cloud")
            return await webrtc_manager.handle_offer_with_relay(request, cloud_manager)
        if getattr(cloud_manager, "is_connected", False):
            logger.info(
                "Using local mode for local-only pipeline graph while remote Scope is connected"
            )

        # Local mode: ensure pipeline is loaded before proceeding.
        # Node-only graphs (no pipeline nodes) skip this check — the graph
        # executor handles custom nodes directly without loading pipelines.
        # Mixed graphs (pipeline + custom node) still require loaded pipelines.
        graph_data = (
            request.initialParameters.graph if request.initialParameters else None
        )
        has_pipeline_nodes = False
        if graph_data is not None:
            nodes = (
                graph_data.get("nodes", [])
                if isinstance(graph_data, dict)
                else getattr(graph_data, "nodes", [])
            )
            has_pipeline_nodes = any(
                (n.get("type") if isinstance(n, dict) else getattr(n, "type", None))
                == "pipeline"
                for n in nodes
            )
        # Skip the check only for node-only graphs (graph present, no pipeline
        # nodes). When no graph is sent at all, fall back to the legacy check.
        is_node_only_graph = graph_data is not None and not has_pipeline_nodes
        if not is_node_only_graph:
            status_info = await pipeline_manager.get_status_info_async()
            if status_info["status"] != "loaded":
                raise HTTPException(
                    status_code=400,
                    detail="Pipeline not loaded. Please load pipeline first.",
                )

        return await webrtc_manager.handle_offer(
            request, pipeline_manager, tempo_sync=tempo_sync
        )

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error handling WebRTC offer: {e}")
        raise HTTPException(status_code=500, detail=str(e)) from e


@app.patch(
    "/api/v1/webrtc/offer/{session_id}", status_code=204, response_class=Response
)
async def add_ice_candidate(
    session_id: str,
    candidate_request: IceCandidateRequest,
    webrtc_manager: "WebRTCManager" = Depends(get_webrtc_manager),
):
    """Add ICE candidate(s) to an existing WebRTC session (Trickle ICE).

    This endpoint follows the Trickle ICE pattern, allowing clients to send
    ICE candidates as they are discovered.

    Note: In cloud mode, the browser still connects to the LOCAL backend via WebRTC.
    The backend then relays frames to/from cloud via a separate WebRTC connection.
    So browser ICE candidates always go to the local WebRTC session.
    """
    # TODO: Validate that the Content-Type is 'application/trickle-ice-sdpfrag'
    # At the moment FastAPI defaults to validating that it is 'application/json'
    try:
        # Always add ICE candidates to the local session
        # (In cloud mode, browser connects to local backend, not directly to cloud)
        for candidate_init in candidate_request.candidates:
            await webrtc_manager.add_ice_candidate(
                session_id=session_id,
                candidate=candidate_init.candidate,
                sdp_mid=candidate_init.sdpMid,
                sdp_mline_index=candidate_init.sdpMLineIndex,
            )

        logger.debug(
            f"Added {len(candidate_request.candidates)} ICE candidates to session {session_id}"
        )

        # Return 204 No Content on success
        return Response(status_code=204)

    except ValueError as e:
        # Session not found or invalid candidate
        raise HTTPException(status_code=404, detail=str(e)) from e
    except Exception as e:
        logger.error(f"Error adding ICE candidate to session {session_id}: {e}")
        raise HTTPException(status_code=500, detail=str(e)) from e


@app.delete(
    "/api/v1/webrtc/offer/{session_id}", status_code=204, response_class=Response
)
async def close_webrtc_session(
    session_id: str,
    webrtc_manager: "WebRTCManager" = Depends(get_webrtc_manager),
):
    """Close and remove a WebRTC session.

    Used by the cloud proxy (fal_app) to tear down the WebRTC peer connection
    when the signaling WebSocket closes (e.g. MAX_DURATION_EXCEEDED).
    """
    try:
        await webrtc_manager.remove_session(session_id)
        return Response(status_code=204)
    except Exception as e:
        logger.error(f"Error closing WebRTC session {session_id}: {e}")
        raise HTTPException(status_code=500, detail=str(e)) from e


def _session_has_graph_record_nodes(session) -> bool:
    """True when the session uses graph record-node queues (per-node MP4)."""
    fp = session.frame_processor
    if fp is None:
        return False
    return bool(fp.sink_manager.recording.get_node_ids())


@app.get("/api/v1/recordings/{session_id}")
async def download_recording(
    session_id: str,
    background_tasks: BackgroundTasks,
    webrtc_manager: "WebRTCManager" = Depends(get_webrtc_manager),
    node_id: str | None = Query(
        None,
        description="Record node id for graph mode (per-node recording file)",
    ),
):
    """Download the recording file for the specified session.

    Serves local recordings for the specified session.

    When the graph has record nodes, pass ``node_id`` to download that node's file.
    """
    try:
        session = webrtc_manager.get_session(session_id)
        if not session:
            raise HTTPException(
                status_code=404,
                detail=f"Session {session_id} not found",
            )

        if node_id:
            if not _session_has_graph_record_nodes(session):
                raise HTTPException(
                    status_code=400,
                    detail="This session has no graph record nodes; omit node_id.",
                )
            coord = session.frame_processor.sink_manager.recording
            if node_id not in coord.get_node_ids():
                raise HTTPException(
                    status_code=400,
                    detail=f"Unknown record node {node_id!r}",
                )
            download_file = await coord.download_recording(node_id)
            if not download_file or not Path(download_file).exists():
                raise HTTPException(
                    status_code=404,
                    detail="Recording file not available",
                )
            background_tasks.add_task(cleanup_temp_file, download_file)
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"recording-{node_id}-{timestamp}.mp4"
            return FileResponse(
                download_file,
                media_type="video/mp4",
                filename=filename,
            )

        if _session_has_graph_record_nodes(session):
            raise HTTPException(
                status_code=400,
                detail="This session uses graph record nodes; add ?node_id=<record node id>.",
            )

        has_local_recording = session.recording_manager

        if has_local_recording:
            download_file = await session.recording_manager.finalize_and_get_recording()
            if not download_file or not Path(download_file).exists():
                raise HTTPException(
                    status_code=404,
                    detail="Recording file not available",
                )

            background_tasks.add_task(cleanup_temp_file, download_file)

            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"recording-{timestamp}.mp4"

            return FileResponse(
                download_file,
                media_type="video/mp4",
                filename=filename,
            )

        raise HTTPException(
            status_code=404,
            detail="No recording available for this session",
        )
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error downloading recording: {e}")
        raise HTTPException(status_code=500, detail=str(e)) from e


@app.post("/api/v1/recordings/{session_id}/start")
async def start_recording(
    session_id: str,
    webrtc_manager: "WebRTCManager" = Depends(get_webrtc_manager),
    node_id: str | None = Query(
        None,
        description="Record node id for graph mode (per-node recording)",
    ),
):
    """Start recording for the specified session.

    Creates a RecordingManager if one does not already exist (session-level).
    For graph record nodes, pass ``node_id`` to record that node's feed.
    """
    try:
        session = webrtc_manager.get_session(session_id)
        if not session:
            raise HTTPException(
                status_code=404, detail=f"Session {session_id} not found"
            )

        if node_id:
            if not _session_has_graph_record_nodes(session):
                raise HTTPException(
                    status_code=400,
                    detail="This session has no graph record nodes; omit node_id.",
                )
            coord = session.frame_processor.sink_manager.recording
            if node_id not in coord.get_node_ids():
                raise HTTPException(
                    status_code=400,
                    detail=f"Unknown record node {node_id!r}",
                )
            fps = session.frame_processor.get_fps()
            ok = await coord.start_recording(node_id, fps)
            if not ok:
                raise HTTPException(
                    status_code=500,
                    detail=f"Failed to start recording for record node {node_id!r}",
                )
            return {"status": "started"}

        if _session_has_graph_record_nodes(session):
            raise HTTPException(
                status_code=400,
                detail="This session uses graph record nodes; pass node_id=<record node id>.",
            )

        if not session.recording_manager:
            if not session.video_track:
                raise HTTPException(
                    status_code=400, detail="Session has no video track"
                )
            rm = RecordingManager(video_track=session.video_track)
            if session.relay:
                rm.set_relay(session.relay)
            session.recording_manager = rm

        if session.recording_manager.is_recording_started:
            return {"status": "already_recording"}

        await session.recording_manager.start_recording()
        return {"status": "started"}
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error starting recording for session {session_id}: {e}")
        raise HTTPException(status_code=500, detail=str(e)) from e


@app.post("/api/v1/recordings/{session_id}/stop")
async def stop_recording(
    session_id: str,
    webrtc_manager: "WebRTCManager" = Depends(get_webrtc_manager),
    node_id: str | None = Query(
        None,
        description="Record node id for graph mode (per-node recording)",
    ),
):
    """Stop recording for the specified session without downloading."""
    try:
        session = webrtc_manager.get_session(session_id)
        if not session:
            raise HTTPException(
                status_code=404, detail=f"Session {session_id} not found"
            )

        if node_id:
            if not _session_has_graph_record_nodes(session):
                raise HTTPException(
                    status_code=400,
                    detail="This session has no graph record nodes; omit node_id.",
                )
            coord = session.frame_processor.sink_manager.recording
            if node_id not in coord.get_node_ids():
                raise HTTPException(
                    status_code=400,
                    detail=f"Unknown record node {node_id!r}",
                )
            ok = await coord.stop_recording(node_id)
            return {"status": "stopped" if ok else "not_recording"}

        if _session_has_graph_record_nodes(session):
            raise HTTPException(
                status_code=400,
                detail="This session uses graph record nodes; pass node_id=<record node id>.",
            )

        if not session.recording_manager:
            raise HTTPException(
                status_code=404,
                detail=f"Recording not available for session {session_id}",
            )
        if not session.recording_manager.is_recording_started:
            return {"status": "not_recording"}

        await session.recording_manager.stop_recording()
        return {"status": "stopped"}
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error stopping recording for session {session_id}: {e}")
        raise HTTPException(status_code=500, detail=str(e)) from e


class ModelStatusResponse(BaseModel):
    downloaded: bool


class DownloadModelsRequest(BaseModel):
    pipeline_id: str


class LoRAFileInfo(BaseModel):
    """Metadata for an available LoRA file on disk."""

    name: str
    path: str
    size_mb: float
    folder: str | None = None
    sha256: str | None = None
    provenance: LoRAProvenance | None = None
    read_only: bool = False


class LoRAFilesResponse(BaseModel):
    """Response containing all discoverable LoRA files."""

    lora_files: list[LoRAFileInfo]


@app.get("/api/v1/loras", response_model=LoRAFilesResponse)
@cloud_proxy(required_keys=("lora_files",))
async def list_lora_files(
    http_request: Request,
    cloud_manager: ScopeCloudBackend = Depends(get_scope_cloud),
):
    """List available LoRA files in the models/lora directory and its subdirectories.

    When cloud mode is active, lists LoRA files from the cloud server instead.
    """

    def process_lora_file(
        file_path: Path, lora_dir: Path, manifest_entries: dict
    ) -> LoRAFileInfo:
        """Extract LoRA file metadata."""
        size_mb = file_path.stat().st_size / (1024 * 1024)
        relative_path = file_path.relative_to(lora_dir)
        folder = (
            str(relative_path.parent) if relative_path.parent != Path(".") else None
        )
        rel_key = relative_path.as_posix()
        entry = manifest_entries.get(rel_key)
        return LoRAFileInfo(
            name=file_path.stem,
            path=str(file_path),
            size_mb=round(size_mb, 2),
            folder=folder,
            sha256=entry.sha256 if entry else None,
            provenance=entry.provenance if entry else None,
        )

    try:
        lora_dir = get_lora_dir()
        manifest = load_manifest(lora_dir)
        lora_files: list[LoRAFileInfo] = []

        for file_path in iter_files(lora_dir, LORA_EXTENSIONS):
            lora_files.append(process_lora_file(file_path, lora_dir, manifest.entries))

        # Also include LoRAs from the shared (persistent) directory if set.
        # This surfaces pre-cached sample LoRAs in cloud mode.
        shared_dir = get_shared_lora_dir()
        if shared_dir and shared_dir.is_dir():
            shared_names = {f.stem for f in iter_files(shared_dir, LORA_EXTENSIONS)}
            # Mark session-dir LoRAs as read_only if they also exist in
            # the shared dir (i.e. they are sample/onboarding LoRAs).
            for lf in lora_files:
                if lf.name in shared_names:
                    lf.read_only = True
            seen = {lf.name for lf in lora_files}
            shared_manifest = load_manifest(shared_dir)
            for file_path in iter_files(shared_dir, LORA_EXTENSIONS):
                if file_path.stem not in seen:
                    info = process_lora_file(
                        file_path, shared_dir, shared_manifest.entries
                    )
                    info.read_only = True
                    lora_files.append(info)

        lora_files.sort(key=lambda x: (x.folder or "", x.name))
        return LoRAFilesResponse(lora_files=lora_files)

    except Exception as e:  # pragma: no cover - defensive logging
        logger.error(f"list_lora_files: Error listing LoRA files: {e}")
        raise HTTPException(status_code=500, detail=str(e)) from e


class LoRAInstallRequest(BaseModel):
    url: str
    filename: str | None = None


class LoRAInstallResponse(BaseModel):
    message: str
    file: LoRAFileInfo


ALLOWED_LORA_HOSTS = {"civitai.com", "huggingface.co", "hf.co"}

# Hosts that resolve to Hugging Face. `hf.co` is the official short
# domain and redirects to huggingface.co. We treat both identically for
# blob-vs-resolve detection and provenance tagging.
_HUGGINGFACE_HOSTS = ("huggingface.co", "hf.co")


def _is_huggingface_host(hostname: str) -> bool:
    return any(hostname == h or hostname.endswith(f".{h}") for h in _HUGGINGFACE_HOSTS)


@app.post("/api/v1/loras", response_model=LoRAInstallResponse)
async def install_lora_file(
    request: LoRAInstallRequest,
    http_request: Request,
    cloud_manager: ScopeCloudBackend = Depends(get_scope_cloud),
):
    """Install a LoRA file from a URL (e.g. HuggingFace, CivitAI).

    When cloud mode is active, the install happens on the cloud machine.
    Token injection for CivitAI URLs happens locally before proxying.
    """
    from urllib.parse import parse_qs, urlparse

    from .models_config import get_civitai_token

    # Inject CivitAI token if needed (before cloud proxying)
    url = request.url
    parsed = urlparse(url)
    hostname = parsed.hostname or ""
    is_civitai = hostname == "civitai.com" or hostname.endswith(".civitai.com")
    is_huggingface = _is_huggingface_host(hostname)

    # Reject HuggingFace "blob" URLs — these point at the file's HTML preview
    # page (e.g. /Repo/blob/main/file.safetensors), not the raw download.
    # Pasting one would either 404 or save HTML as a .safetensors file. The
    # correct URL uses /resolve/main/, which the HF "Copy download link"
    # button on the file's page produces. Apply this *before* cloud proxying
    # so users get the same friendly error in both local and cloud modes.
    if is_huggingface and "/blob/" in parsed.path:
        raise HTTPException(
            status_code=400,
            detail=(
                "This is a Hugging Face file preview page, not a download link. "
                'Open the file on the LoRA page and click "Copy download link" '
                "(it will contain /resolve/main/ instead of /blob/main/)."
            ),
        )

    if is_civitai:
        query_params = parse_qs(parsed.query)
        if "token" not in query_params:
            stored_token = get_civitai_token()
            if stored_token:
                separator = "&" if parsed.query else "?"
                url = f"{url}{separator}token={stored_token}"

    # If connected to cloud, proxy with the (potentially modified) URL
    if cloud_manager.is_connected:
        body = {"url": url, "filename": request.filename}
        return await proxy_with_body(
            cloud_manager, "POST", "/api/v1/loras", body, timeout=300.0
        )

    # Local installation
    import re
    from urllib.parse import unquote

    import httpx

    from .download_models import http_get

    try:
        # Re-parse URL (may have been modified with token)
        parsed = urlparse(url)
        hostname = parsed.hostname or ""

        # Validate hostname is from allowed sources
        is_allowed = any(
            hostname == allowed or hostname.endswith(f".{allowed}")
            for allowed in ALLOWED_LORA_HOSTS
        )
        if not is_allowed:
            raise HTTPException(
                status_code=400,
                detail=f"URL must be from {' or '.join(sorted(ALLOWED_LORA_HOSTS))}",
            )

        # Determine filename from URL if not provided
        filename = request.filename
        if not filename:
            filename = unquote(parsed.path.split("/")[-1])

        # For CivitAI URLs, extract version ID and resolve filename via API
        version_id = None
        civitai_token = None
        if is_civitai:
            # Extract version ID: check query param first, then path
            query_params = parse_qs(parsed.query)
            if "modelVersionId" in query_params:
                version_id = query_params["modelVersionId"][0]
            else:
                # Fall back to last path segment (e.g. /api/download/models/<version_id>)
                path_parts = parsed.path.rstrip("/").split("/")
                candidate = path_parts[-1] if path_parts else None
                if candidate and candidate.isdigit():
                    version_id = candidate
            civitai_token = query_params.get("token", [None])[0]
            logger.info(
                f"CivitAI resolve: version_id={version_id}, "
                f"token_from_url={'yes' if civitai_token else 'no'}, "
                f"filename={filename}"
            )

        if is_civitai and (not filename or "." not in filename):
            if version_id:
                try:
                    from .lora_downloader import resolve_civitai_metadata

                    dl_url, civitai_filename = resolve_civitai_metadata(
                        version_id, token=civitai_token
                    )
                    if civitai_filename:
                        filename = civitai_filename
                    if dl_url:
                        url = dl_url
                except ValueError as e:
                    raise HTTPException(status_code=400, detail=str(e)) from e
                except Exception as e:
                    logger.warning(f"Failed to resolve CivitAI metadata: {e}")

        # If still no filename (or it doesn't look like a file), try Content-Disposition
        if not filename or "." not in filename:
            # Use streaming GET instead of HEAD (some servers return 403 for HEAD)
            with httpx.Client(follow_redirects=True, timeout=10.0) as client:
                with client.stream("GET", url) as response:
                    if response.status_code == 401 or response.status_code == 403:
                        raise HTTPException(
                            status_code=response.status_code,
                            detail="Access denied. Check that the URL is correct and includes any required authentication.",
                        )
                    if response.status_code == 404:
                        raise HTTPException(
                            status_code=404,
                            detail="File not found. Check that the URL is correct.",
                        )
                    if response.status_code >= 400:
                        raise HTTPException(
                            status_code=response.status_code,
                            detail=f"Failed to fetch URL: HTTP {response.status_code}",
                        )
                    content_disp = response.headers.get("content-disposition", "")
                    # Parse filename from Content-Disposition header
                    # e.g., 'attachment; filename="model.safetensors"'
                    match = re.search(
                        r'filename[*]?=["\']?([^"\';]+)["\']?', content_disp
                    )
                    if match:
                        filename = unquote(match.group(1).strip())
                    # Don't read the body - just close the connection

        if not filename or "." not in filename:
            raise HTTPException(
                status_code=400,
                detail="Could not determine filename from URL. Please provide a filename.",
            )

        # Validate file extension
        ext = Path(filename).suffix.lower()
        if ext not in LORA_EXTENSIONS:
            raise HTTPException(
                status_code=400,
                detail=f"Invalid file extension '{ext}'. Allowed: {', '.join(sorted(LORA_EXTENSIONS))}",
            )

        lora_dir = get_lora_dir()
        dest_path = (lora_dir / filename).resolve()
        if not dest_path.is_relative_to(lora_dir.resolve()):
            raise HTTPException(status_code=400, detail="Invalid filename")

        if dest_path.exists():
            raise HTTPException(
                status_code=409,
                detail=f"File '{filename}' already exists.",
            )

        # Install in a thread to avoid blocking the event loop
        loop = asyncio.get_running_loop()
        await loop.run_in_executor(None, http_get, url, dest_path)

        # Update manifest with provenance
        sha256 = await loop.run_in_executor(None, compute_sha256, dest_path)
        size_bytes = dest_path.stat().st_size
        relative_key = dest_path.relative_to(lora_dir).as_posix()

        source: str
        if is_civitai:
            source = "civitai"
        elif _is_huggingface_host(hostname):
            source = "huggingface"
        else:
            source = "url"

        # Strip sensitive query params (e.g. CivitAI API token) before persisting
        from urllib.parse import urlencode, urlunparse
        from urllib.parse import urlparse as _urlparse

        _parsed = _urlparse(url)
        clean_params = {
            k: v for k, v in parse_qs(_parsed.query).items() if k.lower() != "token"
        }
        clean_url = urlunparse(
            _parsed._replace(
                query=urlencode(clean_params, doseq=True) if clean_params else ""
            )
        )

        # Parse structured fields from URLs so downstream downloads can use
        # authenticated paths (hf_hub_url for HF, version API for CivitAI).
        extra: dict = {}
        if source == "huggingface":
            hf_parts = [p for p in _parsed.path.split("/") if p]
            if len(hf_parts) >= 5 and hf_parts[2] in ("resolve", "blob"):
                extra["repo_id"] = f"{hf_parts[0]}/{hf_parts[1]}"
                extra["hf_filename"] = "/".join(hf_parts[4:])
        elif source == "civitai":
            civ_query = parse_qs(_parsed.query)
            if "modelVersionId" in civ_query:
                extra["version_id"] = civ_query["modelVersionId"][0]
            else:
                civ_parts = _parsed.path.rstrip("/").split("/")
                if civ_parts and civ_parts[-1].isdigit():
                    extra["version_id"] = civ_parts[-1]

        provenance = LoRAProvenance(source=source, url=clean_url, **extra)
        entry = add_manifest_entry(
            lora_dir, relative_key, provenance, sha256, size_bytes
        )

        size_mb = size_bytes / (1024 * 1024)
        file_info = LoRAFileInfo(
            name=dest_path.stem,
            path=str(dest_path),
            size_mb=round(size_mb, 2),
            folder=None,
            sha256=sha256,
            provenance=entry.provenance,
        )

        return LoRAInstallResponse(
            message=f"Successfully installed '{filename}'",
            file=file_info,
        )

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"install_lora_file: Error installing LoRA: {e}")
        raise HTTPException(status_code=500, detail=str(e)) from e


class LoRADeleteResponse(BaseModel):
    success: bool
    message: str


@app.delete("/api/v1/loras/{name}")
@cloud_proxy()
async def delete_lora_file(
    name: str,
    http_request: Request,
    cloud_manager: ScopeCloudBackend = Depends(get_scope_cloud),
):
    """Delete a LoRA file by name."""
    try:
        lora_dir = get_lora_dir()

        # Search for the file with any supported extension
        found_path = None
        for ext in LORA_EXTENSIONS:
            candidate = lora_dir / f"{name}{ext}"
            if candidate.exists():
                found_path = candidate
                break

        # Also check subdirectories
        if not found_path:
            for subdir in lora_dir.iterdir():
                if subdir.is_dir():
                    for ext in LORA_EXTENSIONS:
                        candidate = subdir / f"{name}{ext}"
                        if candidate.exists():
                            found_path = candidate
                            break
                    if found_path:
                        break

        if not found_path:
            raise HTTPException(
                status_code=404,
                detail=f"LoRA file '{name}' not found",
            )

        # In cloud mode, prevent deletion of sample/onboarding LoRAs that
        # are cached in the shared persistent directory.
        shared_dir = get_shared_lora_dir()
        if shared_dir and (shared_dir / found_path.name).is_file():
            raise HTTPException(
                status_code=403,
                detail=f"'{name}' is a sample LoRA and cannot be removed in cloud mode",
            )

        # Delete the file
        found_path.unlink()
        logger.info(f"Deleted LoRA file: {found_path}")

        # Remove from manifest if present
        manifest = load_manifest(lora_dir)
        relative_key = found_path.relative_to(lora_dir).as_posix()
        if relative_key in manifest.entries:
            del manifest.entries[relative_key]
            save_manifest(lora_dir, manifest)

        return LoRADeleteResponse(
            success=True,
            message=f"Successfully deleted '{name}'",
        )

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"delete_lora_file: Error deleting LoRA: {e}")
        raise HTTPException(status_code=500, detail=str(e)) from e


@app.post("/api/v1/lora/download")
async def download_lora_endpoint(
    request: LoRADownloadRequest,
    cloud_manager: ScopeCloudBackend = Depends(get_scope_cloud),
) -> LoRADownloadResult:
    """Download a LoRA from HuggingFace, CivitAI, or a direct URL.

    When cloud mode is active, the CivitAI token is resolved locally and
    forwarded so the cloud worker can authenticate without its own token.
    """
    from .lora_downloader import download_lora
    from .models_config import get_civitai_token

    civitai_token = None
    if request.source == "civitai":
        civitai_token = get_civitai_token() or request.civitai_token

    if cloud_manager.is_connected:
        body = request.model_dump(exclude_none=True)
        if civitai_token:
            body["civitai_token"] = civitai_token
        return await proxy_with_body(
            cloud_manager, "POST", "/api/v1/lora/download", body, timeout=300.0
        )

    lora_dir = get_lora_dir()
    try:
        return await download_lora(request, lora_dir, civitai_token=civitai_token)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e)) from e
    except Exception as e:
        logger.error(f"download_lora: Error downloading LoRA: {e}")
        raise HTTPException(status_code=500, detail=str(e)) from e


@app.put("/api/v1/lora/{filename:path}/provenance")
async def tag_lora_provenance(
    filename: str,
    provenance: LoRAProvenance,
) -> LoRAManifestEntry:
    """Retroactively tag a local LoRA with provenance info."""
    lora_dir = get_lora_dir()
    file_path = (lora_dir / filename).resolve()
    if not file_path.is_relative_to(lora_dir.resolve()):
        raise HTTPException(status_code=400, detail="Invalid filename")
    if not file_path.exists():
        raise HTTPException(status_code=404, detail=f"LoRA file '{filename}' not found")

    sha256 = compute_sha256(file_path)
    size_bytes = file_path.stat().st_size

    return add_manifest_entry(lora_dir, filename, provenance, sha256, size_bytes)


# ---------------------------------------------------------------------------
# Workflow resolve
# ---------------------------------------------------------------------------


@app.post("/api/v1/workflow/resolve", response_model=WorkflowResolutionPlan)
@cloud_proxy()
async def resolve_workflow_endpoint(
    workflow: WorkflowRequest,
    http_request: Request,
    cloud_manager: ScopeCloudBackend = Depends(get_scope_cloud),
):
    """Resolve workflow dependencies and return a resolution plan.

    This is side-effect-free: no installs, no downloads.  The request
    body uses ``extra="ignore"`` so the frontend can send the full
    workflow JSON; the backend only reads the fields it needs.
    """
    from scope.core.plugins import get_plugin_manager

    try:
        plugin_manager = get_plugin_manager()
        lora_dir = get_lora_dir()
        shared_lora_dir = get_shared_lora_dir()

        return resolve_workflow(workflow, plugin_manager, lora_dir, shared_lora_dir)
    except Exception as e:
        logger.error("Error resolving workflow: %s", e, exc_info=True)
        raise HTTPException(
            status_code=500,
            detail="Internal error while resolving workflow dependencies",
        ) from e


@app.get("/api/v1/assets", response_model=AssetsResponse)
@cloud_proxy(required_keys=("assets",))
async def list_assets(
    http_request: Request,
    type: str | None = Query(None, description="Filter by asset type (image, video)"),
    cloud_manager: ScopeCloudBackend = Depends(get_scope_cloud),
):
    """List available asset files in the assets directory and its subdirectories.

    When cloud mode is active, lists assets from the cloud server instead.
    """

    def process_asset_file(
        file_path: Path, assets_dir: Path, asset_type: str
    ) -> AssetFileInfo:
        """Extract asset file metadata."""
        size_mb = file_path.stat().st_size / (1024 * 1024)
        created_at = file_path.stat().st_ctime
        relative_path = file_path.relative_to(assets_dir)
        folder = (
            str(relative_path.parent) if relative_path.parent != Path(".") else None
        )
        return AssetFileInfo(
            name=file_path.stem,
            path=str(file_path),
            size_mb=round(size_mb, 2),
            folder=folder,
            type=asset_type,
            created_at=created_at,
        )

    try:
        assets_dir = get_assets_dir()
        asset_files: list[AssetFileInfo] = []

        if type == "image":
            extensions = IMAGE_EXTENSIONS
        elif type == "video":
            extensions = VIDEO_EXTENSIONS
        elif type == "audio":
            extensions = AUDIO_EXTENSIONS
        else:
            extensions = IMAGE_EXTENSIONS | VIDEO_EXTENSIONS | AUDIO_EXTENSIONS

        for file_path in iter_files(assets_dir, extensions):
            ext = file_path.suffix.lower()
            if ext in IMAGE_EXTENSIONS:
                asset_type = "image"
            elif ext in AUDIO_EXTENSIONS:
                asset_type = "audio"
            else:
                asset_type = "video"
            asset_files.append(process_asset_file(file_path, assets_dir, asset_type))

        # Sort by created_at (most recent first), then by folder and name
        asset_files.sort(key=lambda x: (-x.created_at, x.folder or "", x.name))
        return AssetsResponse(assets=asset_files)

    except Exception as e:  # pragma: no cover - defensive logging
        logger.error(f"list_assets: Error listing asset files: {e}")
        raise HTTPException(status_code=500, detail=str(e)) from e


@app.post("/api/v1/assets", response_model=AssetFileInfo)
async def upload_asset(
    request: Request,
    filename: str = Query(...),
    cloud_manager: ScopeCloudBackend = Depends(get_scope_cloud),
):
    """Upload an asset file (image, video, or audio) to the assets directory.

    When cloud mode is active, the file is uploaded to the cloud server instead.
    """

    try:
        allowed_extensions = IMAGE_EXTENSIONS | VIDEO_EXTENSIONS | AUDIO_EXTENSIONS

        file_extension = Path(filename).suffix.lower()
        if file_extension not in allowed_extensions:
            raise HTTPException(
                status_code=400,
                detail=f"Invalid file type. Allowed types: {', '.join(allowed_extensions)}",
            )

        if file_extension in IMAGE_EXTENSIONS:
            asset_type = "image"
            content_type_map = {
                ".png": "image/png",
                ".jpg": "image/jpeg",
                ".jpeg": "image/jpeg",
                ".webp": "image/webp",
                ".bmp": "image/bmp",
            }
        elif file_extension in AUDIO_EXTENSIONS:
            asset_type = "audio"
            content_type_map = {
                ".wav": "audio/wav",
                ".mp3": "audio/mpeg",
                ".flac": "audio/flac",
                ".ogg": "audio/ogg",
            }
        else:
            asset_type = "video"
            content_type_map = {
                ".mp4": "video/mp4",
                ".avi": "video/x-msvideo",
                ".mov": "video/quicktime",
                ".mkv": "video/x-matroska",
                ".webm": "video/webm",
            }
        content_type = content_type_map.get(file_extension, "application/octet-stream")

        # Read file content from request body
        content = await request.body()

        # Apply upload size validation only for cloud uploads.
        # Local mode keeps files on the same machine, so no explicit cap is enforced.
        if cloud_manager.is_connected:
            max_size = 50 * 1024 * 1024  # 50MB
            if len(content) > max_size:
                raise HTTPException(
                    status_code=400,
                    detail=f"File size exceeds maximum of {max_size / (1024 * 1024):.0f}MB",
                )

        # If cloud mode is active, upload to cloud AND save locally for thumbnails
        if cloud_manager.is_connected:
            return await upload_asset_to_cloud(
                cloud_manager,
                content,
                filename,
                content_type,
                asset_type,
                fal_cdn_token=request.headers.get("X-Fal-CDN-Token"),
                fal_cdn_token_type=request.headers.get("X-Fal-CDN-Token-Type"),
                fal_cdn_base_url=request.headers.get("X-Fal-CDN-Base-URL"),
            )

        assets_dir = get_assets_dir()
        assets_dir.mkdir(parents=True, exist_ok=True)

        # Save file to assets directory
        file_path = (assets_dir / filename).resolve()
        if not file_path.is_relative_to(assets_dir.resolve()):
            raise HTTPException(status_code=400, detail="Invalid filename")
        file_path.parent.mkdir(parents=True, exist_ok=True)
        file_path.write_bytes(content)

        # Return file info matching AssetFileInfo structure
        size_mb = len(content) / (1024 * 1024)
        created_at = file_path.stat().st_ctime
        relative_path = file_path.relative_to(assets_dir)
        folder = (
            str(relative_path.parent) if relative_path.parent != Path(".") else None
        )

        logger.info(f"upload_asset: Uploaded {asset_type} file: {file_path}")
        return AssetFileInfo(
            name=file_path.stem,
            path=str(file_path),
            size_mb=round(size_mb, 2),
            folder=folder,
            type=asset_type,
            created_at=created_at,
        )

    except HTTPException:
        raise
    except Exception as e:  # pragma: no cover - defensive logging
        logger.error(f"upload_asset: Error uploading asset file: {e}")
        raise HTTPException(status_code=500, detail=str(e)) from e


@app.get("/api/v1/assets/{asset_path:path}")
async def serve_asset(asset_path: str):
    """Serve an asset file (for thumbnails/previews).

    Handles both relative paths and absolute paths (e.g., from cloud).
    For absolute paths, extracts the filename and serves from local assets.
    """
    try:
        assets_dir = get_assets_dir()

        # Handle absolute paths (e.g., from cloud: /root/.daydream-scope/assets/filename.png)
        # Extract just the filename to serve from local cache
        if asset_path.startswith("/") or asset_path.startswith("root/"):
            # Extract just the filename
            filename = Path(asset_path).name
            file_path = assets_dir / filename
            logger.debug(
                f"serve_asset: Extracted filename '{filename}' from absolute path"
            )
        else:
            file_path = assets_dir / asset_path

        # Security check: ensure the path is within assets directory
        try:
            file_path = file_path.resolve()
            assets_dir_resolved = assets_dir.resolve()
            if not str(file_path).startswith(str(assets_dir_resolved)):
                raise HTTPException(status_code=403, detail="Access denied")
        except Exception:
            raise HTTPException(status_code=403, detail="Invalid path") from None

        if not file_path.exists() or not file_path.is_file():
            raise HTTPException(status_code=404, detail="Asset not found")

        # Determine media type based on extension
        file_extension = file_path.suffix.lower()
        media_types = {
            ".png": "image/png",
            ".jpg": "image/jpeg",
            ".jpeg": "image/jpeg",
            ".webp": "image/webp",
            ".bmp": "image/bmp",
            ".mp4": "video/mp4",
            ".avi": "video/x-msvideo",
            ".mov": "video/quicktime",
            ".mkv": "video/x-matroska",
            ".webm": "video/webm",
        }
        media_type = media_types.get(file_extension, "application/octet-stream")

        return FileResponse(file_path, media_type=media_type)

    except HTTPException:
        raise
    except Exception as e:  # pragma: no cover - defensive logging
        logger.error(f"serve_asset: Error serving asset file: {e}")
        raise HTTPException(status_code=500, detail=str(e)) from e


@app.get("/api/v1/models/status")
@cloud_proxy()
async def get_model_status(
    http_request: Request,
    pipeline_id: str,
    cloud_manager: ScopeCloudBackend = Depends(get_scope_cloud),
):
    """Check if models for a pipeline are downloaded and get download progress."""
    try:
        progress = download_progress_manager.get_progress(pipeline_id)

        # If download is in progress, always report as not downloaded
        if progress and progress.get("is_downloading"):
            return {"downloaded": False, "progress": progress}

        # Check if files actually exist
        downloaded = models_are_downloaded(pipeline_id)

        # Clean up progress if download is complete
        if downloaded and progress:
            download_progress_manager.clear_progress(pipeline_id)
            progress = None

        return {"downloaded": downloaded, "progress": progress}
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error checking model status: {e}")
        raise HTTPException(status_code=500, detail=str(e)) from e


@app.post("/api/v1/models/download")
@cloud_proxy(timeout=60.0)
async def download_pipeline_models(
    request: DownloadModelsRequest,
    http_request: Request,
    cloud_manager: ScopeCloudBackend = Depends(get_scope_cloud),
):
    """Download models for a specific pipeline."""
    try:
        if not request.pipeline_id:
            raise HTTPException(status_code=400, detail="pipeline_id is required")

        pipeline_id = request.pipeline_id

        # Local mode: check if download already in progress
        existing_progress = download_progress_manager.get_progress(pipeline_id)
        if existing_progress and existing_progress.get("is_downloading"):
            raise HTTPException(
                status_code=409,
                detail=f"Download already in progress for {pipeline_id}",
            )

        # Clear any previous error state before starting a new download
        download_progress_manager.clear_progress(pipeline_id)

        # Download in a background thread to avoid blocking
        import threading

        def _is_auth_error(error: Exception) -> bool:
            """Check if a download error is authentication-related."""
            msg = str(error)
            return "401" in msg or "403" in msg or "Unauthorized" in msg

        def download_in_background():
            """Run download in background thread."""
            try:
                download_models(pipeline_id)
                download_progress_manager.mark_complete(pipeline_id)
            except Exception as e:
                logger.error(f"Error downloading models for {pipeline_id}: {e}")
                if _is_auth_error(e):
                    user_msg = "Download failed due to authentication error. For HuggingFace models, make sure your HuggingFace key is configured in Settings > API Keys."
                else:
                    user_msg = "Download failed. Check the server logs for details."
                download_progress_manager.mark_error(pipeline_id, user_msg)

        thread = threading.Thread(target=download_in_background)
        thread.daemon = True
        thread.start()

        return {"message": f"Model download started for {pipeline_id}"}
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error starting model download: {e}")
        raise HTTPException(status_code=500, detail=str(e)) from e


def is_spout_available() -> bool:
    """Check if Spout is available (native Windows only, not WSL)."""
    return sys.platform == "win32"


def is_ndi_output_available() -> bool:
    """Check if NDI SDK is available for output."""
    from scope.core.ndi import is_available

    return is_available()


def is_syphon_output_available() -> bool:
    """Check if Syphon is available for output."""
    if sys.platform != "darwin":
        return False
    try:
        import syphon  # noqa: F401

        return True
    except ImportError:
        return False


def get_spout_availability() -> IntegrationAvailability:
    if is_spout_available():
        return IntegrationAvailability(available=True)
    if sys.platform != "win32":
        return IntegrationAvailability(
            available=False,
            reason="Spout is Windows-only",
            install_hint="Run Scope on Windows to use Spout sources and sinks.",
        )
    return IntegrationAvailability(
        available=False,
        reason="Spout runtime not detected",
        install_hint="Install Spout2 from https://spout.zeal.co and restart Scope.",
    )


def get_ndi_availability() -> IntegrationAvailability:
    if is_ndi_output_available():
        return IntegrationAvailability(available=True)
    return IntegrationAvailability(
        available=False,
        reason="NDI SDK not found",
        install_hint="Install the NDI SDK from https://ndi.video/tools and restart Scope.",
    )


def get_syphon_availability() -> IntegrationAvailability:
    if is_syphon_output_available():
        return IntegrationAvailability(available=True)
    if sys.platform != "darwin":
        return IntegrationAvailability(
            available=False,
            reason="Syphon is macOS-only",
            install_hint="Run Scope on macOS to use Syphon sources and sinks.",
        )
    return IntegrationAvailability(
        available=False,
        reason="Syphon package not installed",
        install_hint="Reinstall Scope dependencies with `uv sync` to enable Syphon.",
    )


_source_discovery_cache: dict[str, tuple[float, list]] = {}
_SOURCE_DISCOVERY_TTL = 10  # seconds


def _resolve_input_source_class(source_type: str):
    """Resolve a source_type string to its InputSource class, or raise HTTPException."""
    from scope.core.inputs import get_input_source_classes

    source_classes = get_input_source_classes()
    source_class = source_classes.get(source_type)
    if source_class is None:
        raise HTTPException(
            status_code=404,
            detail=f"Unknown input source type '{source_type}'. "
            f"Available types: {list(source_classes.keys())}",
        )
    if not source_class.is_available():
        raise HTTPException(
            status_code=400,
            detail=f"Input source '{source_type}' is not available on this system.",
        )
    return source_class


@app.get("/api/v1/input-sources")
async def list_input_source_types():
    """List available input source types with their availability status."""
    from scope.core.inputs import get_input_source_classes

    sources = []
    for cls in get_input_source_classes().values():
        sources.append(
            {
                "source_id": cls.source_id,
                "source_name": cls.source_name,
                "source_description": cls.source_description,
                "available": cls.is_available(),
            }
        )

    return {"input_sources": sources}


@app.get("/api/v1/input-sources/{source_type}/sources")
async def list_input_sources(source_type: str, timeout_ms: int = Query(5000)):
    """List discovered sources for a given input source type."""
    source_class = _resolve_input_source_class(source_type)

    # Return cached results if still fresh
    cached = _source_discovery_cache.get(source_type)
    if cached is not None:
        ts, sources = cached
        if time.monotonic() - ts < _SOURCE_DISCOVERY_TTL:
            return {"source_type": source_type, "sources": sources}

    # Syphon discovery requires pumping NSRunLoop on the main thread.
    # async handlers run on the event-loop (main) thread, so pump here.
    if source_type == "syphon" and sys.platform == "darwin":
        try:
            from .syphon.receiver import drain_notifications

            drain_notifications(0.1)
        except Exception:
            logger.debug("Failed to pump Syphon run loop", exc_info=True)

    event_loop = asyncio.get_event_loop()

    def _discover():
        instance = source_class()
        try:
            discovered = instance.list_sources(timeout_ms=timeout_ms)
            return [
                {
                    "name": s.name,
                    "identifier": s.identifier,
                    "metadata": s.metadata,
                }
                for s in discovered
            ]
        finally:
            instance.close()

    try:
        sources = await event_loop.run_in_executor(None, _discover)
        _source_discovery_cache[source_type] = (time.monotonic(), sources)
        return {"source_type": source_type, "sources": sources}
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error listing sources for '{source_type}': {e}")
        raise HTTPException(status_code=500, detail=str(e)) from e


@app.get("/api/v1/input-sources/{source_type}/sources/{identifier:path}/resolution")
def get_input_source_resolution(
    source_type: str, identifier: str, timeout_ms: int = Query(5000)
):
    """Probe the native resolution of a specific input source."""
    source_class = _resolve_input_source_class(source_type)

    try:
        instance = source_class()
        try:
            resolution = instance.get_source_resolution(
                identifier, timeout_ms=timeout_ms
            )
            if resolution is None:
                raise HTTPException(
                    status_code=408,
                    detail=f"Could not determine resolution for '{identifier}' "
                    f"within {timeout_ms}ms.",
                )
            width, height = resolution
            return {"width": width, "height": height}
        finally:
            instance.close()
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error probing resolution for '{source_type}/{identifier}': {e}")
        raise HTTPException(status_code=500, detail=str(e)) from e


@app.get("/api/v1/input-sources/{source_type}/sources/{identifier:path}/stream")
async def stream_input_source_preview(
    source_type: str,
    identifier: str,
    fps: int = Query(2, ge=1, le=30),
    flip_vertical: bool = Query(False),
):
    """MJPEG stream of an input source for live preview."""
    source_class = _resolve_input_source_class(source_type)

    async def _generate():
        from PIL import Image

        max_w = 320
        interval = 1.0 / fps
        receiver = None
        loop = asyncio.get_event_loop()

        try:
            # Create persistent receiver
            receiver = source_class()
            if source_type == "syphon" and hasattr(receiver, "set_flip_vertical"):
                receiver.set_flip_vertical(flip_vertical)
            connected = await loop.run_in_executor(None, receiver.connect, identifier)
            if not connected:
                logger.warning(f"Preview stream: could not connect to '{identifier}'")
                return

            while True:
                frame = await loop.run_in_executor(None, receiver.receive_frame, 200)

                if frame is not None:
                    h, w = frame.shape[:2]
                    if w > max_w:
                        scale = max_w / w
                        new_w = max_w
                        new_h = int(h * scale)
                        img = Image.fromarray(frame).resize(
                            (new_w, new_h), Image.NEAREST
                        )
                    else:
                        img = Image.fromarray(frame)

                    buf = io.BytesIO()
                    img.save(buf, format="JPEG", quality=70)
                    jpeg = buf.getvalue()

                    yield (
                        b"--frame\r\n"
                        b"Content-Type: image/jpeg\r\n"
                        b"Content-Length: " + str(len(jpeg)).encode() + b"\r\n"
                        b"\r\n" + jpeg + b"\r\n"
                    )

                await asyncio.sleep(interval)
        except asyncio.CancelledError:
            pass
        finally:
            if receiver is not None:
                receiver.close()

    return StreamingResponse(
        _generate(),
        media_type="multipart/x-mixed-replace; boundary=frame",
    )


@app.get("/api/v1/hardware/info", response_model=HardwareInfoResponse)
async def get_hardware_info(
    cloud_manager: ScopeCloudBackend = Depends(get_scope_cloud),
):
    """Get hardware information including available VRAM and Spout availability.

    In cloud mode (when connected to cloud), this proxies the request to the
    cloud-hosted scope backend to get the cloud GPU's hardware info.
    """
    try:
        #  If connected to cloud, proxy the request to get cloud's hardware info
        if cloud_manager.is_connected:
            return await get_hardware_info_from_cloud(
                cloud_manager,
                get_spout_availability(),
                get_ndi_availability(),
                get_syphon_availability(),
            )

        # Local mode: get local hardware info
        import torch  # Lazy import to avoid loading at CLI startup

        vram_gb = None

        if torch.cuda.is_available():
            # Get total VRAM from the first GPU (in bytes), convert to GB
            _, total_mem = torch.cuda.mem_get_info(0)
            vram_gb = total_mem / (1024**3)

        return HardwareInfoResponse(
            vram_gb=vram_gb,
            spout=get_spout_availability(),
            ndi=get_ndi_availability(),
            syphon=get_syphon_availability(),
        )
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error getting hardware info: {e}")
        raise HTTPException(status_code=500, detail=str(e)) from e


@app.get("/api/v1/keys", response_model=ApiKeysListResponse)
async def list_api_keys():
    """List all registered API key services with their status."""
    import os

    from huggingface_hub import get_token

    from .models_config import get_civitai_token, get_civitai_token_source

    # HuggingFace
    hf_token = get_token()
    hf_env_var_set = bool(os.environ.get("HF_TOKEN"))
    if hf_token:
        hf_source = "env_var" if hf_env_var_set else "stored"
    else:
        hf_source = None

    hf_key = ApiKeyInfo(
        id="huggingface",
        name="HuggingFace",
        description="Required for downloading gated models",
        is_set=hf_token is not None,
        source=hf_source,
        env_var="HF_TOKEN",
        key_url="https://huggingface.co/settings/tokens",
    )

    # CivitAI
    civitai_token = get_civitai_token()

    civitai_key = ApiKeyInfo(
        id="civitai",
        name="CivitAI",
        description="Required for downloading LoRAs from CivitAI",
        is_set=civitai_token is not None,
        source=get_civitai_token_source(),
        env_var="CIVITAI_API_TOKEN",
        key_url="https://civitai.com/user/account",
    )

    return ApiKeysListResponse(keys=[hf_key, civitai_key])


@app.put("/api/v1/keys/{service_id}", response_model=ApiKeySetResponse)
async def set_api_key(service_id: str, request: ApiKeySetRequest):
    """Set/save an API key for a service."""
    import os

    from .models_config import CIVITAI_TOKEN_ENV_VAR, set_civitai_token

    if service_id == "huggingface":
        if os.environ.get("HF_TOKEN"):
            raise HTTPException(
                status_code=409,
                detail="HF_TOKEN environment variable is already set. Remove it to manage this key from the UI.",
            )

        from huggingface_hub import login

        login(token=request.value, add_to_git_credential=False)
        return ApiKeySetResponse(success=True, message="HuggingFace token saved")

    elif service_id == "civitai":
        if os.environ.get(CIVITAI_TOKEN_ENV_VAR):
            raise HTTPException(
                status_code=409,
                detail="CIVITAI_API_TOKEN environment variable is already set. Remove it to manage this key from the UI.",
            )

        set_civitai_token(request.value)
        return ApiKeySetResponse(success=True, message="CivitAI token saved")

    else:
        raise HTTPException(status_code=404, detail=f"Unknown service: {service_id}")


@app.delete("/api/v1/keys/{service_id}", response_model=ApiKeyDeleteResponse)
async def delete_api_key(service_id: str):
    """Remove a stored API key for a service."""
    import os

    from .models_config import (
        clear_civitai_token,
        get_civitai_token_source,
    )

    if service_id == "huggingface":
        env_var_set = bool(os.environ.get("HF_TOKEN"))
        if env_var_set:
            raise HTTPException(
                status_code=409,
                detail="Cannot remove token set via HF_TOKEN environment variable. Unset the environment variable instead.",
            )

        from huggingface_hub import logout

        logout()
        return ApiKeyDeleteResponse(success=True, message="HuggingFace token removed")

    elif service_id == "civitai":
        source = get_civitai_token_source()
        if source == "env_var":
            raise HTTPException(
                status_code=409,
                detail="Cannot remove token set via CIVITAI_API_TOKEN environment variable. Unset the environment variable instead.",
            )
        if source != "stored":
            raise HTTPException(status_code=404, detail="No CivitAI token to remove")

        clear_civitai_token()
        return ApiKeyDeleteResponse(success=True, message="CivitAI token removed")

    else:
        raise HTTPException(status_code=404, detail=f"Unknown service: {service_id}")


@app.get("/api/v1/logs/current")
async def get_current_logs():
    """Get the most recent application log file for bug reporting."""
    try:
        log_file_path = get_most_recent_log_file()

        if log_file_path is None or not log_file_path.exists():
            raise HTTPException(
                status_code=404,
                detail="Log file not found. The application may not have logged anything yet.",
            )

        # Read the entire file into memory to avoid Content-Length issues
        # with actively written log files.
        # Use errors='replace' to handle non-UTF-8 bytes gracefully (e.g., Windows-1252
        # characters from subprocess output or exception messages on Windows).
        log_content = log_file_path.read_text(encoding="utf-8", errors="replace")

        # Return as a text response with proper headers for download
        return Response(
            content=log_content,
            media_type="text/plain",
            headers={
                "Content-Disposition": f'attachment; filename="{log_file_path.name.replace(".log", ".txt")}"'
            },
        )
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error retrieving log file: {e}")
        raise HTTPException(status_code=500, detail=str(e)) from e


@app.get("/api/v1/logs/tail")
async def tail_logs(
    lines: int = Query(default=200, ge=1, le=1000),
    since_offset: int = Query(default=0, ge=0),
):
    """Get recent log lines, optionally only new content since a byte offset.

    Returns JSON with "lines" (list of strings) and "offset" (byte offset for next poll).
    """
    log_file_path = get_most_recent_log_file()
    if log_file_path is None or not log_file_path.exists():
        return {"lines": [], "offset": 0}

    file_size = log_file_path.stat().st_size

    if since_offset > 0 and since_offset >= file_size:
        return {"lines": [], "offset": file_size}

    if since_offset > 0 and since_offset < file_size:
        # Read only new content since last offset
        with open(log_file_path, encoding="utf-8", errors="replace") as f:
            f.seek(since_offset)
            new_content = f.read()
            new_lines = [ln for ln in new_content.splitlines() if ln.strip()]
            return {"lines": new_lines[-lines:], "offset": file_size}

    # No offset or offset beyond file: return last N lines
    with open(log_file_path, encoding="utf-8", errors="replace") as f:
        all_lines = f.readlines()
        tail = [ln.rstrip() for ln in all_lines[-lines:] if ln.strip()]
        return {"lines": tail, "offset": file_size}


# Plugin Management API Endpoints

SOURCE_KIND = "source"


def _cloud_is_connected(cloud_manager: ScopeCloudBackend | None) -> bool:
    """Return True iff *cloud_manager* is set and currently connected."""
    return cloud_manager is not None and getattr(cloud_manager, "is_connected", False)


async def _is_locally_installed(name: str) -> bool:
    """Return True if *name* is currently installed in the local venv.

    Used in cloud mode to decide whether uninstall/reload should target
    the local instance (for source-kind plugins) or be proxied to cloud.
    """
    from scope.core.plugins import get_plugin_manager

    try:
        plugins = await get_plugin_manager().list_plugins_async(skip_update_check=True)
    except Exception as e:
        logger.warning(f"Local plugin listing failed during routing decision: {e}")
        return False
    return any(p.get("name") == name for p in plugins)


def _convert_plugin_dict_to_info(plugin_dict: dict) -> "PluginInfo":
    """Convert a plugin dictionary from PluginManager to PluginInfo schema."""
    from .schema import PluginInfo, PluginPipelineInfo, PluginSource

    pipelines = [
        PluginPipelineInfo(
            pipeline_id=p["pipeline_id"],
            pipeline_name=p["pipeline_name"],
        )
        for p in plugin_dict.get("pipelines", [])
    ]

    return PluginInfo(
        name=plugin_dict["name"],
        version=plugin_dict.get("version"),
        author=plugin_dict.get("author"),
        description=plugin_dict.get("description"),
        source=PluginSource(plugin_dict.get("source", "pypi")),
        editable=plugin_dict.get("editable", False),
        editable_path=plugin_dict.get("editable_path"),
        pipelines=pipelines,
        latest_version=plugin_dict.get("latest_version"),
        update_available=plugin_dict.get("update_available"),
        package_spec=plugin_dict.get("package_spec"),
        bundled=plugin_dict.get("bundled", False),
        kind=plugin_dict.get("kind"),
        origin=plugin_dict.get("origin"),
    )


@app.get("/api/v1/plugins")
async def list_plugins(
    http_request: Request,
    cloud_manager: ScopeCloudBackend = Depends(get_scope_cloud),
):
    """List all installed plugins with metadata.

    In cloud mode the response merges plugins from the cloud-hosted scope
    backend with any source-kind plugins installed locally. Each entry is
    tagged with ``origin = "local" | "cloud"`` so the UI can distinguish
    where each plugin runs.
    """
    from scope.core.plugins import get_plugin_manager

    from .schema import FailedPluginInfoSchema, PluginListResponse

    global _plugins_list_cache
    if _plugins_list_cache is not None:
        return _plugins_list_cache

    try:
        plugin_manager = get_plugin_manager()
        cloud_connected = _cloud_is_connected(cloud_manager)

        if cloud_connected:
            local_result, cloud_response = await asyncio.gather(
                plugin_manager.list_plugins_async(),
                _fetch_cloud_plugin_list(cloud_manager),
                return_exceptions=True,
            )
            if isinstance(local_result, BaseException):
                logger.warning(
                    f"Local plugin listing failed in cloud mode; "
                    f"returning cloud plugins only: {local_result}"
                )
                local_plugins_data = []
            else:
                local_plugins_data = local_result
            if isinstance(cloud_response, BaseException):
                cloud_response = None
        else:
            local_plugins_data = await plugin_manager.list_plugins_async()
            cloud_response = None

        local_failed = [
            FailedPluginInfoSchema(
                package_name=f.package_name,
                entry_point_name=f.entry_point_name,
                error_type=f.error_type,
                error_message=f.error_message,
            )
            for f in plugin_manager.get_failed_plugins()
        ]

        if cloud_connected:
            # Only source-kind local plugins surface alongside cloud plugins;
            # other local plugins are cloud-targeted and live under the cloud.
            local_source_plugins = [
                {**p, "origin": "local"}
                for p in local_plugins_data
                if p.get("kind") == SOURCE_KIND
            ]
            cloud_plugins_raw = [
                {**p, "origin": "cloud"}
                for p in (cloud_response or {}).get("plugins", [])
            ]
            combined = [
                _convert_plugin_dict_to_info(p)
                for p in (*local_source_plugins, *cloud_plugins_raw)
            ]
            cloud_failed = [
                FailedPluginInfoSchema(**f)
                for f in (cloud_response or {}).get("failed_plugins", [])
            ]
            response = PluginListResponse(
                plugins=combined,
                total=len(combined),
                failed_plugins=[*local_failed, *cloud_failed],
            )
            _plugins_list_cache = response
            return response

        plugins = [_convert_plugin_dict_to_info(p) for p in local_plugins_data]
        response = PluginListResponse(
            plugins=plugins, total=len(plugins), failed_plugins=local_failed
        )
        _plugins_list_cache = response
        return response
    except Exception as e:
        logger.error(f"Error listing plugins: {e}")
        raise HTTPException(status_code=500, detail=str(e)) from e


async def _fetch_cloud_plugin_list(
    cloud_manager: ScopeCloudBackend,
) -> dict[str, Any]:
    """Fetch plugin list from cloud; return ``{"plugins": [], "failed_plugins": []}``
    on failure so callers can merge unconditionally."""
    try:
        response = await proxy_with_body(
            cloud_manager,
            method="GET",
            path="/api/v1/plugins",
            body=None,
            timeout=30.0,
        )
    except HTTPException as e:
        logger.warning(f"Failed to fetch cloud plugin list: {e.detail}")
        return {"plugins": [], "failed_plugins": []}
    return (
        response
        if isinstance(response, dict)
        else {"plugins": [], "failed_plugins": []}
    )


@app.post("/api/v1/plugins")
async def install_plugin(
    http_request: Request,
    pipeline_manager: "PipelineManager" = Depends(get_pipeline_manager),
    cloud_manager: ScopeCloudBackend = Depends(get_scope_cloud),
):
    """Install a plugin from PyPI, git URL, or local path.

    In cloud mode the request is proxied to the cloud-hosted scope backend
    EXCEPT for source-kind plugins (declared via ``__scope_kind__`` in the
    package's ``__init__.py``), which are always installed on the local
    machine because their input frames originate locally.
    """
    from scope.core.plugins import (
        PluginDependencyError,
        PluginInstallError,
        PluginNameCollisionError,
        get_plugin_manager,
        probe_plugin_kind,
    )

    from .schema import PluginInstallRequest, PluginInstallResponse

    body = await http_request.json()
    install_request = PluginInstallRequest(**body)

    if _cloud_is_connected(cloud_manager):
        kind = probe_plugin_kind(install_request.package)
        if kind != SOURCE_KIND:
            logger.info(
                f"Proxying plugin install to cloud "
                f"(kind={kind!r}): {install_request.package}"
            )
            return await proxy_with_body(
                cloud_manager,
                method="POST",
                path="/api/v1/plugins",
                body=body,
                timeout=60.0,
            )
        logger.info(
            f"Installing source-kind plugin locally despite cloud mode: "
            f"{install_request.package}"
        )

    logger.info(f"Installing plugin: {install_request.package}")
    try:
        plugin_manager = get_plugin_manager()

        result = await plugin_manager.install_plugin_async(
            package=install_request.package,
            editable=install_request.editable,
            upgrade=install_request.upgrade,
            pre=install_request.pre,
            force=install_request.force,
        )

        plugin_info = None
        plugin_name = install_request.package
        if result.get("plugin"):
            plugin_info = _convert_plugin_dict_to_info(result["plugin"])
            plugin_name = plugin_info.name

        srv = get_dmx_server()
        if srv is not None:
            srv.invalidate_known_paths_cache()

        logger.info(f"Plugin installed: {plugin_name}")
        _invalidate_plugin_caches()
        return PluginInstallResponse(
            success=result["success"],
            message=result["message"],
            plugin=plugin_info,
        )

    except PluginDependencyError as e:
        logger.error(
            f"Plugin install failed (dependency error): {install_request.package} - {e}"
        )
        raise HTTPException(
            status_code=422,
            detail=(
                f"Failed to install {install_request.package}: "
                "dependency conflict. Check server logs for details."
            ),
        ) from e
    except PluginNameCollisionError as e:
        logger.error(
            f"Plugin install failed (name collision): {install_request.package} - {e}"
        )
        raise HTTPException(
            status_code=409,
            detail=f"Plugin name collision: {install_request.package}",
        ) from e
    except PluginInstallError as e:
        logger.error(f"Plugin install failed: {install_request.package} - {e}")
        raise HTTPException(
            status_code=500,
            detail=(
                f"Failed to install {install_request.package}. "
                "Check server logs for details."
            ),
        ) from e
    except Exception as e:
        logger.error(f"Plugin install failed: {install_request.package} - {e}")
        raise HTTPException(
            status_code=500,
            detail=(
                f"Failed to install {install_request.package}. "
                "Check server logs for details."
            ),
        ) from e


@app.delete("/api/v1/plugins/{name}")
async def uninstall_plugin(
    name: str,
    http_request: Request,
    pipeline_manager: "PipelineManager" = Depends(get_pipeline_manager),
    cloud_manager: ScopeCloudBackend = Depends(get_scope_cloud),
):
    """Uninstall a plugin, cleaning up loaded pipelines.

    In cloud mode the request is proxied to the cloud-hosted scope backend
    UNLESS the named plugin is installed locally (e.g. a source-kind
    plugin), in which case it is uninstalled from the local venv.
    """
    from scope.core.plugins import (
        PluginInstallError,
        PluginNotFoundError,
        get_plugin_manager,
    )

    from .schema import PluginUninstallResponse

    if _cloud_is_connected(cloud_manager) and not await _is_locally_installed(name):
        logger.info(f"Proxying plugin uninstall to cloud: {name}")
        return await proxy_with_body(
            cloud_manager,
            method="DELETE",
            path=f"/api/v1/plugins/{name}",
            body=None,
            timeout=60.0,
        )

    logger.info(f"Uninstalling plugin: {name}")
    try:
        plugin_manager = get_plugin_manager()

        result = await plugin_manager.uninstall_plugin_async(
            name=name,
            pipeline_manager=pipeline_manager,
        )

        srv = get_dmx_server()
        if srv is not None:
            srv.invalidate_known_paths_cache()

        logger.info(f"Plugin uninstalled: {name}")
        _invalidate_plugin_caches()
        return PluginUninstallResponse(
            success=result["success"],
            message=result["message"],
            unloaded_pipelines=result.get("unloaded_pipelines", []),
        )

    except PluginNotFoundError as e:
        logger.error(f"Plugin uninstall failed (not found): {name} - {e}")
        raise HTTPException(
            status_code=404,
            detail=f"Plugin '{name}' not found",
        ) from e
    except PluginInstallError as e:
        logger.error(f"Plugin uninstall failed: {name} - {e}")
        raise HTTPException(
            status_code=500,
            detail=f"Failed to uninstall {name}. Check server logs for details.",
        ) from e
    except Exception as e:
        logger.error(f"Plugin uninstall failed: {name} - {e}")
        raise HTTPException(
            status_code=500,
            detail=f"Failed to uninstall {name}. Check server logs for details.",
        ) from e


@app.post("/api/v1/plugins/{name}/reload")
async def reload_plugin(
    name: str,
    http_request: Request,
    pipeline_manager: "PipelineManager" = Depends(get_pipeline_manager),
    cloud_manager: ScopeCloudBackend = Depends(get_scope_cloud),
):
    """Reload an editable plugin for development (without server restart).

    In cloud mode the request is proxied to the cloud-hosted scope backend
    UNLESS the named plugin is installed locally, in which case it is
    reloaded in the local venv.
    """
    from scope.core.plugins import (
        PluginInUseError,
        PluginNotEditableError,
        PluginNotFoundError,
        get_plugin_manager,
    )

    from .schema import PluginReloadRequest, PluginReloadResponse

    # Parse request body
    body = await http_request.json()
    reload_request = PluginReloadRequest(**body)

    if _cloud_is_connected(cloud_manager) and not await _is_locally_installed(name):
        logger.info(f"Proxying plugin reload to cloud: {name}")
        return await proxy_with_body(
            cloud_manager,
            method="POST",
            path=f"/api/v1/plugins/{name}/reload",
            body=body,
            timeout=60.0,
        )

    try:
        plugin_manager = get_plugin_manager()

        result = await plugin_manager.reload_plugin_async(
            name=name,
            force=reload_request.force,
            pipeline_manager=pipeline_manager,
        )

        _invalidate_plugin_caches()

        srv = get_dmx_server()
        if srv is not None:
            srv.invalidate_known_paths_cache()

        return PluginReloadResponse(
            success=result["success"],
            message=result["message"],
            reloaded_pipelines=result.get("reloaded_pipelines", []),
            added_pipelines=result.get("added_pipelines", []),
            removed_pipelines=result.get("removed_pipelines", []),
        )

    except PluginNotFoundError as e:
        logger.error(f"Plugin reload failed (not found): {name} - {e}")
        raise HTTPException(
            status_code=404,
            detail=f"Plugin '{name}' not found",
        ) from e
    except PluginNotEditableError as e:
        logger.error(f"Plugin reload failed (not editable): {name} - {e}")
        raise HTTPException(
            status_code=400,
            detail=f"Plugin '{name}' is not installed in editable mode",
        ) from e
    except PluginInUseError as e:
        logger.error(f"Plugin reload failed (in use): {name} - {e}")
        raise HTTPException(
            status_code=409,
            detail={
                "message": f"Plugin '{name}' has loaded pipelines. Use force=true to unload them.",
                "loaded_pipelines": e.loaded_pipelines,
            },
        ) from e
    except Exception as e:
        logger.error(f"Plugin reload failed: {name} - {e}")
        raise HTTPException(
            status_code=500,
            detail=f"Failed to reload {name}. Check server logs for details.",
        ) from e


# =============================================================================
# Cloud Integration Endpoints
# =============================================================================


@app.post("/api/v1/cloud/connect", response_model=CloudStatusResponse)
async def connect_to_cloud(
    request: CloudConnectRequest,
    cloud_manager: ScopeCloudBackend = Depends(get_scope_cloud),
):
    """Connect to cloud for remote GPU inference.

    This establishes a WebSocket connection to the cloud runner,
    which stays open until disconnect is called. Once connected:
    - Pipeline loading is proxied to the cloud-hosted scope backend
    - Video from the browser flows through the backend to cloud
    - The cloud runner stays warm and ready for video processing

    Credentials can be provided in the request body or via CLI args:
    --cloud-app-id and --cloud-api-key (or SCOPE_CLOUD_APP_ID/SCOPE_CLOUD_API_KEY env vars).

    Note: The connection may take 1-2 minutes on cold start while the
    cloud runner initializes.

    Architecture:
        Browser → Backend (WebRTC) → cloud (WebRTC) → Backend → Browser
        Spout → Backend → cloud (WebRTC) → Backend → Spout/Browser
    """
    try:
        global scope_cloud
        # Use request body credentials if provided, otherwise fall back to CLI/env
        remote_url = request.remote_url or os.environ.get("SCOPE_REMOTE_SCOPE_URL")
        app_id = request.app_id or os.environ.get("SCOPE_CLOUD_APP_ID")
        api_key = request.api_key or os.environ.get("SCOPE_CLOUD_API_KEY")
        if remote_url:
            if remote_scope is None:
                raise RuntimeError("Remote Scope backend is not initialized")
            if cloud_manager is not remote_scope and getattr(
                cloud_manager, "is_connected", False
            ):
                await cloud_manager.disconnect()
            scope_cloud = remote_scope
            remote_api_key = request.api_key or os.environ.get(
                "SCOPE_REMOTE_SCOPE_API_KEY"
            )
            logger.info(
                f"Connecting to remote Scope backend (background): {remote_url}"
            )
            await remote_scope.connect_background(
                remote_url=remote_url,
                api_key=remote_api_key,
                user_id=request.user_id,
            )

            _invalidate_plugin_caches()

            return CloudStatusResponse(
                connected=False,
                connecting=True,
                webrtc_connected=False,
                app_id="remote-scope",
                backend="remote_scope",
                remote_url=remote_url,
                credentials_configured=bool(os.environ.get("SCOPE_REMOTE_SCOPE_URL")),
            )

        if not app_id and not api_key:
            raise HTTPException(
                status_code=400,
                detail="cloud credentials not configured. Use --cloud-app-id or --cloud-api-key CLI args, "
                "SCOPE_CLOUD_APP_ID/SCOPE_CLOUD_API_KEY environment variables, "
                "or provide remote_url for a self-hosted Scope server.",
            )
        if livepeer is None:
            raise RuntimeError("Livepeer backend is not initialized")
        if cloud_manager is not livepeer and getattr(
            cloud_manager, "is_connected", False
        ):
            await cloud_manager.disconnect()
        scope_cloud = livepeer

        logger.info(
            f"Connecting to cloud (background): {app_id} (user_id: {request.user_id})"
        )
        await livepeer.connect_background(app_id, api_key, request.user_id)

        # Invalidate cached pipeline schemas so that when the cloud connection
        # completes, subsequent requests either proxy to the cloud (returning
        # cloud pipelines) or rebuild from the local registry instead of
        # serving stale cached data from a previous local-only fetch.
        _invalidate_plugin_caches()

        credentials_configured = bool(os.environ.get("SCOPE_CLOUD_APP_ID"))
        return CloudStatusResponse(
            connected=False,
            connecting=True,
            webrtc_connected=False,
            app_id=app_id,
            backend="livepeer",
            credentials_configured=credentials_configured,
        )
    except Exception as e:
        logger.error(f"Error connecting to cloud: {e}")
        raise HTTPException(status_code=500, detail=str(e)) from e


@app.post("/api/v1/cloud/disconnect", response_model=CloudStatusResponse)
async def disconnect_from_cloud(
    cloud_manager: ScopeCloudBackend = Depends(get_scope_cloud),
):
    """Disconnect from cloud.

    This closes the WebSocket and WebRTC connections to cloud, returning
    to local GPU processing mode. Any in-progress operations will be interrupted.
    """
    try:
        global scope_cloud
        await cloud_manager.disconnect()
        scope_cloud = livepeer
        # Invalidate cached pipeline schemas so that post-disconnect requests
        # rebuild the list from the local registry instead of returning stale
        # cloud-era data.
        _invalidate_plugin_caches()
        credentials_configured = bool(
            os.environ.get("SCOPE_CLOUD_APP_ID")
            or os.environ.get("SCOPE_REMOTE_SCOPE_URL")
        )
        return CloudStatusResponse(
            connected=False,
            webrtc_connected=False,
            app_id=None,
            backend=None,
            remote_url=None,
            credentials_configured=credentials_configured,
        )
    except Exception as e:
        logger.error(f"Error disconnecting from cloud: {e}")
        raise HTTPException(status_code=500, detail=str(e)) from e


@app.get("/api/v1/cloud/status", response_model=CloudStatusResponse)
async def get_cloud_status(
    cloud_manager: ScopeCloudBackend = Depends(get_scope_cloud),
):
    """Get current cloud connection status."""
    status = cloud_manager.get_status()
    # Check if credentials are configured via CLI/env
    credentials_configured = bool(
        os.environ.get("SCOPE_CLOUD_APP_ID") or os.environ.get("SCOPE_REMOTE_SCOPE_URL")
    )
    return CloudStatusResponse(**status, credentials_configured=credentials_configured)


# ---------------------------------------------------------------------------
# Onboarding
# ---------------------------------------------------------------------------


def _get_onboarding_file() -> Path:
    return get_base_dir() / "onboarding.json"


class OnboardingStatusResponse(BaseModel):
    completed: bool
    inference_mode: str | None = None
    onboarding_style: str | None = None
    referral_source: str | None = None
    use_case: str | None = None


class OnboardingStatusUpdate(BaseModel):
    completed: bool | None = None
    inference_mode: str | None = None
    onboarding_style: str | None = None
    referral_source: str | None = None
    use_case: str | None = None


@app.get("/api/v1/onboarding/status", response_model=OnboardingStatusResponse)
async def get_onboarding_status():
    """Read onboarding completion state from onboarding.json."""
    onboarding_file = _get_onboarding_file()
    if onboarding_file.exists():
        try:
            data = json.loads(onboarding_file.read_text())
            return OnboardingStatusResponse(
                completed=data.get("completed", False),
                inference_mode=data.get("inference_mode"),
                onboarding_style=data.get("onboarding_style"),
                referral_source=data.get("referral_source"),
                use_case=data.get("use_case"),
            )
        except Exception:
            pass
    return OnboardingStatusResponse(completed=False)


@app.put("/api/v1/onboarding/status", response_model=OnboardingStatusResponse)
async def update_onboarding_status(body: OnboardingStatusUpdate):
    """Write onboarding completion state to onboarding.json."""
    onboarding_file = _get_onboarding_file()
    onboarding_file.parent.mkdir(parents=True, exist_ok=True)
    # Merge with existing data so we don't lose fields when partially updating
    existing: dict = {}
    if onboarding_file.exists():
        try:
            existing = json.loads(onboarding_file.read_text())
        except Exception:
            pass
    existing.update(body.model_dump(exclude_none=True))
    onboarding_file.write_text(json.dumps(existing))
    return OnboardingStatusResponse(
        completed=existing.get("completed", False),
        inference_mode=existing.get("inference_mode"),
        onboarding_style=existing.get("onboarding_style"),
        referral_source=existing.get("referral_source"),
        use_case=existing.get("use_case"),
    )


@app.get("/{path:path}")
async def serve_frontend(request: Request, path: str):
    """Serve the frontend for all non-API routes (fallback for client-side routing)."""
    frontend_dist = Path(__file__).parent.parent.parent.parent / "frontend" / "dist"

    # Only serve SPA if frontend dist exists (production mode)
    if not frontend_dist.exists():
        raise HTTPException(status_code=404, detail="Frontend not built")

    # Check if requesting a specific file that exists
    file_path = (frontend_dist / path).resolve()
    if not file_path.is_relative_to(frontend_dist.resolve()):
        raise HTTPException(status_code=403, detail="Access denied")
    if file_path.exists() and file_path.is_file():
        # Determine media type based on extension to fix MIME type issues on Windows
        file_extension = file_path.suffix.lower()
        media_types = {
            ".js": "application/javascript",
            ".mjs": "application/javascript",
            ".css": "text/css",
            ".html": "text/html",
            ".json": "application/json",
            ".png": "image/png",
            ".jpg": "image/jpeg",
            ".jpeg": "image/jpeg",
            ".gif": "image/gif",
            ".svg": "image/svg+xml",
            ".ico": "image/x-icon",
            ".woff": "font/woff",
            ".woff2": "font/woff2",
            ".ttf": "font/ttf",
            ".eot": "application/vnd.ms-fontobject",
        }
        media_type = media_types.get(file_extension)
        return FileResponse(file_path, media_type=media_type)

    # Fallback to index.html for SPA routing
    # This ensures clients like Electron alway fetch the latest HTML (which references hashed assets)
    index_file = frontend_dist / "index.html"
    if index_file.exists():
        return FileResponse(
            index_file,
            media_type="text/html",
            headers={
                "Cache-Control": "no-cache, no-store, must-revalidate",
                "Pragma": "no-cache",
                "Expires": "0",
            },
        )

    raise HTTPException(status_code=404, detail="Frontend index.html not found")


def open_browser_when_ready(host: str, port: int, server):
    """Open browser when server is ready, with fallback to URL logging."""
    # Wait for server to be ready
    while not getattr(server, "started", False):
        time.sleep(0.1)

    # Determine the URL to open
    url = (
        f"http://localhost:{port}"
        if host in ["0.0.0.0", "127.0.0.1"]
        else f"http://{host}:{port}"
    )

    try:
        success = webbrowser.open(url)
        if success:
            logger.info(f"🌐 Opened browser at {url}")
    except Exception:
        success = False

    if not success:
        logger.info(f"🌐 UI is available at: {url}")


def run_server(reload: bool, host: str, port: int, no_browser: bool):
    """Run the Daydream Scope server."""
    _configure_logging()

    from scope.core.pipelines.registry import (
        PipelineRegistry,  # noqa: F401 - imported for side effects (registry initialization)
    )

    # Propagate host/port so lifespan can read them (e.g. for OSC UDP bind)
    os.environ["SCOPE_HOST"] = host
    os.environ["SCOPE_PORT"] = str(port)

    # Configure static file serving
    configure_static_files()

    # Check if we're in production mode (frontend dist exists)
    frontend_dist = Path(__file__).parent.parent.parent / "frontend" / "dist"
    is_production = frontend_dist.exists()

    if is_production:
        # Create server instance for production mode
        config = uvicorn.Config(
            "scope.server.app:app",
            host=host,
            port=port,
            reload=reload,
            log_config=None,  # Use our logging config, don't override it
            timeout_graceful_shutdown=1,
        )
        server = uvicorn.Server(config)

        # Start browser opening thread (unless disabled)
        if not no_browser:
            browser_thread = threading.Thread(
                target=open_browser_when_ready,
                args=(host, port, server),
                daemon=True,
            )
            browser_thread.start()
        else:
            logger.info("main: Skipping browser auto-launch due to --no-browser")

        # Run the server
        try:
            server.run()
        except KeyboardInterrupt:
            pass  # Clean shutdown on Ctrl+C
    else:
        # Development mode - just run normally
        uvicorn.run(
            "scope.server.app:app",
            host=host,
            port=port,
            reload=reload,
            log_config=None,  # Use our logging config, don't override it
            timeout_graceful_shutdown=1,
        )


@click.group(invoke_without_command=True)
@click.option("--version", is_flag=True, help="Show version information and exit")
@click.option(
    "--reload", is_flag=True, help="Enable auto-reload for development (default: False)"
)
@click.option(
    "--host", default="127.0.0.1", help="Host to bind to (default: 127.0.0.1)"
)
@click.option("--port", default=8000, help="Port to bind to (default: 8000)")
@click.option(
    "-N",
    "--no-browser",
    is_flag=True,
    help="Do not automatically open a browser window after the server starts",
)
@click.option(
    "--cloud-app-id",
    default=None,
    envvar="SCOPE_CLOUD_APP_ID",
    help="Cloud app ID for Livepeer mode (e.g., 'daydream/scope-livepeer--prod/ws')",
)
@click.option(
    "--cloud-api-key",
    default=None,
    envvar="SCOPE_CLOUD_API_KEY",
    help="Cloud API key for cloud mode",
)
@click.option(
    "--remote-scope-url",
    default=None,
    envvar="SCOPE_REMOTE_SCOPE_URL",
    help="Self-hosted remote Scope server URL to use as the compute backend",
)
@click.option(
    "--mcp",
    is_flag=True,
    help="Run as an MCP (Model Context Protocol) server over stdio instead of the HTTP server. "
    "Connects to a running Scope instance on --port.",
)
@click.pass_context
def main(
    ctx,
    version: bool,
    reload: bool,
    host: str,
    port: int,
    no_browser: bool,
    cloud_app_id: str | None,
    cloud_api_key: str | None,
    remote_scope_url: str | None,
    mcp: bool,
):
    # Handle version flag
    if version:
        print_version_info()
        sys.exit(0)

    # MCP mode: run the MCP stdio server instead of the HTTP server
    if mcp:
        import click.core

        from .mcp_server import run_mcp_server

        # Only pre-connect if --port was explicitly provided on the command line
        source = ctx.get_parameter_source("port")
        explicit_port = (
            port if source == click.core.ParameterSource.COMMANDLINE else None
        )
        run_mcp_server(port=explicit_port)
        return

    # Store cloud credentials in environment for app access
    if cloud_app_id:
        os.environ["SCOPE_CLOUD_APP_ID"] = cloud_app_id
    if cloud_api_key:
        os.environ["SCOPE_CLOUD_API_KEY"] = cloud_api_key
    if remote_scope_url:
        os.environ["SCOPE_REMOTE_SCOPE_URL"] = remote_scope_url

    # If no subcommand was invoked, run the server
    if ctx.invoked_subcommand is None:
        run_server(reload, host, port, no_browser)


@main.command()
def plugins():
    """List all installed plugins."""
    import asyncio

    from scope.core.plugins.manager import get_plugin_manager

    @suppress_init_output
    def _list_plugins():
        pm = get_plugin_manager()
        pm.load_plugins()
        return asyncio.run(pm.list_plugins_async())

    plugin_list = _list_plugins()

    if not plugin_list:
        click.echo("No plugins installed.")
        return

    click.echo(f"{len(plugin_list)} plugin(s) installed:\n")

    for plugin in plugin_list:
        name = plugin["name"]
        version = plugin.get("version", "unknown")
        source = plugin.get("source", "unknown")
        pipelines = plugin.get("pipelines", [])

        click.echo(f"  {name} ({version})")
        click.echo(f"    Source: {source}")
        if pipelines:
            pipeline_ids = [p["pipeline_id"] for p in pipelines]
            click.echo(f"    Pipelines: {', '.join(pipeline_ids)}")


@main.command()
def pipelines():
    """List all available pipelines."""

    @suppress_init_output
    def _load_pipelines():
        from scope.core.pipelines.registry import PipelineRegistry

        return PipelineRegistry.list_pipelines()

    all_pipelines = _load_pipelines()

    if not all_pipelines:
        click.echo("No pipelines available.")
        return

    click.echo(f"{len(all_pipelines)} pipeline(s) available:\n")

    # List all pipelines
    for pipeline_id in all_pipelines:
        click.echo(f"  • {pipeline_id}")


@main.command()
@click.argument("package", required=False)
@click.option("--upgrade", is_flag=True, help="Upgrade package to latest version")
@click.option(
    "-e", "--editable", help="Install a project in editable mode from this path"
)
@click.option(
    "--pre", is_flag=True, help="Include pre-release and development versions"
)
@click.option("--force", is_flag=True, help="Skip dependency validation")
def install(package, upgrade, editable, pre, force):
    """Install a plugin."""
    import asyncio

    from scope.core.plugins.manager import (
        PluginDependencyError,
        PluginInstallError,
        PluginNameCollisionError,
        get_plugin_manager,
    )

    if not package and not editable:
        click.echo("Error: Must specify a package or use -e/--editable", err=True)
        sys.exit(1)

    # Determine what to install
    install_package = editable if editable else package
    is_editable = bool(editable)

    @suppress_init_output
    def _install():
        pm = get_plugin_manager()
        return asyncio.run(
            pm.install_plugin_async(
                package=install_package,
                editable=is_editable,
                upgrade=upgrade,
                pre=pre,
                force=force,
            )
        )

    try:
        result = _install()
        click.echo(result["message"])
    except PluginDependencyError as e:
        click.echo(f"Dependency error: {e}", err=True)
        click.echo("\nUse --force to install anyway (may break environment)", err=True)
        sys.exit(1)
    except PluginNameCollisionError as e:
        click.echo(f"Name collision: {e}", err=True)
        sys.exit(1)
    except PluginInstallError as e:
        click.echo(f"Installation failed: {e}", err=True)
        sys.exit(1)


@main.command()
@click.argument("name", required=True)
def uninstall(name):
    """Uninstall a plugin."""
    import asyncio

    from scope.core.plugins.manager import (
        PluginInstallError,
        PluginNotFoundError,
        get_plugin_manager,
    )

    @suppress_init_output
    def _uninstall():
        pm = get_plugin_manager()
        pm.load_plugins()
        return asyncio.run(pm.uninstall_plugin_async(name=name))

    try:
        result = _uninstall()
        click.echo(result["message"])
        if result.get("unloaded_pipelines"):
            click.echo(f"Unloaded pipelines: {', '.join(result['unloaded_pipelines'])}")
    except PluginNotFoundError as e:
        click.echo(f"Error: {e}", err=True)
        sys.exit(1)
    except PluginInstallError as e:
        click.echo(f"Uninstall failed: {e}", err=True)
        sys.exit(1)


if __name__ == "__main__":
    main()
