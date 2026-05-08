"""TURN relay controls for aiortc connections."""

from __future__ import annotations

import logging
import os
from typing import Any
from urllib.parse import parse_qs, urlparse

from aiortc import RTCIceServer

logger = logging.getLogger(__name__)

FALSE_VALUES = {"0", "false", "no", "off", ""}


def env_flag(name: str, default: str = "0") -> bool:
    return os.getenv(name, default).strip().lower() not in FALSE_VALUES


def prefer_turn_relay_ice_servers(
    ice_servers: list[RTCIceServer],
    *,
    relay_only: bool,
    label: str,
) -> list[RTCIceServer]:
    """Prefer TCP/TLS TURN URLs and optionally remove non-relay servers.

    aiortc/aioice only uses the first TURN URI it sees, so Twilio's URL order
    matters. Put TCP/TLS TURN entries first so lossy UDP paths are avoided.
    """

    turn_servers: list[tuple[int, int, RTCIceServer]] = []
    other_servers: list[tuple[int, RTCIceServer]] = []
    ordinal = 0

    for server in ice_servers:
        urls = _as_url_list(server.urls)
        for url in urls:
            ordinal += 1
            if _is_turn_url(url):
                turn_servers.append(
                    (
                        _turn_url_rank(url),
                        ordinal,
                        RTCIceServer(
                            urls=[url],
                            username=server.username,
                            credential=server.credential,
                            credentialType=server.credentialType,
                        ),
                    )
                )
            elif not relay_only:
                other_servers.append(
                    (
                        ordinal,
                        RTCIceServer(
                            urls=[url],
                            username=server.username,
                            credential=server.credential,
                            credentialType=server.credentialType,
                        ),
                    )
                )

    if not turn_servers:
        if relay_only:
            logger.warning(
                "TURN relay-only requested for %s, but no TURN ICE servers are configured",
                label,
            )
            return []
        return ice_servers

    ordered = [server for _, _, server in sorted(turn_servers, key=lambda item: item[:2])]
    if not relay_only:
        ordered.extend(server for _, server in sorted(other_servers, key=lambda item: item[0]))

    logger.info(
        "Using %s ICE server(s) for %s: %s",
        len(ordered),
        label,
        summarize_ice_servers(ordered),
    )
    return ordered


def force_turn_relay_transport_policy(pc: Any, label: str) -> None:
    """Force aiortc's private aioice connections to gather relay candidates only."""

    try:
        from aioice.ice import TransportPolicy
    except Exception as exc:
        logger.warning("Could not import aioice relay policy for %s: %s", label, exc)
        return

    changed = 0
    for ice_transport in _ice_transports(pc):
        gatherer = getattr(ice_transport, "iceGatherer", None)
        connection = getattr(gatherer, "_connection", None)
        if connection is None:
            continue
        connection._transport_policy = TransportPolicy.RELAY
        changed += 1

    if changed:
        logger.info("Forced TURN relay ICE policy on %s transport(s): %s", changed, label)
    else:
        logger.warning("TURN relay policy requested, but no ICE transports exist yet: %s", label)


def log_selected_ice_candidate_pairs(pc: Any, label: str) -> None:
    """Log selected ICE candidate pair types without exposing TURN credentials."""

    found = False
    for index, ice_transport in enumerate(_ice_transports(pc)):
        gatherer = getattr(ice_transport, "iceGatherer", None)
        connection = getattr(gatherer, "_connection", None)
        nominated = getattr(connection, "_nominated", {}) if connection else {}
        for component, pair in sorted(nominated.items()):
            found = True
            logger.info(
                "Selected ICE pair for %s transport=%s component=%s local=%s remote=%s",
                label,
                index,
                component,
                _candidate_summary(pair.local_candidate),
                _candidate_summary(pair.remote_candidate),
            )
    if not found:
        logger.info("No selected ICE candidate pair available yet for %s", label)


def summarize_ice_servers(ice_servers: list[RTCIceServer]) -> list[dict[str, Any]]:
    return [
        {
            "urls": _as_url_list(server.urls),
            "username": bool(server.username),
            "credential": bool(server.credential),
        }
        for server in ice_servers
    ]


def _ice_transports(pc: Any) -> list[Any]:
    return list(getattr(pc, "_RTCPeerConnection__iceTransports", []) or [])


def _as_url_list(urls: str | list[str]) -> list[str]:
    if isinstance(urls, str):
        return [urls]
    return list(urls)


def _is_turn_url(url: str) -> bool:
    return urlparse(url).scheme.lower() in {"turn", "turns"}


def _turn_url_rank(url: str) -> int:
    parsed = urlparse(url)
    scheme = parsed.scheme.lower()
    query = parse_qs(parsed.query)
    transport = (query.get("transport") or ["tcp" if scheme == "turns" else "udp"])[0]
    transport = transport.lower()
    address = parsed.netloc or parsed.path

    if scheme == "turns" and transport == "tcp":
        return 0
    if scheme == "turn" and transport == "tcp" and ":443" in address:
        return 1
    if scheme == "turn" and transport == "tcp":
        return 2
    return 3


def _candidate_summary(candidate: Any) -> str:
    candidate_type = getattr(candidate, "type", "unknown")
    transport = getattr(candidate, "transport", "unknown")
    host = getattr(candidate, "host", None)
    port = getattr(candidate, "port", None)
    if host is None or port is None:
        return f"{candidate_type}/{transport}"
    return f"{candidate_type}/{transport}@{host}:{port}"
