from __future__ import annotations

from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from .livepeer import LivepeerConnection
    from .remote_scope import RemoteScopeConnection

    type ScopeCloudBackend = LivepeerConnection | RemoteScopeConnection
else:
    ScopeCloudBackend = Any
