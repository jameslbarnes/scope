"""Scope plugin entry point for Cue nodes."""

from scope.core.plugins import hookimpl


@hookimpl
def register_nodes(register):
    from .nodes.cue_session import CueSessionNode

    register(CueSessionNode)
