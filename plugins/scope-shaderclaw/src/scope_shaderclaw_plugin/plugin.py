"""Scope plugin entry point for ShaderClaw pipelines."""

from scope.core.plugins import hookimpl


@hookimpl
def register_pipelines(register):
    from .pipelines.pipeline import ShaderClawPipeline

    register(ShaderClawPipeline)
