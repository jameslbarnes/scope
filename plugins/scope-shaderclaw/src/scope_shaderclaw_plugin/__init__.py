"""ShaderClaw integration plugin for Daydream Scope."""

__scope_kind__ = "source"

from .pipelines.pipeline import ShaderClawPipeline

__all__ = ["ShaderClawPipeline"]
