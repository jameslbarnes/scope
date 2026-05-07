"""Schema for the ShaderClaw3 native shader source pipeline."""

import json
import os
from pathlib import Path
from typing import Any

from pydantic import Field

from scope.core.pipelines.base_schema import (
    BasePipelineConfig,
    ModeDefaults,
    ui_field_config,
)


def _shader_manifest_paths() -> list[Path]:
    paths: list[Path] = []
    for env_name in (
        "SCOPE_SHADERCLAW_SHADERS_DIR",
        "SHADERCLAW_SHADERS_DIR",
        "SCOPE_SHADERCLAW_REPO",
        "SHADERCLAW_REPO",
    ):
        if value := os.getenv(env_name):
            path = Path(value)
            paths.append(path / "manifest.json" if path.name == "shaders" else path / "shaders" / "manifest.json")
    paths.extend(
        [
            Path.cwd().parent / "shader-claw3" / "shaders" / "manifest.json",
            Path.home() / "shader-claw3" / "shaders" / "manifest.json",
        ]
    )
    return paths


def _shader_title_options() -> list[str]:
    for path in _shader_manifest_paths():
        try:
            manifest = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            continue
        if not isinstance(manifest, list):
            continue
        titles = [
            str(item.get("title"))
            for item in manifest
            if isinstance(item, dict) and item.get("title")
        ]
        if titles:
            return titles
    return []


def _shader_field_extra() -> dict[str, Any]:
    extra = ui_field_config(
        order=1,
        category="input",
        is_load_param=False,
        label="Shader",
    )
    if options := _shader_title_options():
        extra["enum"] = options
    return extra


class ShaderClawConfig(BasePipelineConfig):
    """Configuration for the ShaderClaw3 native renderer pipeline."""

    pipeline_id = "shaderclaw-3"
    pipeline_name = "ShaderClaw 3"
    pipeline_description = (
        "Renders ShaderClaw3 ISF shader files natively as a Scope video source."
    )
    pipeline_version = "0.1.0"
    estimated_vram_gb = 0.0
    requires_models = False
    supports_prompts = False
    inputs = []
    outputs = ["video"]

    modes = {"text": ModeDefaults(default=True)}

    height: int = Field(
        default=720,
        ge=1,
        description="Output height in pixels",
        json_schema_extra=ui_field_config(
            order=1,
            is_load_param=True,
            label="Height",
        ),
    )
    width: int = Field(
        default=1280,
        ge=1,
        description="Output width in pixels",
        json_schema_extra=ui_field_config(
            order=2,
            is_load_param=True,
            label="Width",
        ),
    )
    shaders_dir: str = Field(
        default="",
        description="Optional ShaderClaw3 shaders directory or repo path",
        json_schema_extra=ui_field_config(
            order=3,
            is_load_param=True,
            label="Shaders Dir",
        ),
    )
    shader: str = Field(
        default="Gradient",
        description="ShaderClaw manifest title, numeric id, or .fs file name",
        json_schema_extra=_shader_field_extra(),
    )
    parameters_json: str = Field(
        default="{}",
        description="JSON object of ShaderClaw parameter names to values",
        json_schema_extra=ui_field_config(
            order=2,
            category="input",
            is_load_param=False,
            label="Parameters JSON",
        ),
    )
    reload_token: int = Field(
        default=0,
        ge=0,
        description="Increment to force a ShaderClaw shader reload",
        json_schema_extra=ui_field_config(
            order=3,
            category="input",
            is_load_param=False,
            label="Reload",
        ),
    )
