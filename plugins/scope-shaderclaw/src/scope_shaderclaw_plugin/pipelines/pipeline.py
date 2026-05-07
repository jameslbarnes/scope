"""ShaderClaw3 native shader source pipeline."""

from __future__ import annotations

import base64
import logging
from io import BytesIO
from typing import TYPE_CHECKING

import numpy as np
import torch
from PIL import Image

from scope.core.pipelines.interface import Pipeline

from ..native_renderer import (
    NativeShaderClawRenderer,
    ShaderClawNativeError,
    parse_parameters_json,
)
from .schema import ShaderClawConfig

if TYPE_CHECKING:
    from scope.core.pipelines.base_schema import BasePipelineConfig

logger = logging.getLogger(__name__)


def data_url_to_tensor(
    data_url: str,
    *,
    width: int,
    height: int,
    device: torch.device,
) -> torch.Tensor:
    """Decode a PNG data URL into a THWC float tensor in [0, 1].

    Kept for compatibility with older tests and external callers. The pipeline
    itself no longer uses screenshots.
    """

    if "," not in data_url:
        raise ShaderClawNativeError("Screenshot data URL is missing a base64 payload")
    _header, payload = data_url.split(",", 1)
    try:
        raw = base64.b64decode(payload)
    except ValueError as e:
        raise ShaderClawNativeError("Screenshot data URL is not valid base64") from e

    image = Image.open(BytesIO(raw)).convert("RGB")
    if image.size != (width, height):
        image = image.resize((width, height), Image.Resampling.BILINEAR)

    array = np.asarray(image, dtype=np.float32) / 255.0
    tensor = torch.from_numpy(array).unsqueeze(0).contiguous()
    if device.type != "cpu":
        tensor = tensor.to(device=device, non_blocking=True)
    return tensor


class ShaderClawPipeline(Pipeline):
    """Render ShaderClaw3 ISF shaders natively as Scope video."""

    @classmethod
    def get_config_class(cls) -> type[BasePipelineConfig]:
        return ShaderClawConfig

    def __init__(
        self,
        height: int = 720,
        width: int = 1280,
        shaders_dir: str | None = None,
        shader: str = "Gradient",
        parameters_json: str = "{}",
        reload_token: int = 0,
        device: torch.device | None = None,
        **kwargs,
    ):
        self.height = int(height)
        self.width = int(width)
        self.device = device or torch.device("cpu")
        self.shaders_dir = shaders_dir
        self.shader = shader
        self.parameters_json = parameters_json
        self.reload_token = reload_token
        self._configured_shader: str | None = None
        self._configured_parameters_json: str | None = None
        self._configured_reload_token: int | None = None
        self._last_error: str | None = None
        self._last_logged_error: str | None = None
        self._last_frame: torch.Tensor | None = None
        self._black_frame = torch.zeros(
            (1, self.height, self.width, 3),
            dtype=torch.float32,
            device=self.device,
        )
        self.renderer = NativeShaderClawRenderer(
            height=self.height,
            width=self.width,
            shaders_dir=shaders_dir,
            device=self.device,
        )

    def __call__(self, **kwargs) -> dict:
        shader = str(kwargs.get("shader", self.shader) or "Gradient")
        parameters_json = str(
            kwargs.get("parameters_json", self.parameters_json) or "{}"
        )
        reload_token = int(kwargs.get("reload_token", self.reload_token) or 0)

        try:
            parameters = parse_parameters_json(parameters_json)
            if self._needs_configuration(shader, parameters_json, reload_token):
                self.renderer.load_shader(shader, parameters)
                self._configured_shader = shader
                self._configured_parameters_json = parameters_json
                self._configured_reload_token = reload_token

            self._last_frame = self.renderer.render_tensor(shader, parameters)
            self._clear_error()
            return {"video": self._last_frame}
        except ShaderClawNativeError as e:
            return {"video": self._handle_error(e)}

    def _needs_configuration(
        self,
        shader: str,
        parameters_json: str,
        reload_token: int,
    ) -> bool:
        return (
            self._configured_shader != shader
            or self._configured_parameters_json != parameters_json
            or self._configured_reload_token != reload_token
        )

    def _handle_error(self, error: ShaderClawNativeError) -> torch.Tensor:
        self._last_error = str(error)
        if self._last_logged_error != self._last_error:
            logger.warning("ShaderClaw native pipeline error: %s", self._last_error)
            self._last_logged_error = self._last_error
        return self._last_frame if self._last_frame is not None else self._black_frame

    def _clear_error(self) -> None:
        self._last_error = None
        self._last_logged_error = None
