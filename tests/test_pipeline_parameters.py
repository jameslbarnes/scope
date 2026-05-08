import queue

import torch

from scope.core.nodes.base import NodeDefinition
from scope.server.frame_processor import FrameProcessor
from scope.server.pipeline_processor import PipelineProcessor


class RecordingPipeline:
    def __init__(self):
        self.calls = []

    def get_definition(self):
        return NodeDefinition(node_type_id="recording", display_name="Recording")

    def prepare(self, **kwargs):
        return None

    def __call__(self, **kwargs):
        self.calls.append(dict(kwargs))
        return {"video": torch.zeros(1, 8, 8, 3)}


class RecordingProcessor:
    def __init__(self):
        self.calls = []

    def update_parameters(self, parameters):
        self.calls.append(dict(parameters))
        return True


def _prompt(text: str) -> list[dict]:
    return [{"text": text, "weight": 100.0}]


def test_pipeline_processor_coalesces_prompt_updates_before_generation():
    pipeline = RecordingPipeline()
    processor = PipelineProcessor(pipeline=pipeline, pipeline_id="longlive")
    processor.update_parameters({"prompts": _prompt("old atom")})
    processor.update_parameters({"prompts": _prompt("middle bird"), "reset_cache": True})
    processor.update_parameters({"prompts": _prompt("new frog")})

    processor.process_chunk()

    assert len(pipeline.calls) == 1
    assert pipeline.calls[0]["prompts"] == _prompt("new frog")
    assert pipeline.calls[0]["init_cache"] is True
    assert processor.parameters["prompts"] == _prompt("new frog")
    assert "reset_cache" not in processor.parameters


def test_pipeline_processor_keeps_latest_prompt_when_queue_is_full():
    pipeline = RecordingPipeline()
    processor = PipelineProcessor(pipeline=pipeline, pipeline_id="longlive")
    processor.parameters_queue = queue.Queue(maxsize=1)

    assert processor.update_parameters({"prompts": _prompt("old atom")}) is True
    assert (
        processor.update_parameters(
            {"prompts": _prompt("new frog"), "reset_cache": True}
        )
        is True
    )

    queued = processor.parameters_queue.get_nowait()
    assert queued["prompts"] == _prompt("new frog")
    assert queued["reset_cache"] is True


def test_frame_processor_does_not_keep_reset_cache_in_session_state():
    processor = FrameProcessor(
        pipeline_manager=None,
        initial_parameters={"reset_cache": True, "node_id": "longlive"},
    )
    target = RecordingProcessor()
    processor._processors_by_node_id["longlive"] = target
    processor._graph_ready = True

    processor.update_parameters(
        {
            "node_id": "longlive",
            "prompts": _prompt("new frog"),
            "reset_cache": True,
        }
    )

    assert target.calls == [{"prompts": _prompt("new frog"), "reset_cache": True}]
    assert processor.parameters["prompts"] == _prompt("new frog")
    assert processor.parameters["node_id"] == "longlive"
    assert "reset_cache" not in processor.parameters


def test_pipeline_processor_records_batch_telemetry_for_cache_reset():
    pipeline = RecordingPipeline()
    processor = PipelineProcessor(pipeline=pipeline, pipeline_id="longlive")
    processor.output_queues = {"video": [queue.Queue(maxsize=10)]}
    processor.update_parameters(
        {
            "prompts": _prompt("new frog"),
            "reset_cache": True,
            "kv_cache_attention_bias": 0.5,
        }
    )

    processor.process_chunk()

    telemetry = processor.get_telemetry()
    assert telemetry["last_batch"]["frames"] == 1
    assert telemetry["last_batch"]["init_cache"] is True
    assert telemetry["last_batch"]["reset_cache"] is True
    assert telemetry["last_batch"]["kv_cache_attention_bias"] == 0.5
    assert telemetry["output_queue_sizes"] == {"video": [1]}
