import numpy as np
import pytest
from av import VideoFrame

from scope.server.cloud_proxy import (
    _normalize_legacy_longlive_node_definitions,
    _normalize_legacy_longlive_schema,
)
from scope.server.graph_schema import GraphConfig
from scope.server.remote_scope import (
    build_remote_scope_initial_parameters,
    build_remote_scope_output_mapping,
    legacy_pipeline_load_request,
    normalize_remote_scope_url,
    parse_remote_scope_graph,
    pipeline_load_request_identity,
    remote_pipeline_status_matches_load,
    remote_scope_input_track_count,
    remote_scope_runtime_bootstrap_parameters,
)
from scope.server.sink_manager import SinkManager


def test_normalize_remote_scope_url_defaults_to_https():
    assert normalize_remote_scope_url("abc-8000.proxy.runpod.net") == (
        "https://abc-8000.proxy.runpod.net"
    )


def test_normalize_remote_scope_url_rejects_empty():
    with pytest.raises(ValueError):
        normalize_remote_scope_url("")


def test_graph_sources_are_ordered_for_remote_webrtc_tracks():
    params = {
        "input_mode": "video",
        "graph": {
            "nodes": [
                {"id": "camera", "type": "source", "source_mode": "camera"},
                {"id": "syphon", "type": "source", "source_mode": "syphon"},
                {"id": "pipe", "type": "pipeline", "pipeline_id": "longlive"},
                {"id": "out", "type": "sink"},
            ],
            "edges": [],
        },
    }

    graph = parse_remote_scope_graph(params)
    assert graph.source_node_to_track_index == {"camera": 0, "syphon": 1}
    assert remote_scope_input_track_count(params, graph) == 2
    assert build_remote_scope_initial_parameters(params)["source_track_order"] == [
        "camera",
        "syphon",
    ]


def test_text_mode_does_not_create_input_tracks():
    params = {"input_mode": "text", "pipeline_ids": ["longlive"]}
    graph = parse_remote_scope_graph(params)
    assert remote_scope_input_track_count(params, graph) == 0


def test_runtime_bootstrap_flattens_video_defaults_for_legacy_shared_processors():
    params = {
        "input_mode": "video",
        "pipeline_ids": ["longlive"],
        "graph": {"nodes": [], "edges": []},
        "source_track_order": ["input"],
        "node_id": "longlive",
        "prompts": [{"text": "a flower", "weight": 100}],
        "denoising_step_list": [1000, 750],
        "noise_scale": None,
        "noise_controller": None,
        "vace_context_scale": 1.0,
    }

    assert remote_scope_runtime_bootstrap_parameters(params) == {
        "input_mode": "video",
        "prompts": [{"text": "a flower", "weight": 100}],
        "denoising_step_list": [1000, 750],
        "vace_context_scale": 1.0,
        "noise_scale": 0.7,
        "noise_controller": True,
        "reset_cache": True,
    }


def test_runtime_bootstrap_preserves_explicit_video_noise_settings():
    params = {
        "input_mode": "video",
        "noise_scale": 0.42,
        "noise_controller": False,
    }

    assert remote_scope_runtime_bootstrap_parameters(params) == {
        "input_mode": "video",
        "noise_scale": 0.42,
        "noise_controller": False,
        "reset_cache": True,
    }


def test_legacy_longlive_schema_defaults_are_normalized_for_desktop():
    schema = {
        "pipelines": {
            "longlive": {
                "config_schema": {
                    "properties": {
                        "height": {"default": 320},
                        "width": {"default": 576},
                        "vae_type": {"default": "wan"},
                        "denoising_steps": {"default": [1000, 750, 500, 250]},
                    }
                },
                "mode_defaults": {
                    "video": {
                        "height": 512,
                        "width": 512,
                        "denoising_steps": [1000, 750],
                    },
                },
            }
        }
    }

    assert _normalize_legacy_longlive_schema(schema)["pipelines"]["longlive"] == {
        "config_schema": {
            "properties": {
                "height": {"default": 480},
                "width": {"default": 832},
                "vae_type": {"default": "lighttae"},
                "denoising_steps": {"default": [1000, 875, 750]},
            }
        },
        "mode_defaults": {
            "video": {
                "height": 480,
                "width": 832,
                "denoising_steps": [1000, 875, 750],
            },
        },
    }


def test_legacy_longlive_node_definition_defaults_are_normalized_for_desktop():
    definitions = {
        "nodes": [
            {"node_type_id": "passthrough", "pipeline_meta": None},
            {
                "node_type_id": "longlive",
                "pipeline_meta": {
                    "config_schema": {
                        "properties": {
                            "height": {"default": 320},
                            "width": {"default": 576},
                            "vae_type": {"default": "wan"},
                            "denoising_steps": {"default": [1000, 750]},
                        }
                    },
                    "mode_defaults": {"video": {"height": 512, "width": 512}},
                },
            },
        ]
    }

    longlive = _normalize_legacy_longlive_node_definitions(definitions)["nodes"][1][
        "pipeline_meta"
    ]
    props = longlive["config_schema"]["properties"]

    assert props["height"]["default"] == 480
    assert props["width"]["default"] == 832
    assert props["vae_type"]["default"] == "lighttae"
    assert props["denoising_steps"]["default"] == [1000, 875, 750]
    assert longlive["mode_defaults"]["video"] == {
        "height": 480,
        "width": 832,
        "denoising_steps": [1000, 875, 750],
    }


def test_legacy_pipeline_load_request_translates_shared_load_params():
    body = {
        "pipelines": [
            {
                "node_id": "pipe",
                "pipeline_id": "longlive",
                "load_params": {"width": 832, "height": 480},
            }
        ],
        "connection_id": "remote-scope-test",
    }

    assert legacy_pipeline_load_request(body) == {
        "pipeline_ids": ["longlive"],
        "load_params": {"width": 832, "height": 480},
        "connection_id": "remote-scope-test",
    }


def test_legacy_pipeline_load_request_rejects_per_node_load_params():
    body = {
        "pipelines": [
            {
                "node_id": "pipe-a",
                "pipeline_id": "longlive",
                "load_params": {"width": 832, "height": 480},
            },
            {
                "node_id": "pipe-b",
                "pipeline_id": "passthrough",
                "load_params": {"width": 640, "height": 360},
            },
        ]
    }

    assert legacy_pipeline_load_request(body) is None


def test_pipeline_load_request_identity_accepts_current_and_legacy_shapes():
    current = {
        "pipelines": [
            {
                "node_id": "pipe",
                "pipeline_id": "longlive",
                "load_params": {"width": 832, "height": 480},
            }
        ]
    }
    legacy = {
        "pipeline_ids": ["longlive"],
        "load_params": {"width": 832, "height": 480},
    }

    assert pipeline_load_request_identity(current) == pipeline_load_request_identity(
        legacy
    )


def test_remote_pipeline_status_matches_equivalent_loaded_pipeline():
    requested = pipeline_load_request_identity(
        {
            "pipelines": [
                {
                    "node_id": "pipe",
                    "pipeline_id": "longlive",
                    "load_params": {
                        "width": 832,
                        "height": 480,
                        "vae_type": "LIGHTTAE",
                        "vace_enabled": True,
                    },
                }
            ]
        }
    )

    assert requested is not None
    assert remote_pipeline_status_matches_load(
        {
            "status": "loaded",
            "pipeline_id": "longlive",
            "load_params": {
                "width": 832,
                "height": 480,
                "vae_type": "lighttae",
                "vace_enabled": True,
                "seed": 42,
            },
        },
        requested,
    )


def test_remote_pipeline_status_rejects_different_resolution():
    requested = pipeline_load_request_identity(
        {
            "pipeline_ids": ["longlive"],
            "load_params": {"width": 1280, "height": 720},
        }
    )

    assert requested is not None
    assert not remote_pipeline_status_matches_load(
        {
            "status": "loaded",
            "pipeline_id": "longlive",
            "load_params": {"width": 832, "height": 480},
        },
        requested,
    )


def test_sink_attached_record_is_mirrored_locally():
    params = {
        "graph": {
            "nodes": [
                {"id": "input", "type": "source"},
                {"id": "pipe", "type": "pipeline", "pipeline_id": "longlive"},
                {"id": "out", "type": "sink"},
                {"id": "record", "type": "record"},
            ],
            "edges": [
                {
                    "from": "out",
                    "from_port": "video",
                    "to_node": "record",
                    "to_port": "video",
                    "kind": "stream",
                }
            ],
        }
    }

    graph = parse_remote_scope_graph(params)
    mapping = build_remote_scope_output_mapping(graph)
    assert mapping.num_output_tracks == 1
    assert mapping.num_local_handlers == 2
    assert mapping.sink_tee_pairs == [(0, 1)]
    runner_params = build_remote_scope_initial_parameters(params)
    runner_node_ids = [node["id"] for node in runner_params["graph"]["nodes"]]
    assert runner_node_ids == ["input", "pipe", "out"]


def test_pipeline_attached_record_gets_remote_output_slot():
    params = {
        "graph": {
            "nodes": [
                {"id": "input", "type": "source"},
                {"id": "pipe", "type": "pipeline", "pipeline_id": "longlive"},
                {"id": "out", "type": "sink"},
                {"id": "record", "type": "record"},
            ],
            "edges": [
                {
                    "from": "pipe",
                    "from_port": "video",
                    "to_node": "out",
                    "to_port": "video",
                    "kind": "stream",
                },
                {
                    "from": "pipe",
                    "from_port": "video",
                    "to_node": "record",
                    "to_port": "video",
                    "kind": "stream",
                },
            ],
        }
    }

    graph = parse_remote_scope_graph(params)
    mapping = build_remote_scope_output_mapping(graph)
    assert graph.remote_record_node_ids == ["record"]
    assert mapping.num_output_tracks == 2
    assert mapping.num_local_handlers == 2
    assert mapping.remote_to_local == [0, 1]

    runner_params = build_remote_scope_initial_parameters(params)
    runner_record_node = next(
        node for node in runner_params["graph"]["nodes"] if node["id"] == "record"
    )
    assert runner_record_node["type"] == "sink"


def test_cloud_graph_sets_local_sink_queues_for_remote_outputs():
    graph = GraphConfig(
        nodes=[
            {"id": "input", "type": "source"},
            {"id": "pipe", "type": "pipeline", "pipeline_id": "longlive"},
            {"id": "preview", "type": "sink"},
            {
                "id": "native",
                "type": "sink",
                "sink_mode": "ndi",
                "sink_name": "Scope Remote Out",
            },
        ],
        edges=[
            {
                "from": "pipe",
                "from_port": "video",
                "to_node": "preview",
                "to_port": "video",
                "kind": "stream",
            },
            {
                "from": "pipe",
                "from_port": "video",
                "to_node": "native",
                "to_port": "video",
                "kind": "stream",
            },
        ],
    )
    manager = SinkManager()
    manager.start()
    manager.setup_cloud_graph(graph)

    assert manager.get_sink_node_ids() == ["preview", "native"]
    assert "native" in manager._sink_hardware_queues_by_node

    frame = VideoFrame.from_ndarray(
        np.zeros((8, 8, 3), dtype=np.uint8),
        format="rgb24",
    )
    manager.put_to_sink("preview", frame)

    packet = manager.get_packet_from_sink("preview")
    assert packet is not None
    assert tuple(packet.tensor.shape) == (8, 8, 3)

    manager.stop()
