import asyncio
import copy

import numpy as np
import pytest
from av import VideoFrame

import scope.server.cloud_proxy as cloud_proxy
import scope.server.remote_scope as remote_scope
from scope.server.cloud_proxy import (
    _normalize_legacy_longlive_node_definitions,
    _normalize_legacy_longlive_schema,
)
from scope.server.graph_schema import GraphConfig
from scope.server.remote_scope import (
    build_remote_scope_initial_parameters,
    build_remote_scope_output_mapping,
    build_remote_scope_session_parameters,
    ensure_remote_scope_graph_output_edges,
    filter_remote_scope_initial_parameters,
    filter_remote_scope_parameter_update,
    filter_remote_scope_pipeline_load_body,
    is_remote_scope_local_only_models_status_request,
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


def test_cloud_node_definitions_keep_local_source_plugins(monkeypatch):
    monkeypatch.setattr(
        cloud_proxy,
        "_local_source_node_definitions",
        lambda: [
            {
                "node_type_id": "cue.session",
                "display_name": "Cue Director",
                "category": "cue",
                "description": "local cue node",
                "inputs": [{"name": "transcript", "port_type": "string"}],
                "outputs": [{"name": "reset", "port_type": "boolean"}],
                "params": [],
                "continuous": True,
                "pipeline_meta": None,
                "plugin_name": "scope-cue",
            }
        ],
    )
    definitions = {
        "nodes": [
            {"node_type_id": "longlive", "pipeline_meta": {}},
            {
                "node_type_id": "cue.session",
                "display_name": "stale cloud cue",
                "inputs": [],
                "outputs": [],
            },
        ]
    }

    nodes = _normalize_legacy_longlive_node_definitions(definitions)["nodes"]

    assert [node["node_type_id"] for node in nodes] == ["longlive", "cue.session"]
    cue = nodes[1]
    assert cue["display_name"] == "Cue Director"
    assert cue["outputs"] == [{"name": "reset", "port_type": "boolean"}]


def test_cloud_pipeline_schemas_keep_local_source_plugins(monkeypatch):
    monkeypatch.setattr(
        cloud_proxy,
        "_local_source_pipeline_schemas",
        lambda: {
            "shaderclaw-3": {
                "id": "shaderclaw-3",
                "name": "ShaderClaw 3",
                "config_schema": {
                    "properties": {
                        "shader": {
                            "default": "Gradient",
                            "enum": ["Gradient", "Etherea"],
                        }
                    }
                },
                "plugin_name": "scope-shaderclaw",
            }
        },
    )
    schemas = {
        "pipelines": {
            "longlive": {"config_schema": {"properties": {}}},
            "shaderclaw-3": {
                "id": "shaderclaw-3",
                "name": "stale remote shaderclaw",
                "plugin_name": "cloud-copy",
            },
        }
    }

    pipelines = _normalize_legacy_longlive_schema(schemas)["pipelines"]

    assert set(pipelines) == {"longlive", "shaderclaw-3"}
    assert pipelines["shaderclaw-3"]["name"] == "ShaderClaw 3"
    assert pipelines["shaderclaw-3"]["plugin_name"] == "scope-shaderclaw"
    assert pipelines["shaderclaw-3"]["config_schema"]["properties"]["shader"][
        "enum"
    ] == ["Gradient", "Etherea"]


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


def test_remote_pipeline_load_filters_local_only_cue_node():
    body = {
        "pipelines": [
            {
                "node_id": "longlive",
                "pipeline_id": "longlive",
                "load_params": {"width": 832, "height": 480},
            },
            {
                "node_id": "cue",
                "pipeline_id": "cue.session",
                "load_params": {},
            },
        ],
        "connection_id": "remote-scope-test",
    }

    assert filter_remote_scope_pipeline_load_body(body) == {
        "pipelines": [
            {
                "node_id": "longlive",
                "pipeline_id": "longlive",
                "load_params": {"width": 832, "height": 480},
            }
        ],
        "connection_id": "remote-scope-test",
    }


def test_remote_pipeline_load_filters_local_only_source_plugin(monkeypatch):
    monkeypatch.setattr(
        remote_scope,
        "is_remote_scope_local_only_node_type",
        lambda value: value in {"cue.session", "shaderclaw-3"},
    )
    body = {
        "pipelines": [
            {
                "node_id": "longlive",
                "pipeline_id": "longlive",
                "load_params": {"width": 832, "height": 480},
            },
            {
                "node_id": "shader",
                "pipeline_id": "shaderclaw-3",
                "load_params": {"shader": "water"},
            },
        ],
        "connection_id": "remote-scope-test",
    }

    assert filter_remote_scope_pipeline_load_body(body) == {
        "pipelines": [
            {
                "node_id": "longlive",
                "pipeline_id": "longlive",
                "load_params": {"width": 832, "height": 480},
            }
        ],
        "connection_id": "remote-scope-test",
    }


def test_remote_pipeline_load_returns_none_for_only_local_source_plugin(monkeypatch):
    monkeypatch.setattr(
        remote_scope,
        "is_remote_scope_local_only_node_type",
        lambda value: value == "shaderclaw-3",
    )
    body = {
        "pipeline_ids": ["shaderclaw-3"],
        "load_params": {"shader": "water"},
    }

    assert filter_remote_scope_pipeline_load_body(body) is None


def test_remote_source_plugins_are_local_only(monkeypatch):
    class FakePluginManager:
        def get_plugin_for_type_id(self, type_id):
            return {"shaderclaw-3": "scope-shaderclaw"}.get(type_id)

        def list_plugins_sync(self, *, skip_update_check=False):
            assert skip_update_check
            return [
                {"name": "scope-shaderclaw", "kind": "source"},
                {"name": "scope-longlive", "kind": None},
            ]

    import scope.core.plugins as plugins

    monkeypatch.setattr(plugins, "get_plugin_manager", lambda: FakePluginManager())

    assert remote_scope.is_remote_scope_local_only_node_type("shaderclaw-3")
    assert not remote_scope.is_remote_scope_local_only_node_type("longlive")


def test_remote_pipeline_load_returns_none_for_only_local_nodes():
    body = {
        "pipeline_ids": ["cue.session"],
        "load_params": {},
    }

    assert filter_remote_scope_pipeline_load_body(body) is None


def test_remote_models_status_short_circuits_local_only_cue_node():
    assert is_remote_scope_local_only_models_status_request(
        "pipeline_id=cue.session"
    )
    assert not is_remote_scope_local_only_models_status_request(
        "pipeline_id=longlive"
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


def test_remote_initial_parameters_filter_local_only_cue_node_and_edges():
    params = {
        "input_mode": "video",
        "graph": {
            "nodes": [
                {"id": "input", "type": "source"},
                {"id": "cue", "type": "node", "node_type_id": "cue.session"},
                {"id": "pipe", "type": "pipeline", "pipeline_id": "longlive"},
                {"id": "out", "type": "sink"},
            ],
            "edges": [
                {
                    "from": "input",
                    "from_port": "video",
                    "to_node": "pipe",
                    "to_port": "video",
                    "kind": "stream",
                },
                {
                    "from": "cue",
                    "from_port": "prompt",
                    "to_node": "pipe",
                    "to_port": "prompt",
                    "kind": "data",
                },
                {
                    "from": "pipe",
                    "from_port": "video",
                    "to_node": "out",
                    "to_port": "video",
                    "kind": "stream",
                },
            ],
            "ui_state": {
                "node_params": {
                    "cue": {"session_id": "demo"},
                    "pipe": {"width": 832},
                }
            },
        },
    }

    filtered = filter_remote_scope_initial_parameters(params)
    assert [node["id"] for node in filtered["graph"]["nodes"]] == [
        "input",
        "pipe",
        "out",
    ]
    assert [edge["from"] for edge in filtered["graph"]["edges"]] == [
        "input",
        "pipe",
    ]
    assert filtered["graph"]["ui_state"]["node_params"] == {"pipe": {"width": 832}}

    runner_params = build_remote_scope_initial_parameters(params)
    assert [node["id"] for node in runner_params["graph"]["nodes"]] == [
        "input",
        "pipe",
        "out",
    ]
    assert runner_params["source_track_order"] == ["input"]


def test_remote_initial_parameters_filter_local_only_source_pipeline_and_edges(
    monkeypatch,
):
    monkeypatch.setattr(
        remote_scope,
        "is_remote_scope_local_only_node_type",
        lambda value: value in {"shaderclaw-3"},
    )
    params = {
        "input_mode": "text",
        "graph": {
            "nodes": [
                {"id": "shader", "type": "pipeline", "pipeline_id": "shaderclaw-3"},
                {"id": "pipe", "type": "pipeline", "pipeline_id": "longlive"},
                {"id": "out", "type": "sink"},
            ],
            "edges": [
                {
                    "from": "shader",
                    "from_port": "video",
                    "to_node": "out",
                    "to_port": "video",
                    "kind": "stream",
                },
                {
                    "from": "pipe",
                    "from_port": "video",
                    "to_node": "out",
                    "to_port": "video",
                    "kind": "stream",
                },
            ],
            "ui_state": {
                "node_params": {
                    "shader": {"shader": "water"},
                    "pipe": {"width": 832},
                }
            },
        },
    }

    filtered = filter_remote_scope_initial_parameters(params)

    assert [node["id"] for node in filtered["graph"]["nodes"]] == ["pipe", "out"]
    assert [edge["from"] for edge in filtered["graph"]["edges"]] == ["pipe"]
    assert filtered["graph"]["ui_state"]["node_params"] == {"pipe": {"width": 832}}


def test_remote_initial_parameters_add_output_edge_for_simple_plugin_graph():
    params = {
        "input_mode": "text",
        "graph": {
            "nodes": [
                {"id": "longlive", "type": "pipeline", "pipeline_id": "longlive"},
                {"id": "output", "type": "sink"},
                {"id": "cue", "type": "node", "node_type_id": "cue.session"},
            ],
            "edges": [],
        },
    }

    filtered = build_remote_scope_initial_parameters(params)

    assert filtered["graph"]["nodes"] == [
        {"id": "longlive", "type": "pipeline", "pipeline_id": "longlive"},
        {"id": "output", "type": "sink"},
    ]
    assert filtered["graph"]["edges"] == [
        {
            "from": "longlive",
            "from_port": "video",
            "to_node": "output",
            "to_port": "video",
            "kind": "stream",
        }
    ]


def test_remote_output_edge_repair_does_not_guess_between_multiple_pipelines():
    params = {
        "graph": {
            "nodes": [
                {"id": "pipe-a", "type": "pipeline", "pipeline_id": "longlive"},
                {"id": "pipe-b", "type": "pipeline", "pipeline_id": "longlive"},
                {"id": "output", "type": "sink"},
            ],
            "edges": [],
        }
    }

    assert ensure_remote_scope_graph_output_edges(params)["graph"]["edges"] == []


def test_remote_output_edge_repair_materializes_sink_to_sink_syphon_tee():
    params = {
        "input_mode": "text",
        "graph": {
            "nodes": [
                {"id": "longlive", "type": "pipeline", "pipeline_id": "longlive"},
                {"id": "output", "type": "sink"},
                {
                    "id": "output_sink",
                    "type": "sink",
                    "sink_mode": "syphon",
                    "sink_name": "Scope",
                },
            ],
            "edges": [
                {
                    "from_node": "output",
                    "from_port": "out",
                    "to_node": "output_sink",
                    "to_port": "video",
                    "kind": "stream",
                },
            ],
        },
    }

    filtered = build_remote_scope_initial_parameters(params)
    session_params = build_remote_scope_session_parameters(params)
    edges = filtered["graph"]["edges"]
    graph = session_params.graph_info
    mapping = build_remote_scope_output_mapping(graph)

    assert {
        (edge.get("from", edge.get("from_node")), edge["to_node"])
        for edge in edges
    } == {("longlive", "output")}
    assert [node["id"] for node in filtered["graph"]["nodes"]] == [
        "longlive",
        "output",
    ]
    assert mapping.num_output_tracks == 1
    assert mapping.num_local_handlers == 2
    assert mapping.remote_to_local == [0]
    assert mapping.sink_tee_pairs == [(0, 1)]
    assert all(edge.get("from_node") != "output" for edge in edges)
    assert session_params.runner_params == filtered


def test_remote_parameter_update_filters_dedupes_and_collapses_local_sinks():
    update = {
        "reset_cache": False,
        "graph": {
            "nodes": [
                {"id": "longlive", "type": "pipeline", "pipeline_id": "longlive"},
                {"id": "output", "type": "sink"},
                {
                    "id": "cue",
                    "type": "node",
                    "node_type_id": "cue.session",
                },
                {
                    "id": "output_sink",
                    "type": "sink",
                    "sink_mode": "syphon",
                    "sink_name": "Scope",
                },
            ],
            "edges": [
                {
                    "from_node": "cue",
                    "from_port": "reset",
                    "to_node": "longlive",
                    "to_port": "reset_cache",
                    "kind": "stream",
                },
                {
                    "from_node": "cue",
                    "from_port": "reset",
                    "to_node": "longlive",
                    "to_port": "reset_cache",
                    "kind": "stream",
                },
                {
                    "from_node": "longlive",
                    "from_port": "video",
                    "to_node": "output",
                    "to_port": "video",
                    "kind": "stream",
                },
                {
                    "from_node": "longlive",
                    "from_port": "video",
                    "to_node": "output",
                    "to_port": "video",
                    "kind": "stream",
                },
                {
                    "from_node": "longlive",
                    "from_port": "video",
                    "to_node": "output_sink",
                    "to_port": "video",
                    "kind": "stream",
                },
            ],
        },
    }

    filtered = filter_remote_scope_parameter_update(update)

    assert filtered["reset_cache"] is False
    assert [node["id"] for node in filtered["graph"]["nodes"]] == [
        "longlive",
        "output",
    ]
    assert filtered["graph"]["edges"] == [
        {
            "from_node": "longlive",
            "from_port": "video",
            "to_node": "output",
            "to_port": "video",
            "kind": "stream",
        }
    ]


def test_remote_parameter_updates_post_over_http_with_latest_wins(monkeypatch):
    async def run_test():
        connection = remote_scope.RemoteScopeConnection()
        connection._connected = True
        connection._base_url = "https://remote.example"
        connection._loop = asyncio.get_running_loop()

        calls = []
        first_started = asyncio.Event()
        release_first = asyncio.Event()
        second_done = asyncio.Event()

        async def fake_api_request(method, path, body=None, timeout=None):
            calls.append((method, path, copy.deepcopy(body), timeout))
            if len(calls) == 1:
                first_started.set()
                await release_first.wait()
            if len(calls) == 2:
                second_done.set()
            return {"status": 200, "data": {"applied_parameters": body or {}}}

        monkeypatch.setattr(connection, "api_request", fake_api_request)

        connection.send_parameters(
            {"prompts": [{"text": "old", "weight": 100}], "node_id": "longlive"}
        )
        await asyncio.wait_for(first_started.wait(), timeout=1)

        connection.send_parameters(
            {"prompts": [{"text": "middle", "weight": 100}], "node_id": "longlive"}
        )
        connection.send_parameters(
            {"prompts": [{"text": "new", "weight": 100}], "node_id": "longlive"}
        )
        await asyncio.sleep(0)

        release_first.set()
        await asyncio.wait_for(second_done.wait(), timeout=1)
        await asyncio.sleep(0)

        assert [call[1] for call in calls] == [
            "/api/v1/session/parameters",
            "/api/v1/session/parameters",
        ]
        assert [call[2]["prompts"][0]["text"] for call in calls] == ["old", "new"]
        assert calls[0][3] == 10.0
        assert calls[1][3] == 10.0

    asyncio.run(run_test())


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
