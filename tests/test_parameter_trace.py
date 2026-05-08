from scope.server.parameter_trace import (
    parameter_trace_summary,
    should_trace_parameters,
)


def test_parameter_trace_summary_hashes_prompt_text_and_weight():
    summary = parameter_trace_summary(
        {
            "node_id": "longlive",
            "prompts": [{"text": "a submarine in deep water", "weight": 100}],
            "reset_cache": True,
        }
    )

    assert summary["prompt_hash"]
    assert summary["prompt_preview"] == "a submarine in deep water"
    assert summary["prompt_count"] == 1
    assert summary["node_id"] == "longlive"
    assert summary["reset_cache"] is True


def test_parameter_trace_summary_prefers_transition_target_prompt():
    summary = parameter_trace_summary(
        {
            "prompts": [{"text": "old prompt", "weight": 100}],
            "transition": {
                "target_prompts": [{"text": "new prompt", "weight": 100}],
                "num_steps": 4,
            },
        }
    )

    assert summary["prompt_preview"] == "new prompt"
    assert summary["has_transition"] is True


def test_should_trace_parameters_only_traces_prompt_or_reset_updates():
    assert should_trace_parameters({"noise_scale": 0.35}) is False
    assert should_trace_parameters({"reset_cache": True}) is True
    assert should_trace_parameters({"prompts": [{"text": "octopus"}]}) is True
