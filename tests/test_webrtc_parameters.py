from types import SimpleNamespace

from scope.server.webrtc import (
    WebRTCManager,
    _expects_webrtc_video_input,
    _mark_video_outputs_sendonly,
)


class FakeCloudTrack:
    def __init__(self):
        self.calls = []

    def pause(self, paused):
        self.paused = paused

    def update_parameters(self, parameters):
        self.calls.append(dict(parameters))
        parameters.pop("node_id", None)


class FakeFrameProcessor:
    def __init__(self):
        self.calls = []

    def update_parameters(self, parameters):
        self.calls.append(dict(parameters))
        parameters.pop("node_id", None)


def test_broadcast_parameter_update_uses_cloud_track_update_path():
    manager = WebRTCManager.__new__(WebRTCManager)
    cloud_track = FakeCloudTrack()
    local_frame_processor = FakeFrameProcessor()
    manager.sessions = {
        "cloud": SimpleNamespace(
            pc=SimpleNamespace(connectionState="connected"),
            video_track=cloud_track,
            frame_processor=local_frame_processor,
        )
    }
    manager.headless_session = None

    parameters = {"denoising_step_list": [1000, 750], "node_id": "longlive"}

    manager.broadcast_parameter_update(parameters)

    assert cloud_track.calls == [parameters]
    assert local_frame_processor.calls == []
    assert parameters == {"denoising_step_list": [1000, 750], "node_id": "longlive"}


def test_broadcast_parameter_update_copies_params_per_session():
    manager = WebRTCManager.__new__(WebRTCManager)
    first = FakeFrameProcessor()
    second = FakeFrameProcessor()
    manager.sessions = {
        "a": SimpleNamespace(
            pc=SimpleNamespace(connectionState="connected"),
            video_track=None,
            frame_processor=first,
        ),
        "b": SimpleNamespace(
            pc=SimpleNamespace(connectionState="connected"),
            video_track=None,
            frame_processor=second,
        ),
    }
    manager.headless_session = None

    manager.broadcast_parameter_update({"node_id": "longlive", "reset_cache": True})

    assert first.calls == [{"node_id": "longlive", "reset_cache": True}]
    assert second.calls == [{"node_id": "longlive", "reset_cache": True}]


def test_text_session_without_sources_does_not_expect_video_input():
    assert not _expects_webrtc_video_input({"input_mode": "text"}, [])
    assert _expects_webrtc_video_input({"input_mode": "video"}, [])
    assert _expects_webrtc_video_input({"input_mode": "text"}, ["camera"])


def test_mark_video_outputs_sendonly_only_marks_sending_video_transceivers():
    video_sender = SimpleNamespace(track=object())
    recvonly_sender = SimpleNamespace(track=None)
    audio_sender = SimpleNamespace(track=object())
    video_transceiver = SimpleNamespace(
        kind="video",
        sender=video_sender,
        direction="sendrecv",
    )
    recvonly_transceiver = SimpleNamespace(
        kind="video",
        sender=recvonly_sender,
        direction="recvonly",
    )
    audio_transceiver = SimpleNamespace(
        kind="audio",
        sender=audio_sender,
        direction="sendrecv",
    )
    pc = SimpleNamespace(
        getTransceivers=lambda: [
            video_transceiver,
            recvonly_transceiver,
            audio_transceiver,
        ]
    )

    _mark_video_outputs_sendonly(pc, "test")

    assert video_transceiver.direction == "sendonly"
    assert recvonly_transceiver.direction == "recvonly"
    assert audio_transceiver.direction == "sendrecv"
