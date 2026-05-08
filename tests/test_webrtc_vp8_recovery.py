from scope.server.webrtc_vp8_recovery import should_force_periodic_vp8_keyframe


class DummyEncoder:
    pass


def test_periodic_vp8_keyframe_is_forced_on_first_frame():
    encoder = DummyEncoder()

    assert should_force_periodic_vp8_keyframe(encoder, 30, False)


def test_periodic_vp8_keyframe_repeats_after_interval():
    encoder = DummyEncoder()
    decisions = [
        should_force_periodic_vp8_keyframe(encoder, 3, False) for _ in range(7)
    ]

    assert decisions == [True, False, False, True, False, False, True]


def test_vp8_keyframe_request_is_preserved_when_interval_disabled():
    encoder = DummyEncoder()

    assert should_force_periodic_vp8_keyframe(encoder, 0, True)
    assert not should_force_periodic_vp8_keyframe(encoder, 0, False)
