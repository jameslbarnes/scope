from types import SimpleNamespace

from scope.server.webrtc_rtp_stats import RtpStatsSampler


def test_rtp_stats_sampler_reports_recent_inbound_loss():
    sampler = RtpStatsSampler()
    sampler.sample(
        [
            SimpleNamespace(
                type="inbound-rtp",
                id="inbound-video",
                ssrc=1,
                kind="video",
                packetsReceived=100,
                packetsLost=2,
                jitter=900,
                framesReceived=30,
                framesDecoded=28,
                framesDropped=1,
            )
        ],
        now=10,
    )

    sample = sampler.sample(
        [
            SimpleNamespace(
                type="inbound-rtp",
                id="inbound-video",
                ssrc=1,
                kind="video",
                packetsReceived=190,
                packetsLost=12,
                jitter=1800,
                framesReceived=90,
                framesDecoded=85,
                framesDropped=4,
                framesPerSecond=24.0,
            )
        ],
        now=20,
    )

    video = sample["aggregate"]["inbound"]["video"]
    stream = sample["streams"][0]
    assert video["loss_pct_total"] == 5.941
    assert video["recent_loss_pct"] == 10.0
    assert video["recent_frames_received"] == 60
    assert video["recent_frames_decoded_per_second"] == 5.7
    assert video["recent_frames_dropped"] == 3
    assert stream["jitter_ms"] == 20.0
    assert stream["frames_per_second"] == 24.0
    assert stream["recent"]["packets_per_second"] == 9.0


def test_rtp_stats_sampler_reports_recent_outbound_bitrate():
    sampler = RtpStatsSampler()
    sampler.sample(
        [
            SimpleNamespace(
                type="outbound-rtp",
                id="outbound-video",
                ssrc=2,
                kind="video",
                packetsSent=100,
                bytesSent=1000,
                framesSent=25,
                framesEncoded=24,
            )
        ],
        now=10,
    )

    sample = sampler.sample(
        [
            SimpleNamespace(
                type="outbound-rtp",
                id="outbound-video",
                ssrc=2,
                kind="video",
                packetsSent=160,
                bytesSent=11000,
                framesSent=85,
                framesEncoded=82,
            )
        ],
        now=20,
    )

    video = sample["aggregate"]["outbound"]["video"]
    stream = sample["streams"][0]
    assert video["recent_packets_sent"] == 60
    assert video["recent_bytes_sent"] == 10000
    assert video["recent_bitrate_bps"] == 8000.0
    assert video["recent_frames_sent_per_second"] == 6.0
    assert video["recent_frames_encoded"] == 58
    assert stream["recent"]["bitrate_bps"] == 8000.0
