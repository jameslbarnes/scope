# Remote Stream Stability

This note tracks the local Scope branch used for the Etherea/LongLive remote
pod stream debugging work on 2026-05-08.

## Intent

The branch keeps the pod alive while improving visibility into the end-to-end
stream path:

1. LongLive pipeline batch production on the pod.
2. Pod WebRTC outbound stream.
3. Local Scope remote connection inbound stream.
4. Local cloud relay queue.
5. Browser-facing WebRTC session.

The goal is to distinguish model/pipeline stalls from WebRTC packet loss,
codec corruption, relay backpressure, and browser delivery problems.

## Difference From Vanilla Scope

- Adds stream telemetry logs prefixed with `[STREAM-TELEMETRY]`.
- Adds RTP stat aggregation for outbound, inbound, remote outbound, and remote
  inbound video streams.
- Extends `/api/v1/session/metrics` with session RTP, cloud relay, pipeline,
  and queue data.
- Adds LongLive pipeline batch telemetry: processing time, batch size,
  production FPS, playback FPS, cache reset state, queue sizes, and dropped
  frame counts.
- Adds cloud relay frame queue telemetry so latest-frame drops are visible.
- Adds remote Scope WebRTC telemetry for pod-to-local transport.
- Adds browser session WebRTC telemetry for local-to-browser transport.
- Adds parameter tracing tests and stream transport tests around the modified
  runtime behavior.
- Adds WebRTC helpers for TURN relay policy, RTP statistics, and VP8 recovery.

## Known Good Live Settings

Live pod observed during this work:

- RunPod pod: `05tq2ievtpu6qb`
- Proxy URL: `https://05tq2ievtpu6qb-8000.proxy.runpod.net`
- Local Scope server: `http://127.0.0.1:52178`
- Local remote connection: `remote-scope-3e95b874d5e7`
- Browser/session observed: `7009ff7d-f65d-4619-902d-e3876e05c8be`
- Pod-side Scope session observed: `09a5e3d9-1a80-400c-8e79-f174dec7c9a1`

Local Scope environment used for the stable run:

```bash
SCOPE_REMOTE_SCOPE_FORCE_TURN_RELAY=0
SCOPE_REMOTE_SCOPE_PREFER_H264=1
SCOPE_WEBRTC_FORCE_TURN_RELAY=0
SCOPE_WEBRTC_PREFER_H264=0
SCOPE_WEBRTC_DEFAULT_BITRATE=8000000
SCOPE_WEBRTC_MIN_BITRATE=4000000
SCOPE_WEBRTC_MAX_BITRATE=12000000
SCOPE_WEBRTC_MAX_FRAME_RATE_HINT=30
SCOPE_WEBRTC_VP8_PACKET_MAX=1100
SCOPE_WEBRTC_VP8_KEYFRAME_INTERVAL_FRAMES=30
```

## What The Logs Showed

After the telemetry patch and restart, the stream showed:

- Browser-facing WebRTC connected with ICE `completed`.
- Local output around `27.5 fps`.
- Pod-side output around `28 fps`.
- Browser-facing recent packet loss at `0.0%` in sampled metrics.
- Pod LongLive batch production usually above playback rate.
- Intermittent H264 decoder warnings still present.
- Occasional cloud relay latest-frame drops from a queue of size 2.
- Some cumulative pod-to-local packet loss, roughly `0.24%` to `0.5%` in
  observed samples.

The logging did not show a major regression caused by telemetry itself. The
remaining risk is transport/queue headroom under changing network conditions.

## Useful Commands

Current local metrics:

```bash
curl -sS http://127.0.0.1:52178/api/v1/session/metrics | python3 -m json.tool
```

Tail local stream telemetry:

```bash
tail -f /Users/etherealmachine/.daydream-scope/logs/scope-logs-*.log | grep STREAM-TELEMETRY
```

Tail pod stream telemetry:

```bash
ssh -tt -o StrictHostKeyChecking=no -i ~/.ssh/id_ed25519 \
  05tq2ievtpu6qb-64410e0a@ssh.runpod.io \
  'tail -f /workspace/logs/scope-logs-2026-05-08-17-19-38.log | grep STREAM-TELEMETRY'
```

Validation used before committing:

```bash
uv run pytest tests/test_webrtc_rtp_stats.py tests/test_pipeline_parameters.py tests/test_webrtc_parameters.py
uv run ruff check src/scope/server/stream_telemetry.py src/scope/server/webrtc_rtp_stats.py src/scope/server/pipeline_processor.py src/scope/server/frame_processor.py src/scope/server/cloud_relay.py src/scope/server/remote_scope.py src/scope/server/webrtc.py src/scope/server/app.py src/scope/server/mcp_router.py tests/test_webrtc_rtp_stats.py tests/test_pipeline_parameters.py
python3 -m py_compile src/scope/server/stream_telemetry.py src/scope/server/webrtc_rtp_stats.py src/scope/server/pipeline_processor.py src/scope/server/frame_processor.py src/scope/server/cloud_relay.py src/scope/server/remote_scope.py src/scope/server/webrtc.py src/scope/server/mcp_router.py
```
