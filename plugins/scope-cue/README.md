# Scope Cue Plugin

Local development plugin that exposes a Cue session as a Scope director node.

## Install for development

```bash
uv run daydream-scope install -e plugins/scope-cue
```

After Scope restarts, add the `Cue Director` custom node from the graph editor.
The node keeps the Etherea-style chatbox as the main surface while presenting
Scope graph ports as Cue observations and emitted actions.

## Node

`cue.session` subscribes to Cue events, polls state as a fallback, and emits:

- `prompt`: latest `video.update_prompt` payload prompt
- `reset`: latest prompt action reset flag
- `action`: full latest Cue action as JSON
- `param_patch`: latest parameter patch payload when the action type targets params
- `shader_patch`: latest shader patch payload when the action type targets shaders
- `transcript`: current Cue transcript snapshot
- `source`: latest available Cue source/output payload
- `status`: connection or polling status
- `tick`: incrementing trigger count when observed Cue state changes
- `decision`: compact latest Cue decision summary

The visible inputs are `transcript`, `vision`, `signal`, `context`, `control`,
and `refresh`. The node still accepts the older `chat_in` and `context_json`
names internally for compatibility. The custom node UI uses Scope's
`/api/v1/cue/...` HTTP and WebSocket proxies so the desktop app can talk to a
local Cue server without browser CORS failures.

The chatbox can also open local device inputs. The mic button streams browser
audio chunks through `/api/v1/cue/sessions/:session_id/transcription`, which
proxies to Cue's `/sessions/:session_id/transcription` provider. The camera
button samples the webcam once per second and sends JPEG frames through
`/api/v1/cue/sessions/:session_id/vlm`, which proxies to Cue's
`/sessions/:session_id/vlm` provider for Moondream or another configured VLM.

Connection settings, action type, poll interval, raw mappings, and debug state
live inside the node's Tune/Trace drawers instead of appearing as primary Scope
fields.

The chat surface also carries Etherea-style style tags and reset. Style tags are
stored on the node as `style_tags_json` and posted to Cue as
`scope.style_tags` observations so the live director can treat them as current
operator intent. Reset posts a `scope.control` observation with `control:
"reset"` rather than only clearing the local UI, which lets Cue decide how to
translate the reset into downstream prompt or host actions.

## `.cue` manifests

The node reads Cue's live workflow manifest from
`/sessions/:session_id/agent` when the server exposes `workflowManifest`. That
server manifest is preferred because it reflects the running session rather than
a stale file. The upload button remains a fallback for offline inspection or
older Cue servers; loading a file stores the raw manifest in `cue_file_json`,
opens the Workflow drawer, and summarizes the workflow's prompts, cues, tools,
inputs, outputs, providers, runtime controls, style tags, and evals.

The active manifest is also used to infer the node's observation and output
mappings.

A useful manifest shape is:

```json
{
  "id": "etherea",
  "version": "v12_simple",
  "description": "Realtime video prompt director for Etherea.",
  "model": { "provider": "cerebras", "model": "qwen-3-235b-a22b-instruct-2507" },
  "prompts": {
    "system": "You generate video prompts for LongLive...",
    "userTemplate": "Your recent prompts... {{blocks}}"
  },
  "cues": [
    { "type": "punctuation" },
    { "type": "word_count", "threshold": 24 },
    { "type": "idle", "seconds": 45 }
  ],
  "tools": [
    {
      "name": "video.create_scene",
      "actionType": "video.update_prompt",
      "mappedReset": true,
      "description": "Create a new LongLive realtime video scene."
    },
    {
      "name": "video.update_scene",
      "actionType": "video.update_prompt",
      "mappedReset": false,
      "description": "Update the current LongLive scene without a hard cut."
    }
  ],
  "inputs": [
    { "name": "transcript", "type": "transcript.segment" },
    { "name": "vision", "type": "vision.description" },
    { "name": "control", "type": "scope.control" }
  ],
  "outputs": [
    { "id": "current_prompt", "kind": "text", "event": "prompt" },
    { "id": "etherea_video", "kind": "video", "provider": "scope.longlive" }
  ],
  "runtimeProfile": {
    "priorPromptCount": 3,
    "includeVisionFeedback": true,
    "updatePolicy": "Act on stable semantic turns, then pass through repetition."
  },
  "styleTags": ["cinematic", "organic motion"],
  "evals": ["poem", "stories", "action_quality", "moondream"]
}
```

This is still a manifest reader, not a runtime loader. The active Cue server
session remains the process running at `cue_base_url` and `session_id`.
