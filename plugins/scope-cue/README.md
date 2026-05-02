# Scope Cue Plugin

Local development plugin that exposes Cue session state as Scope graph outputs.

## Install for development

```bash
uv run daydream-scope install -e plugins/scope-cue
```

After Scope restarts, add the `Cue Session` custom node from the graph editor.
The node renders a Cue chat surface in Scope, loads a `.cue`/JSON file into the
node, and stores input/output mappings with the graph.

## Node

`cue.session` polls a Cue server session and emits:

- `prompt`: latest `video.update_prompt` payload prompt
- `reset`: latest prompt action reset flag
- `action_json`: full latest matching action as JSON
- `transcript`: current Cue transcript snapshot
- `status`: connection or polling status
- `tick`: incrementing trigger count when observed Cue state changes
- `chat_status`: manual chat / mapped input submission status
- `mapping_json`: current input/output mapping

The node also accepts `chat_in` and `context_json` string inputs. `chat_in`
becomes a Cue `transcript.segment`; `context_json` becomes the mapped context
observation type. The custom node UI uses Scope's `/api/v1/cue/...` proxy so the
desktop app can talk to a local Cue server without browser CORS failures.
