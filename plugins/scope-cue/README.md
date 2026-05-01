# Scope Cue Plugin

Local development plugin that exposes Cue session state as Scope graph outputs.

## Install for development

```bash
uv run daydream-scope install -e plugins/scope-cue
```

After Scope restarts, add the `Cue Session` custom node from the graph editor.

## Node

`cue.session` polls a Cue server session and emits:

- `prompt`: latest `video.update_prompt` payload prompt
- `reset`: latest prompt action reset flag
- `action_json`: full latest matching action as JSON
- `transcript`: current Cue transcript snapshot
- `status`: connection or polling status
- `tick`: incrementing trigger count when observed Cue state changes

This is intentionally conservative: it gives Scope a real Cue bridge without
forcing Scope to host Cue or decide Cue runtime policy.
