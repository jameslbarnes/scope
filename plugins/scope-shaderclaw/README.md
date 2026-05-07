# Scope ShaderClaw Plugin

Local development plugin that renders ShaderClaw3 ISF shader files natively as
a Scope video source. It does not require the ShaderClaw browser renderer.

## Install for development

```bash
uv run daydream-scope install -e plugins/scope-shaderclaw
```

The plugin looks for ShaderClaw shaders in these locations:

- `SCOPE_SHADERCLAW_SHADERS_DIR` or `SHADERCLAW_SHADERS_DIR`
- `SCOPE_SHADERCLAW_REPO` or `SHADERCLAW_REPO`
- `../shader-claw3/shaders`
- `~/shader-claw3/shaders`

## Pipeline

`shaderclaw-3` is a text-mode video source. It reads
`shaders/manifest.json`, compiles the selected `.fs` shader in a native
offscreen OpenGL context, applies parameter values, and returns the rendered
frame to Scope as video.

Important fields:

- `shaders_dir`: optional ShaderClaw repo path or `shaders` directory
- `shader`: Shader title, numeric manifest id, or shader file name
- `parameters_json`: JSON object of ShaderClaw parameter names to values
- `reload_token`: increment this to force a shader reload with the same values

Example `parameters_json`:

```json
{"speed": 0.35, "Color1": [1, 0.2, 0.1, 1], "transparentBg": false}
```

Scope can route the pipeline output to its normal NDI, Syphon, recording, or
graph sinks.
