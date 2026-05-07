import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import { RefreshCw } from "lucide-react";
import {
  NodeParamRow,
  NodePill,
  NodePillInput,
  NodePillSearchableSelect,
  NodePillSelect,
  NodePillToggle,
  NODE_TOKENS,
} from "../ui";
import { ShaderClawPreviewCanvas } from "../../ShaderClawPreviewCanvas";

interface ShaderClawWidgetProps {
  nodeId: string;
  parameterValues: Record<string, unknown>;
  onParameterChange?: (nodeId: string, key: string, value: unknown) => void;
}

interface ShaderManifestEntry {
  id?: number | string | null;
  title?: string;
  file?: string;
  hidden?: boolean;
}

interface ShaderClawInput {
  name: string;
  type: string;
  value?: unknown;
  default?: unknown;
  min?: number;
  max?: number;
  values?: unknown[];
  labels?: string[];
}

interface ManifestResponse {
  shaders: ShaderManifestEntry[];
}

interface LoadResponse {
  inputs: ShaderClawInput[];
}

const DEFAULT_SHADER = "Gradient";

function errorMessage(error: unknown): string {
  return error instanceof Error ? error.message : String(error);
}

async function apiPost<T>(path: string, body: Record<string, unknown>): Promise<T> {
  const response = await fetch(path, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(body),
  });
  const data = (await response.json().catch(() => null)) as
    | { detail?: string }
    | null;
  if (!response.ok) {
    throw new Error(data?.detail || response.statusText);
  }
  return data as T;
}

function parseParameterJson(value: unknown): Record<string, unknown> {
  if (typeof value !== "string" || value.trim() === "") return {};
  try {
    const parsed = JSON.parse(value) as unknown;
    return parsed && typeof parsed === "object" && !Array.isArray(parsed)
      ? (parsed as Record<string, unknown>)
      : {};
  } catch {
    return {};
  }
}

function clamp01(value: number): number {
  return Math.max(0, Math.min(1, value));
}

function numberValue(value: unknown, fallback = 0): number {
  if (typeof value === "number" && Number.isFinite(value)) return value;
  if (typeof value === "string" && value.trim() !== "") {
    const parsed = Number(value);
    if (Number.isFinite(parsed)) return parsed;
  }
  return fallback;
}

function valueToHexColor(value: unknown): string {
  const rgba = Array.isArray(value) ? value : [1, 1, 1, 1];
  const channels = [0, 1, 2].map(index => {
    const channel = clamp01(numberValue(rgba[index], 1));
    return Math.round(channel * 255)
      .toString(16)
      .padStart(2, "0");
  });
  return `#${channels.join("")}`;
}

function hexToColor(hex: string, alpha: number): number[] {
  const normalized = hex.replace("#", "");
  const r = parseInt(normalized.slice(0, 2), 16) / 255;
  const g = parseInt(normalized.slice(2, 4), 16) / 255;
  const b = parseInt(normalized.slice(4, 6), 16) / 255;
  return [r, g, b, clamp01(alpha)];
}

function inputLabel(input: ShaderClawInput): string {
  return input.name.replace(/_/g, " ");
}

function previewDimensions(width: number, height: number) {
  const maxWidth = 480;
  const scale = width > maxWidth ? maxWidth / width : 1;
  return {
    width: Math.max(1, Math.round(width * scale)),
    height: Math.max(1, Math.round(height * scale)),
  };
}

export function ShaderClawWidget({
  nodeId,
  parameterValues,
  onParameterChange,
}: ShaderClawWidgetProps) {
  const shadersDir = String(parameterValues.shaders_dir || "");
  const shader = String(parameterValues.shader || DEFAULT_SHADER);
  const previewSize = previewDimensions(
    numberValue(parameterValues.width, 1280),
    numberValue(parameterValues.height, 720)
  );
  const parameters = useMemo(
    () => parseParameterJson(parameterValues.parameters_json),
    [parameterValues.parameters_json]
  );
  const parametersRef = useRef<Record<string, unknown>>(parameters);
  const [manifest, setManifest] = useState<ShaderManifestEntry[]>([]);
  const [inputs, setInputs] = useState<ShaderClawInput[]>([]);
  const [busy, setBusy] = useState(false);
  const [controlError, setControlError] = useState<string | null>(null);
  const [streamError, setStreamError] = useState<string | null>(null);
  const [streamNonce, setStreamNonce] = useState(0);
  const error = controlError || streamError;

  useEffect(() => {
    parametersRef.current = parameters;
  }, [parameters]);

  const loadShader = useCallback(async () => {
    setBusy(true);
    setControlError(null);
    try {
      const result = await apiPost<LoadResponse>("/api/v1/shaderclaw/load", {
        shaders_dir: shadersDir || null,
        shader,
        parameters: parametersRef.current,
      });
      setInputs(result.inputs || []);
    } catch (err) {
      setControlError(errorMessage(err));
    } finally {
      setBusy(false);
    }
  }, [shader, shadersDir]);

  useEffect(() => {
    let cancelled = false;
    const params = new URLSearchParams();
    if (shadersDir) params.set("shaders_dir", shadersDir);
    fetch(`/api/v1/shaderclaw/manifest?${params}`)
      .then(response => response.json())
      .then((data: ManifestResponse) => {
        if (!cancelled) setManifest(data.shaders || []);
      })
      .catch(err => {
        if (!cancelled) setControlError(errorMessage(err));
      });
    return () => {
      cancelled = true;
    };
  }, [shadersDir]);

  useEffect(() => {
    void loadShader();
  }, [loadShader]);

  const shaderOptions = useMemo(
    () => {
      const options = manifest
        .filter(entry => !entry.hidden && (entry.title || entry.file))
        .map(entry => ({
          value: String(entry.title || entry.file),
          label: String(entry.title || entry.file),
        }));
      return options.some(option => option.value === shader)
        ? options
        : [{ value: shader, label: shader }, ...options];
    },
    [manifest, shader]
  );

  const setShader = (value: string) => {
    onParameterChange?.(nodeId, "shader", value);
  };

  const setInputValue = async (input: ShaderClawInput, value: unknown) => {
    const nextParameters = {
      ...parametersRef.current,
      [input.name]: value,
    };
    parametersRef.current = nextParameters;
    onParameterChange?.(
      nodeId,
      "parameters_json",
      JSON.stringify(nextParameters)
    );
    setInputs(current =>
      current.map(item =>
        item.name === input.name ? { ...item, value } : item
      )
    );
    try {
      await apiPost<LoadResponse>("/api/v1/shaderclaw/parameter", {
        shaders_dir: shadersDir || null,
        shader,
        parameters: nextParameters,
        name: input.name,
        value,
      });
      setControlError(null);
    } catch (err) {
      setControlError(errorMessage(err));
    }
  };

  const reload = () => {
    onParameterChange?.(nodeId, "reload_token", Date.now());
    setStreamNonce(value => value + 1);
    void loadShader();
  };

  return (
    <div className="space-y-2 rounded-md border border-[rgba(255,255,255,0.06)] bg-[rgba(255,255,255,0.02)] p-2">
      <div className="aspect-video overflow-hidden rounded bg-black">
        <ShaderClawPreviewCanvas
          shadersDir={shadersDir}
          shader={shader}
          parameters={parameters}
          width={previewSize.width}
          height={previewSize.height}
          nonce={streamNonce}
          className="relative h-full w-full bg-black"
          onError={setStreamError}
        />
      </div>

      <NodeParamRow label="Shader">
        <NodePillSearchableSelect
          value={shader}
          onChange={setShader}
          options={shaderOptions}
          placeholder="Shader"
          disabled={busy}
        />
      </NodeParamRow>

      <NodeParamRow label="Reload">
        <button
          type="button"
          onClick={reload}
          disabled={busy}
          className={`${NODE_TOKENS.pill} flex w-full items-center justify-center gap-1 cursor-pointer hover:bg-[#2a2a2a] active:bg-[#333] transition-colors disabled:opacity-50`}
          title="Reload ShaderClaw shader"
        >
          <RefreshCw className="h-3 w-3 text-[#fafafa]" />
          <span className={NODE_TOKENS.primaryText}>
            {busy ? "Loading" : "Reload"}
          </span>
        </button>
      </NodeParamRow>

      {inputs
        .filter(input => input.type !== "image")
        .map(input => (
          <ShaderClawInputRow
            key={input.name}
            input={input}
            value={
              Object.prototype.hasOwnProperty.call(parameters, input.name)
                ? parameters[input.name]
                : input.value ?? input.default
            }
            onChange={value => void setInputValue(input, value)}
          />
        ))}

      {error && (
        <div className="truncate text-[10px] text-amber-400" title={error}>
          {error}
        </div>
      )}
    </div>
  );
}

function ShaderClawInputRow({
  input,
  value,
  onChange,
}: {
  input: ShaderClawInput;
  value: unknown;
  onChange: (value: unknown) => void;
}) {
  if (
    input.type === "long" &&
    Array.isArray(input.values) &&
    input.values.length > 0
  ) {
    return (
      <NodeParamRow label={inputLabel(input)}>
        <NodePillSelect
          value={String(value ?? input.default ?? input.values[0])}
          onChange={next => {
            const numeric = Number(next);
            onChange(Number.isNaN(numeric) ? next : numeric);
          }}
          options={input.values.map((option, index) => ({
            value: String(option),
            label: String(input.labels?.[index] ?? option),
          }))}
        />
      </NodeParamRow>
    );
  }

  if (input.type === "bool") {
    return (
      <NodeParamRow label={inputLabel(input)}>
        <NodePillToggle
          checked={Boolean(value)}
          onChange={checked => onChange(checked)}
        />
      </NodeParamRow>
    );
  }

  if (input.type === "color") {
    const rgba = Array.isArray(value) ? value : input.default;
    const alpha = Array.isArray(rgba) ? numberValue(rgba[3], 1) : 1;
    return (
      <NodeParamRow label={inputLabel(input)}>
        <div className="flex min-w-0 gap-1">
          <input
            type="color"
            value={valueToHexColor(rgba)}
            onChange={event => onChange(hexToColor(event.target.value, alpha))}
            className="nodrag h-[18px] w-8 shrink-0 rounded border border-[rgba(255,255,255,0.08)] bg-transparent"
            title={inputLabel(input)}
          />
          <NodePillInput
            type="number"
            value={alpha}
            min={0}
            max={1}
            step={0.01}
            onChange={next =>
              onChange(hexToColor(valueToHexColor(rgba), Number(next)))
            }
          />
        </div>
      </NodeParamRow>
    );
  }

  if (Array.isArray(value)) {
    const values = value.map(item => numberValue(item));
    return (
      <NodeParamRow label={inputLabel(input)}>
        <div className="grid min-w-0 grid-cols-2 gap-1">
          {values.slice(0, 2).map((item, index) => (
            <NodePillInput
              key={index}
              type="number"
              value={item}
              min={0}
              max={1}
              step={0.01}
              onChange={next => {
                const updated = [...values];
                updated[index] = Number(next);
                onChange(updated);
              }}
            />
          ))}
        </div>
      </NodeParamRow>
    );
  }

  if (input.type === "float" || input.type === "long") {
    return (
      <NodeParamRow label={inputLabel(input)}>
        <NodePillInput
          type="number"
          value={numberValue(value, numberValue(input.default))}
          min={input.min}
          max={input.max}
          step={input.type === "long" ? 1 : 0.01}
          onChange={next => onChange(Number(next))}
        />
      </NodeParamRow>
    );
  }

  if (input.type === "text") {
    return (
      <NodeParamRow label={inputLabel(input)}>
        <NodePillInput
          type="text"
          value={String(value ?? "")}
          onChange={next => onChange(String(next))}
        />
      </NodeParamRow>
    );
  }

  return (
    <NodeParamRow label={inputLabel(input)}>
      <NodePill>{String(value ?? "")}</NodePill>
    </NodeParamRow>
  );
}
