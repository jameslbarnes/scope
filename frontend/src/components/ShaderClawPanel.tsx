import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import type { ReactNode } from "react";
import { RefreshCw } from "lucide-react";

import { Button } from "./ui/button";
import { Input } from "./ui/input";
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "./ui/select";
import { Switch } from "./ui/switch";
import { ShaderClawPreviewCanvas } from "./ShaderClawPreviewCanvas";

export const SHADERCLAW_MANAGED_FIELD_KEYS = new Set([
  "shader",
  "parameters_json",
  "reload_token",
]);

interface ShaderClawPanelProps {
  values?: Record<string, unknown>;
  onChange?: (key: string, value: unknown, isRuntimeParam?: boolean) => void;
  disabled?: boolean;
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

async function apiPost<T>(
  path: string,
  body: Record<string, unknown>
): Promise<T> {
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

async function apiGet<T>(
  path: string,
  params: Record<string, string>
): Promise<T> {
  const searchParams = new URLSearchParams(params);
  const response = await fetch(`${path}?${searchParams}`);
  const data = (await response.json().catch(() => null)) as
    | ({ detail?: string } & T)
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

function valueForInput(
  input: ShaderClawInput,
  parameters: Record<string, unknown>
): unknown {
  if (Object.prototype.hasOwnProperty.call(parameters, input.name)) {
    return parameters[input.name];
  }
  if (input.value !== undefined) return input.value;
  if (input.default !== undefined) return input.default;
  if (input.type === "bool") return false;
  if (input.type === "color") return [1, 1, 1, 1];
  if (input.type === "float" || input.type === "long") return 0;
  return "";
}

export function ShaderClawPanel({
  values = {},
  onChange,
  disabled = false,
}: ShaderClawPanelProps) {
  const shadersDir = String(values.shaders_dir || "");
  const shader = String(values.shader || DEFAULT_SHADER);
  const previewSize = previewDimensions(
    numberValue(values.width, 1280),
    numberValue(values.height, 720)
  );
  const parameters = useMemo(
    () => parseParameterJson(values.parameters_json),
    [values.parameters_json]
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
    apiGet<ManifestResponse>(
      "/api/v1/shaderclaw/manifest",
      shadersDir ? { shaders_dir: shadersDir } : {}
    )
      .then(data => {
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
    onChange?.("shader", value, true);
  };

  const setInputValue = async (input: ShaderClawInput, value: unknown) => {
    const nextParameters = {
      ...parametersRef.current,
      [input.name]: value,
    };
    parametersRef.current = nextParameters;
    onChange?.("parameters_json", JSON.stringify(nextParameters), true);
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
    onChange?.("reload_token", Date.now(), true);
    setStreamNonce(value => value + 1);
    void loadShader();
  };

  return (
    <div className="space-y-3 rounded-md border border-border bg-muted/20 p-3">
      <div className="flex items-center justify-between gap-2">
        <div className="min-w-0">
          <h3 className="truncate text-sm font-medium">ShaderClaw</h3>
          {error && (
            <div className="truncate text-xs text-amber-500" title={error}>
              {error}
            </div>
          )}
        </div>
        <Button
          type="button"
          variant="outline"
          size="icon"
          onClick={reload}
          disabled={disabled || busy}
          title="Reload ShaderClaw shader"
          className="h-8 w-8 shrink-0"
        >
          <RefreshCw className={busy ? "h-4 w-4 animate-spin" : "h-4 w-4"} />
        </Button>
      </div>

      <ShaderClawPreviewCanvas
        shadersDir={shadersDir}
        shader={shader}
        parameters={parameters}
        width={previewSize.width}
        height={previewSize.height}
        nonce={streamNonce}
        onError={setStreamError}
      />

      <ControlRow label="Shader">
        <Select
          value={shader}
          onValueChange={setShader}
          disabled={disabled || busy}
        >
          <SelectTrigger className="h-8">
            <SelectValue placeholder="Shader" />
          </SelectTrigger>
          <SelectContent>
            {shaderOptions.map(option => (
              <SelectItem key={option.value} value={option.value}>
                {option.label}
              </SelectItem>
            ))}
          </SelectContent>
        </Select>
      </ControlRow>

      <div className="space-y-2">
        {inputs
          .filter(input => input.type !== "image")
          .map(input => (
            <ShaderClawInputRow
              key={input.name}
              input={input}
              value={valueForInput(input, parameters)}
              disabled={disabled || busy}
              onChange={value => void setInputValue(input, value)}
            />
          ))}
      </div>
    </div>
  );
}

function ControlRow({
  label,
  children,
}: {
  label: string;
  children: ReactNode;
}) {
  return (
    <div className="grid grid-cols-[minmax(0,7rem)_minmax(0,1fr)] items-center gap-2">
      <span
        className="truncate text-xs font-medium text-muted-foreground"
        title={label}
      >
        {label}
      </span>
      <div className="min-w-0">{children}</div>
    </div>
  );
}

function ShaderClawInputRow({
  input,
  value,
  disabled,
  onChange,
}: {
  input: ShaderClawInput;
  value: unknown;
  disabled: boolean;
  onChange: (value: unknown) => void;
}) {
  if (
    input.type === "long" &&
    Array.isArray(input.values) &&
    input.values.length > 0
  ) {
    return (
      <ControlRow label={inputLabel(input)}>
        <Select
          value={String(value ?? input.default ?? input.values[0])}
          onValueChange={next => {
            const numeric = Number(next);
            onChange(Number.isNaN(numeric) ? next : numeric);
          }}
          disabled={disabled}
        >
          <SelectTrigger className="h-8">
            <SelectValue />
          </SelectTrigger>
          <SelectContent>
            {input.values.map((option, index) => (
              <SelectItem key={String(option)} value={String(option)}>
                {String(input.labels?.[index] ?? option)}
              </SelectItem>
            ))}
          </SelectContent>
        </Select>
      </ControlRow>
    );
  }

  if (input.type === "bool") {
    return (
      <ControlRow label={inputLabel(input)}>
        <div className="flex h-8 items-center justify-end">
          <Switch
            checked={Boolean(value)}
            onCheckedChange={onChange}
            disabled={disabled}
          />
        </div>
      </ControlRow>
    );
  }

  if (input.type === "color") {
    const rgba = Array.isArray(value) ? value : input.default;
    const alpha = Array.isArray(rgba) ? numberValue(rgba[3], 1) : 1;
    return (
      <ControlRow label={inputLabel(input)}>
        <div className="flex min-w-0 gap-2">
          <input
            type="color"
            value={valueToHexColor(rgba)}
            onChange={event => onChange(hexToColor(event.target.value, alpha))}
            disabled={disabled}
            className="h-8 w-12 shrink-0 rounded-md border border-input bg-background"
            title={inputLabel(input)}
          />
          <Input
            type="number"
            value={alpha}
            min={0}
            max={1}
            step={0.01}
            onChange={event =>
              onChange(
                hexToColor(
                  valueToHexColor(rgba),
                  numberValue(event.target.value, alpha)
                )
              )
            }
            disabled={disabled}
            className="h-8 min-w-0"
          />
        </div>
      </ControlRow>
    );
  }

  if (Array.isArray(value)) {
    const values = value.map(item => numberValue(item));
    return (
      <ControlRow label={inputLabel(input)}>
        <div className="grid min-w-0 grid-cols-2 gap-2">
          {values.slice(0, 4).map((item, index) => (
            <Input
              key={index}
              type="number"
              value={item}
              min={0}
              max={1}
              step={0.01}
              onChange={event => {
                const updated = [...values];
                updated[index] = numberValue(event.target.value, item);
                onChange(updated);
              }}
              disabled={disabled}
              className="h-8"
            />
          ))}
        </div>
      </ControlRow>
    );
  }

  if (input.type === "float" || input.type === "long") {
    return (
      <ControlRow label={inputLabel(input)}>
        <Input
          type="number"
          value={numberValue(value, numberValue(input.default))}
          min={input.min}
          max={input.max}
          step={input.type === "long" ? 1 : 0.01}
          onChange={event => {
            const next = Number(event.target.value);
            if (Number.isFinite(next)) onChange(next);
          }}
          disabled={disabled}
          className="h-8"
        />
      </ControlRow>
    );
  }

  if (input.type === "text") {
    return (
      <ControlRow label={inputLabel(input)}>
        <Input
          type="text"
          value={String(value ?? "")}
          onChange={event => onChange(event.target.value)}
          disabled={disabled}
          className="h-8"
        />
      </ControlRow>
    );
  }

  return (
    <ControlRow label={inputLabel(input)}>
      <Input value={String(value ?? "")} readOnly disabled className="h-8" />
    </ControlRow>
  );
}
