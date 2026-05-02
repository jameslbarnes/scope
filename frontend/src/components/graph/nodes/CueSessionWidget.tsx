import { useEffect, useMemo, useRef, useState } from "react";
import {
  FileText,
  Link2,
  Mic,
  Radio,
  Send,
  Trash2,
  Upload,
} from "lucide-react";
import type { FlowNodeData } from "../../../lib/graphUtils";
import type { NodeParamDef, NodePortDef } from "../../../lib/api";
import { getCueSessionState, postCueObservation } from "../../../lib/api";

interface CueSessionWidgetProps {
  data: FlowNodeData;
  params: NodeParamDef[];
  outputs: NodePortDef[];
  setParam: (name: string, value: unknown) => void;
}

interface ChatMessage {
  id: string;
  role: "user" | "cue" | "prompt" | "status";
  text: string;
  meta: string;
}

type JsonRecord = Record<string, unknown>;

const DEFAULT_BASE_URL = "http://127.0.0.1:8792";
const DEFAULT_SESSION_ID = "demo";
const DEFAULT_INPUT_MAPPING: Record<string, string> = {
  chat: "transcript.segment",
  chat_in: "transcript.segment",
  context_json: "scope.context",
};
const DEFAULT_OUTPUT_MAPPING: Record<string, string> = {
  prompt: "longlive.prompt",
  transcript: "chat.transcript",
  action_json: "cue.action",
};

export function CueSessionWidget({
  data,
  params,
  outputs,
  setParam,
}: CueSessionWidgetProps) {
  const fileInputRef = useRef<HTMLInputElement>(null);
  const [state, setState] = useState<JsonRecord | null>(null);
  const [status, setStatus] = useState("waiting");
  const [draft, setDraft] = useState("");
  const [localMessages, setLocalMessages] = useState<ChatMessage[]>([]);
  const [micActive, setMicActive] = useState(false);

  const baseUrl = stringParam(data, params, "cue_base_url", DEFAULT_BASE_URL);
  const sessionId = stringParam(data, params, "session_id", DEFAULT_SESSION_ID);
  const timeoutMs = numberParam(data, params, "timeout_ms", 1000);
  const enabled = booleanParam(data, params, "enabled", true);
  const cueFilePath = stringParam(data, params, "cue_file_path", "");
  const cueFileJson = stringParam(data, params, "cue_file_json", "");
  const inputMapping = useMemo(
    () =>
      parseMapping(
        stringParam(data, params, "input_mapping_json", ""),
        DEFAULT_INPUT_MAPPING
      ),
    [data, params]
  );
  const outputMapping = useMemo(
    () =>
      parseMapping(
        stringParam(data, params, "output_mapping_json", ""),
        DEFAULT_OUTPUT_MAPPING
      ),
    [data, params]
  );

  useEffect(() => {
    if (!enabled) {
      setStatus("disabled");
      return;
    }
    let cancelled = false;
    let timer: ReturnType<typeof setInterval> | undefined;

    const refresh = async () => {
      try {
        const nextState = await getCueSessionState({
          baseUrl,
          sessionId,
          timeoutMs,
        });
        if (cancelled) return;
        setState(nextState);
        setStatus("live");
      } catch (error) {
        if (cancelled) return;
        setStatus(error instanceof Error ? error.message : "Cue unavailable");
      }
    };

    void refresh();
    timer = setInterval(refresh, 1000);

    return () => {
      cancelled = true;
      if (timer) clearInterval(timer);
    };
  }, [baseUrl, enabled, sessionId, timeoutMs]);

  const remoteMessages = useMemo(() => messagesFromState(state), [state]);
  const messages = useMemo(
    () => [...localMessages, ...remoteMessages].slice(0, 8),
    [localMessages, remoteMessages]
  );

  const submitChat = async () => {
    const text = draft.trim();
    if (!text) return;
    const message: ChatMessage = {
      id: `local-${Date.now()}`,
      role: "user",
      text,
      meta: "now",
    };
    setLocalMessages(prev => [message, ...prev].slice(0, 4));
    setDraft("");
    setParam("chat_text", text);

    try {
      await postCueObservation({
        baseUrl,
        sessionId,
        timeoutMs,
        observation: transcriptObservation(text),
      });
      setStatus("sent");
    } catch (error) {
      setStatus(error instanceof Error ? error.message : "send failed");
    }
  };

  const loadCueFile = async (file: File) => {
    const text = await file.text();
    setParam("cue_file_path", file.name);
    setParam("cue_file_json", text);

    const decoded = parseJsonObject(text);
    if (!decoded) return;

    const inferredInputs = inferNames(decoded, ["inputs", "sources"]);
    if (inferredInputs.length > 0) {
      setParam(
        "input_mapping_json",
        prettyJson({
          ...inputMapping,
          ...Object.fromEntries(
            inferredInputs.map(name => [
              name,
              name.toLowerCase().includes("chat") ||
              name.toLowerCase().includes("transcript")
                ? "transcript.segment"
                : "scope.context",
            ])
          ),
        })
      );
    }

    const inferredOutputs = inferNames(decoded, ["outputs", "actions", "tools"]);
    if (inferredOutputs.length > 0) {
      setParam(
        "output_mapping_json",
        prettyJson({
          ...outputMapping,
          ...Object.fromEntries(
            inferredOutputs.map(name => [name, outputMapping[name] ?? name])
          ),
        })
      );
    }
  };

  const updateMapping = (
    paramName: "input_mapping_json" | "output_mapping_json",
    mapping: Record<string, string>,
    key: string,
    value: string
  ) => {
    setParam(paramName, prettyJson({ ...mapping, [key]: value }));
  };

  return (
    <div className="nodrag nowheel flex w-[360px] flex-col gap-2 rounded-md border border-white/5 bg-[#161616] p-2 text-[10px] text-[#e8e8e8]">
      <div className="flex items-center justify-between">
        <div className="flex items-center gap-1.5">
          <span className="text-[12px] font-semibold text-[#f0f0f0]">Chat</span>
          <span
            className={`inline-flex items-center gap-1 rounded-full px-1.5 py-0.5 text-[9px] ${
              status === "live" || status === "sent"
                ? "bg-emerald-500/10 text-emerald-300"
                : "bg-white/5 text-[#9ca3af]"
            }`}
            title={status}
          >
            <Radio size={10} />
            {status === "live" || status === "sent" ? "Live" : "Idle"}
          </span>
        </div>
        <div className="flex items-center gap-1">
          <button
            type="button"
            className="rounded-md p-1 text-[#9ca3af] hover:bg-white/10 hover:text-white"
            title="Load .cue file"
            onClick={() => fileInputRef.current?.click()}
          >
            <Upload size={13} />
          </button>
          <button
            type="button"
            className="rounded-md p-1 text-[#9ca3af] hover:bg-white/10 hover:text-white"
            title="Clear local chat"
            onClick={() => setLocalMessages([])}
          >
            <Trash2 size={13} />
          </button>
        </div>
      </div>

      <input
        ref={fileInputRef}
        type="file"
        accept=".cue,.json,application/json,text/plain"
        className="hidden"
        onChange={event => {
          const file = event.currentTarget.files?.[0];
          event.currentTarget.value = "";
          if (file) void loadCueFile(file);
        }}
      />

      <div className="flex items-center gap-1.5 rounded-md border border-white/5 bg-[#1d1d1d] px-2 py-1 text-[#9ca3af]">
        <FileText size={12} />
        <span className="truncate">
          {cueFilePath || (cueFileJson ? "Loaded cue file" : "No cue file")}
        </span>
      </div>

      <div className="flex max-h-[160px] min-h-[120px] flex-col-reverse gap-1 overflow-y-auto rounded-md border border-white/5 bg-[#101010] p-1.5">
        {messages.length === 0 ? (
          <div className="rounded-md border border-dashed border-white/10 px-2 py-5 text-center text-[#737373]">
            Waiting for Cue
          </div>
        ) : (
          messages.map(message => (
            <div
              key={message.id}
              className={`rounded-md border px-2 py-1.5 ${
                message.role === "user"
                  ? "border-sky-400/15 bg-sky-400/10"
                  : message.role === "prompt"
                    ? "border-emerald-400/15 bg-emerald-400/10"
                    : "border-white/5 bg-white/[0.04]"
              }`}
            >
              <div className="line-clamp-3 text-[10px] leading-snug text-[#f2f2f2]">
                {message.text}
              </div>
              <div className="mt-1 text-[9px] uppercase tracking-wider text-[#737373]">
                {message.meta}
              </div>
            </div>
          ))
        )}
      </div>

      <div className="flex items-center gap-1 rounded-md border border-white/5 bg-[#1d1d1d] p-1">
        <input
          value={draft}
          onChange={event => setDraft(event.target.value)}
          onKeyDown={event => {
            if (event.key === "Enter" && !event.shiftKey) {
              event.preventDefault();
              void submitChat();
            }
          }}
          className="min-w-0 flex-1 bg-transparent px-1.5 py-1 text-[10px] text-white outline-none placeholder:text-[#737373]"
          placeholder="Speak your vision..."
        />
        <button
          type="button"
          className={`rounded-md p-1.5 ${
            micActive
              ? "bg-red-500/15 text-red-300"
              : "text-[#9ca3af] hover:bg-white/10 hover:text-white"
          }`}
          title="Voice input"
          onClick={() => setMicActive(prev => !prev)}
        >
          <Mic size={13} />
        </button>
        <button
          type="button"
          className="rounded-md bg-emerald-500/15 p-1.5 text-emerald-300 hover:bg-emerald-500/25"
          title="Send"
          onClick={() => void submitChat()}
        >
          <Send size={13} />
        </button>
      </div>

      <div className="grid grid-cols-2 gap-2">
        <MappingColumn
          title="Inputs"
          mapping={inputMapping}
          onChange={(key, value) =>
            updateMapping("input_mapping_json", inputMapping, key, value)
          }
        />
        <MappingColumn
          title="Outputs"
          mapping={withOutputDefaults(outputMapping, outputs)}
          onChange={(key, value) =>
            updateMapping("output_mapping_json", outputMapping, key, value)
          }
        />
      </div>

      <div className="flex items-center gap-1 truncate text-[9px] text-[#737373]">
        <Link2 size={10} />
        <span className="truncate">
          {baseUrl} / {sessionId}
        </span>
      </div>
    </div>
  );
}

function MappingColumn({
  title,
  mapping,
  onChange,
}: {
  title: string;
  mapping: Record<string, string>;
  onChange: (key: string, value: string) => void;
}) {
  const entries = Object.entries(mapping).slice(0, 5);
  return (
    <div className="min-w-0 rounded-md border border-white/5 bg-[#101010] p-1.5">
      <div className="mb-1 text-[9px] font-semibold uppercase tracking-wider text-[#737373]">
        {title}
      </div>
      <div className="flex flex-col gap-1">
        {entries.map(([key, value]) => (
          <label key={key} className="grid grid-cols-[0.8fr_1fr] items-center gap-1">
            <span className="truncate text-[#9ca3af]" title={key}>
              {key}
            </span>
            <input
              value={value}
              onChange={event => onChange(key, event.target.value)}
              className="min-w-0 rounded border border-white/5 bg-[#1d1d1d] px-1.5 py-0.5 text-[9px] text-white outline-none focus:border-sky-400/50"
            />
          </label>
        ))}
      </div>
    </div>
  );
}

function stringParam(
  data: FlowNodeData,
  params: NodeParamDef[],
  name: string,
  fallback: string
): string {
  const value = data.customNodeParams?.[name];
  if (typeof value === "string") return value;
  const def = params.find(param => param.name === name)?.default;
  return typeof def === "string" ? def : fallback;
}

function numberParam(
  data: FlowNodeData,
  params: NodeParamDef[],
  name: string,
  fallback: number
): number {
  const value = data.customNodeParams?.[name];
  if (typeof value === "number") return value;
  if (typeof value === "string" && value.trim()) {
    const parsed = Number(value);
    if (!Number.isNaN(parsed)) return parsed;
  }
  const def = params.find(param => param.name === name)?.default;
  return typeof def === "number" ? def : fallback;
}

function booleanParam(
  data: FlowNodeData,
  params: NodeParamDef[],
  name: string,
  fallback: boolean
): boolean {
  const value = data.customNodeParams?.[name];
  if (typeof value === "boolean") return value;
  const def = params.find(param => param.name === name)?.default;
  return typeof def === "boolean" ? def : fallback;
}

function parseMapping(value: string, fallback: Record<string, string>) {
  const decoded = parseJsonObject(value);
  if (!decoded) return fallback;
  const entries = Object.entries(decoded)
    .filter(([, v]) => typeof v === "string")
    .map(([k, v]) => [k, String(v)] as const);
  return entries.length > 0 ? Object.fromEntries(entries) : fallback;
}

function parseJsonObject(value: string): JsonRecord | null {
  if (!value.trim()) return null;
  try {
    const decoded = JSON.parse(value);
    return isRecord(decoded) ? decoded : null;
  } catch {
    return null;
  }
}

function prettyJson(value: Record<string, unknown>): string {
  return JSON.stringify(value, null, 2);
}

function withOutputDefaults(
  mapping: Record<string, string>,
  outputs: NodePortDef[]
): Record<string, string> {
  const defaults = Object.fromEntries(
    outputs
      .filter(port => ["prompt", "transcript", "action_json"].includes(port.name))
      .map(port => [port.name, mapping[port.name] ?? port.name])
  );
  return { ...defaults, ...mapping };
}

function transcriptObservation(text: string): Record<string, unknown> {
  return {
    type: "transcript.segment",
    source: "scope.chat",
    timestamp: Date.now() / 1000,
    payload: {
      text,
      isFinal: true,
    },
  };
}

function messagesFromState(state: JsonRecord | null): ChatMessage[] {
  if (!state) return [];
  const snapshot = isRecord(state.state) ? state.state : {};
  const transcript = typeof snapshot.transcript === "string" ? snapshot.transcript : "";
  const latestAction = latestPromptAction(state);
  const promptPayload = isRecord(latestAction?.payload) ? latestAction.payload : {};
  const prompt = typeof promptPayload.prompt === "string" ? promptPayload.prompt : "";
  const messages: ChatMessage[] = [];

  if (prompt) {
    messages.push({
      id: `prompt-${hashText(prompt)}`,
      role: "prompt",
      text: prompt,
      meta: "prompt",
    });
  }
  if (transcript) {
    messages.push({
      id: `transcript-${hashText(transcript)}`,
      role: "cue",
      text: transcript,
      meta: "transcript",
    });
  }
  return messages;
}

function latestPromptAction(state: JsonRecord): JsonRecord | null {
  for (const collectionName of ["decisionHistory", "decisionTrace"]) {
    const collection = state[collectionName];
    if (!Array.isArray(collection)) continue;
    for (const item of [...collection].reverse()) {
      if (!isRecord(item)) continue;
      const result = isRecord(item.result) ? item.result : {};
      const actions = result.actions;
      if (!Array.isArray(actions)) continue;
      for (const action of [...actions].reverse()) {
        if (isRecord(action) && typeof action.type === "string") {
          return action;
        }
      }
    }
  }
  return null;
}

function inferNames(root: JsonRecord, fields: string[]): string[] {
  const names = new Set<string>();
  for (const field of fields) {
    const value = root[field];
    if (Array.isArray(value)) {
      for (const item of value) {
        if (typeof item === "string") names.add(item);
        if (isRecord(item)) {
          const name = item.name ?? item.id ?? item.type;
          if (typeof name === "string") names.add(name);
        }
      }
    } else if (isRecord(value)) {
      for (const key of Object.keys(value)) names.add(key);
    }
  }
  return [...names].slice(0, 8);
}

function hashText(value: string): string {
  let hash = 0;
  for (let i = 0; i < value.length; i += 1) {
    hash = (hash * 31 + value.charCodeAt(i)) >>> 0;
  }
  return hash.toString(16);
}

function isRecord(value: unknown): value is JsonRecord {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}
