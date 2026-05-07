import {
  useCallback,
  useEffect,
  useMemo,
  useRef,
  useState,
  type MouseEvent,
  type PointerEvent,
  type ReactNode,
  type WheelEvent,
} from "react";
import {
  Activity,
  ChevronUp,
  Eye,
  FileText,
  Image,
  Mic,
  Plus,
  Radio,
  RotateCcw,
  Send,
  Settings2,
  SlidersHorizontal,
  Trash2,
  Upload,
  X,
} from "lucide-react";
import type { FlowNodeData } from "../../../lib/graphUtils";
import type { NodeParamDef, SessionParameters } from "../../../lib/api";
import {
  getCueSessionAgent,
  getCueSessionEventsUrl,
  getCueSessionState,
  getCueSessionTranscriptionUrl,
  getCueSessionVlmUrl,
  postCueObservation,
  resetCueSession,
  updateSessionParameters,
} from "../../../lib/api";

interface CueSessionWidgetProps {
  data: FlowNodeData;
  params: NodeParamDef[];
  setParam: (name: string, value: unknown) => void;
  setOutputValues?: (values: Record<string, unknown>) => void;
}

interface ChatMessage {
  id: string;
  role: "user" | "cue" | "prompt" | "action" | "pass" | "vision" | "status";
  text: string;
  meta: string;
  toolCall?: ToolCallSummary;
}

interface ToolCallSummary {
  toolName: string;
  kind: "action" | "pass";
  actionType?: string;
  reset?: boolean;
  reason?: string;
  prompt?: string;
  actionCount?: number;
  confidence?: number;
}

interface RuntimeParameterTarget {
  nodeId: string;
  paramName: string;
}

interface TranscriptionPreviewState {
  processed: string;
  unprocessed: string;
}

interface MicDiagnostics {
  phase: string;
  chunks: number;
  bytes: number;
  transcripts: number;
  lastTranscript: string;
  lastError: string;
}

interface ManifestItem {
  name: string;
  kind?: string;
  detail?: string;
  preview?: string;
}

interface CueWorkflowManifest {
  title: string;
  version?: string;
  description?: string;
  model?: string;
  inputs: ManifestItem[];
  outputs: ManifestItem[];
  tools: ManifestItem[];
  cues: ManifestItem[];
  prompts: ManifestItem[];
  evals: ManifestItem[];
  providers: ManifestItem[];
  runtime: ManifestItem[];
  styleTags: string[];
}

type Drawer = "workflow" | "tune" | "debug" | null;
type JsonRecord = Record<string, unknown>;

const DEFAULT_BASE_URL = "http://127.0.0.1:8792";
const DEFAULT_SESSION_ID = "demo";
const DEFAULT_ACTION_TYPE = "video.update_prompt";
const DEFAULT_INPUT_MAPPING: Record<string, string> = {
  chat: "transcript.segment",
  chat_in: "transcript.segment",
  transcript: "transcript.segment",
  vision: "vision.description",
  signal: "signal.value",
  context: "scope.context",
  context_json: "scope.context",
  control: "scope.control",
};
const DEFAULT_OUTPUT_MAPPING: Record<string, string> = {
  prompt: "longlive.prompt",
  reset: "longlive.reset_cache",
  transcript: "chat.transcript",
  action: "cue.action",
  action_json: "cue.action",
  param_patch: "scope.params",
  shader_patch: "scope.shader",
  source: "scope.source",
};
const DEFAULT_RESET_RUNTIME_TARGET = "longlive.reset_cache";
const DEFAULT_PROMPT_WEIGHT = 100;
const LEGACY_PROMPT_TOOL_NAMES = new Set(["video.update_realtime_prompt"]);
const EMPTY_TRANSCRIPTION_PREVIEW: TranscriptionPreviewState = {
  processed: "",
  unprocessed: "",
};
const EMPTY_MIC_DIAGNOSTICS: MicDiagnostics = {
  phase: "idle",
  chunks: 0,
  bytes: 0,
  transcripts: 0,
  lastTranscript: "",
  lastError: "",
};
const MIC_ALWAYS_ON_STORAGE_KEY = "scope.cueSession.micAlwaysOn";
const RESET_OUTPUT_PULSE_MS = 1000;

export function CueSessionWidget({
  data,
  params,
  setParam,
  setOutputValues,
}: CueSessionWidgetProps) {
  const fileInputRef = useRef<HTMLInputElement>(null);
  const appliedLiveManifestSignatureRef = useRef("");
  const publishedOutputSignatureRef = useRef("");
  const micSocketRef = useRef<WebSocket | null>(null);
  const micRecorderRef = useRef<MediaRecorder | null>(null);
  const micStreamRef = useRef<MediaStream | null>(null);
  const micPausedRef = useRef(false);
  const autoMicAttemptedRef = useRef(false);
  const resetPulseTimerRef = useRef<ReturnType<typeof setTimeout> | null>(null);
  const resetInFlightRef = useRef(false);
  const lastRuntimeToolMessageIdRef = useRef("");
  const transcriptPreviewTimerRef = useRef<ReturnType<typeof setTimeout> | null>(null);
  const chatScrollRef = useRef<HTMLDivElement | null>(null);
  const chatStickToBottomRef = useRef(true);
  const cameraSocketRef = useRef<WebSocket | null>(null);
  const cameraStreamRef = useRef<MediaStream | null>(null);
  const cameraVideoRef = useRef<HTMLVideoElement | null>(null);
  const cameraTimerRef = useRef<ReturnType<typeof setInterval> | null>(null);
  const [state, setState] = useState<JsonRecord | null>(null);
  const [agent, setAgent] = useState<JsonRecord | null>(null);
  const [status, setStatus] = useState("waiting");
  const [draft, setDraft] = useState("");
  const [timeline, setTimeline] = useState<ChatMessage[]>([]);
  const [eventPrompt, setEventPrompt] = useState("");
  const [drawer, setDrawer] = useState<Drawer>(null);
  const [micActive, setMicActive] = useState(false);
  const [micPaused, setMicPaused] = useState(false);
  const [micAlwaysOn, setMicAlwaysOn] = useState(readInitialMicAlwaysOn);
  const [micPopoverOpen, setMicPopoverOpen] = useState(false);
  const [micDiagnostics, setMicDiagnostics] =
    useState<MicDiagnostics>(EMPTY_MIC_DIAGNOSTICS);
  const [cameraActive, setCameraActive] = useState(false);
  const [styleTagDraft, setStyleTagDraft] = useState("");
  const [resetPulseActive, setResetPulseActive] = useState(false);
  const [transcriptionPreview, setTranscriptionPreview] =
    useState<TranscriptionPreviewState>(EMPTY_TRANSCRIPTION_PREVIEW);

  const pulseResetOutput = useCallback(() => {
    if (resetPulseTimerRef.current) {
      clearTimeout(resetPulseTimerRef.current);
      resetPulseTimerRef.current = null;
    }
    setResetPulseActive(true);
    resetPulseTimerRef.current = setTimeout(() => {
      setResetPulseActive(false);
      resetPulseTimerRef.current = null;
    }, RESET_OUTPUT_PULSE_MS);
  }, []);

  const baseUrl = stringParam(data, params, "cue_base_url", DEFAULT_BASE_URL);
  const sessionId = stringParam(data, params, "session_id", DEFAULT_SESSION_ID);
  const timeoutMs = numberParam(data, params, "timeout_ms", 1000);
  const enabled = booleanParam(data, params, "enabled", true);
  const actionType = stringParam(data, params, "action_type", DEFAULT_ACTION_TYPE);
  const pollIntervalMs = numberParam(data, params, "poll_interval_ms", 250);
  const cueFilePath = stringParam(data, params, "cue_file_path", "");
  const cueFileJson = stringParam(data, params, "cue_file_json", "");
  const uploadedCueManifest = useMemo(
    () => manifestFromCueFile(cueFileJson, cueFilePath),
    [cueFileJson, cueFilePath]
  );
  const liveCueManifest = useMemo(() => manifestFromAgent(agent), [agent]);
  const cueManifest = liveCueManifest ?? uploadedCueManifest;
  const cueManifestSource = liveCueManifest ? "Cue server" : cueFilePath;
  const defaultPrompt = useMemo(
    () => defaultPromptFromAgent(agent) || defaultPromptFromCueFile(cueFileJson),
    [agent, cueFileJson]
  );
  const styleTags = useMemo(
    () => stringListParam(data, params, "style_tags_json"),
    [data, params]
  );
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
    setEventPrompt("");
  }, [baseUrl, sessionId]);
  const applyTranscriptionPreview = (event: JsonRecord) => {
    const preview = transcriptionPreviewFromEvent(event);
    if (!preview) return;

    if (transcriptPreviewTimerRef.current) {
      clearTimeout(transcriptPreviewTimerRef.current);
      transcriptPreviewTimerRef.current = null;
    }

    setTranscriptionPreview(preview.value);
    if (preview.final) {
      transcriptPreviewTimerRef.current = setTimeout(() => {
        setTranscriptionPreview(EMPTY_TRANSCRIPTION_PREVIEW);
        transcriptPreviewTimerRef.current = null;
      }, 1200);
    }
  };

  useEffect(() => {
    if (!liveCueManifest) return;
    const signature = manifestSignature(liveCueManifest);
    if (appliedLiveManifestSignatureRef.current === signature) return;
    appliedLiveManifestSignatureRef.current = signature;

    if (liveCueManifest.inputs.length > 0) {
      setParam(
        "input_mapping_json",
        prettyJson({
          ...inputMapping,
          ...Object.fromEntries(
            liveCueManifest.inputs.map(item => [item.name, inputCueType(item)])
          ),
        })
      );
    }

    const inferredOutputs = [...liveCueManifest.outputs, ...liveCueManifest.tools];
    if (inferredOutputs.length > 0) {
      const baseMapping = pruneStaleManifestMappings(outputMapping, liveCueManifest);
      setParam(
        "output_mapping_json",
        prettyJson({
          ...baseMapping,
          ...Object.fromEntries(
            inferredOutputs.map(item => [
              item.name,
              outputCueType(item, baseMapping),
            ])
          ),
        })
      );
    }

    if (liveCueManifest.styleTags.length > 0 && styleTags.length === 0) {
      setParam("style_tags_json", JSON.stringify(liveCueManifest.styleTags));
    }
  }, [inputMapping, liveCueManifest, outputMapping, setParam, styleTags.length]);

  useEffect(() => {
    if (!enabled) {
      setStatus("disabled");
      return;
    }
    let cancelled = false;
    const refresh = async () => {
      try {
        const [nextState, nextAgent] = await Promise.all([
          getCueSessionState({ baseUrl, sessionId, timeoutMs }),
          getCueSessionAgent({ baseUrl, sessionId, timeoutMs }).catch(() => null),
        ]);
        if (cancelled) return;
        setState(nextState);
        setAgent(nextAgent);
        setStatus("live");
      } catch (error) {
        if (cancelled) return;
        setStatus(error instanceof Error ? error.message : "Cue unavailable");
      }
    };

    void refresh();
    const timer = setInterval(refresh, Math.max(pollIntervalMs, 1500));
    return () => {
      cancelled = true;
      clearInterval(timer);
    };
  }, [baseUrl, enabled, pollIntervalMs, sessionId, timeoutMs]);

  useEffect(() => {
    if (!enabled) return;
    let websocket: WebSocket | null = null;
    let closed = false;
    let reconnectTimer: ReturnType<typeof setTimeout> | null = null;

    const connect = () => {
      if (closed) return;
      try {
        websocket = new WebSocket(
          getCueSessionEventsUrl({ baseUrl, sessionId, timeoutMs })
        );
        websocket.onopen = () => setStatus("listening");
        websocket.onmessage = event => {
          const decoded = parseJsonObject(String(event.data));
          if (!decoded) return;
          applyTranscriptionPreview(decoded);
          const nextMessage = messageFromCueEvent(decoded);
          if (nextMessage) {
            setTimeline(prev => dedupeMessages([nextMessage, ...prev], 12));
          }
          const promptText = promptTextFromEvent(decoded);
          if (promptText) setEventPrompt(promptText);
          if (decoded.type === "state.snapshot" && isRecord(decoded.state)) {
            setState(previous => ({ ...(previous ?? {}), sessionId, state: decoded.state }));
          }
          if (decoded.type === "ready") setStatus("live");
          if (decoded.type === "error" && typeof decoded.error === "string") {
            setStatus(decoded.error);
          }
        };
        websocket.onerror = () => setStatus("event stream unavailable");
        websocket.onclose = () => {
          if (closed) return;
          setStatus("polling");
          reconnectTimer = setTimeout(connect, 1000);
        };
      } catch {
        setStatus("polling");
        reconnectTimer = setTimeout(connect, 1000);
      }
    };

    connect();

    return () => {
      closed = true;
      if (reconnectTimer) clearTimeout(reconnectTimer);
      websocket?.close();
    };
  }, [baseUrl, enabled, sessionId, timeoutMs]);

  useEffect(() => {
    return () => {
      stopMicStream();
      stopCameraFeedback();
      if (transcriptPreviewTimerRef.current) {
        clearTimeout(transcriptPreviewTimerRef.current);
        transcriptPreviewTimerRef.current = null;
      }
      if (resetPulseTimerRef.current) {
        clearTimeout(resetPulseTimerRef.current);
        resetPulseTimerRef.current = null;
      }
    };
  }, []);

  useEffect(() => {
    if (!enabled || !micAlwaysOn || micActive || autoMicAttemptedRef.current) return;
    autoMicAttemptedRef.current = true;
    const timer = setTimeout(() => {
      void startMicStream();
    }, 400);
    return () => clearTimeout(timer);
  }, [enabled, micActive, micAlwaysOn]);

  useEffect(() => {
    const handleKeyDown = (event: KeyboardEvent) => {
      if (event.code !== "Space" || event.repeat || micAlwaysOn || isTextEntryTarget(event.target)) {
        return;
      }
      event.preventDefault();
      if (!micActive) {
        setMicAlwaysOnPreference(false);
        void startMicStream();
      } else {
        setMicPausedState(false);
      }
    };

    const handleKeyUp = (event: KeyboardEvent) => {
      if (event.code !== "Space" || micAlwaysOn || isTextEntryTarget(event.target)) return;
      event.preventDefault();
      if (micActive) setMicPausedState(true);
    };

    window.addEventListener("keydown", handleKeyDown);
    window.addEventListener("keyup", handleKeyUp);
    return () => {
      window.removeEventListener("keydown", handleKeyDown);
      window.removeEventListener("keyup", handleKeyUp);
    };
  }, [micActive, micAlwaysOn]);

  const stateMessages = useMemo(() => messagesFromState(state), [state]);
  const messages = useMemo(() => {
    return dedupeMessages([...timeline, ...stateMessages], 14);
  }, [stateMessages, timeline]);
  const latestToolMessage = useMemo(() => {
    return (
      stateMessages.find(message => message.toolCall) ??
      timeline.find(message => message.toolCall) ??
      messages.find(message => message.toolCall)
    );
  }, [messages, stateMessages, timeline]);
  const latestActionToolMessageId =
    latestToolMessage?.toolCall?.kind === "action" ? latestToolMessage.id : "";
  const sendRuntimeAction = useCallback(
    async (
      promptText?: string,
      toolCall?: ToolCallSummary,
      options: { seed?: number } = {}
    ) => {
      const prompt = promptText?.trim() ?? "";
      const shouldReset = toolCall?.reset === true;
      if (!prompt && !shouldReset) return;
      const targets = shouldReset
        ? resetRuntimeTargets(outputMapping, toolCall)
        : promptRuntimeTargets(outputMapping, toolCall);

      await Promise.all(
        targets.map(target => {
          const parameters: SessionParameters = { node_id: target.nodeId };
          if (prompt) {
            parameters.prompts = [{ text: prompt, weight: DEFAULT_PROMPT_WEIGHT }];
          }
          if (shouldReset) {
            parameters[target.paramName] = true;
          }
          if (options.seed !== undefined) {
            parameters.seed = options.seed;
          }
          return updateSessionParameters(parameters);
        })
      );
    },
    [outputMapping]
  );
  const sendRuntimeReset = useCallback(
    async (
      promptText?: string,
      toolCall?: ToolCallSummary,
      options: { seed?: number } = {}
    ) => {
      await sendRuntimeAction(
        promptText,
        {
          toolName: toolCall?.toolName ?? "scope.reset",
          kind: "action",
          actionType: toolCall?.actionType ?? "video.create_scene",
          reset: true,
          prompt: toolCall?.prompt,
          reason: toolCall?.reason,
        },
        options
      );
    },
    [sendRuntimeAction]
  );
  const latestActionPrompt =
    latestToolMessage?.toolCall?.prompt || eventPrompt || latestPromptFromState(state) || "";
  useEffect(() => {
    if (!latestActionToolMessageId) return;
    if (lastRuntimeToolMessageIdRef.current === latestActionToolMessageId) return;
    const toolCall = latestToolMessage?.toolCall;
    if (!toolCall || (!toolCall.prompt && toolCall.reset !== true)) return;
    lastRuntimeToolMessageIdRef.current = latestActionToolMessageId;
    if (toolCall.reset) pulseResetOutput();
    void sendRuntimeAction(latestActionPrompt, toolCall)
      .then(() => setStatus(toolCall.reset ? "prompt reset sent" : "prompt sent"))
      .catch(error => setStatus(error instanceof Error ? error.message : "prompt failed"));
  }, [
    latestActionPrompt,
    latestActionToolMessageId,
    latestToolMessage,
    pulseResetOutput,
    sendRuntimeAction,
  ]);
  const displayMessages = useMemo(
    () => [...messages].reverse().filter(message => !message.toolCall),
    [messages]
  );

  useEffect(() => {
    const container = chatScrollRef.current;
    if (!container) return;
    if (!chatStickToBottomRef.current) return;
    requestAnimationFrame(() => {
      container.scrollTop = container.scrollHeight;
    });
  }, [messages]);

  const updateChatStickiness = () => {
    const container = chatScrollRef.current;
    if (!container) return;
    const distanceFromBottom =
      container.scrollHeight - container.scrollTop - container.clientHeight;
    chatStickToBottomRef.current = distanceFromBottom < 48;
  };

  const handleChatWheel = (event: WheelEvent<HTMLDivElement>) => {
    event.stopPropagation();
    const container = chatScrollRef.current;
    if (!container || container.scrollHeight <= container.clientHeight) return;
    event.preventDefault();
    const deltaScale =
      event.deltaMode === 1 ? 16 : event.deltaMode === 2 ? container.clientHeight : 1;
    container.scrollTop += event.deltaY * deltaScale;
    updateChatStickiness();
  };

  const handleChatPointerDown = (event: PointerEvent<HTMLDivElement>) => {
    event.stopPropagation();
  };
  const live = [
    "live",
    "listening",
    "sent",
    "reset",
    "tags",
    "mic",
    "mic live",
    "mic paused",
    "camera",
    "camera live",
  ].includes(status);
  const profileLabel = cueManifest?.title || cueManifestSource || agentLabel(agent) || "Cue Director";
  const addStatusMessage = (text: string, meta = "status") => {
    setTimeline(prev =>
      dedupeMessages(
        [
          {
            id: `${meta}-${Date.now()}-${hashText(text)}`,
            role: "status",
            text,
            meta,
          },
          ...prev,
        ],
        12
      )
    );
  };

  const submitChat = async () => {
    const text = draft.trim();
    if (!text) return;
    const message: ChatMessage = {
      id: `local-${Date.now()}`,
      role: "user",
      text,
      meta: "operator",
    };
    setTimeline(prev => dedupeMessages([message, ...prev], 12));
    setDraft("");
    setParam("chat_text", text);
    setParam("chat_submit_count", Date.now());

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

  const postStyleTags = async (tags: string[]) => {
    setParam("style_tags_json", JSON.stringify(tags));
    try {
      await postCueObservation({
        baseUrl,
        sessionId,
        timeoutMs,
        observation: styleTagsObservation(tags),
      });
      setStatus("tags");
    } catch (error) {
      setStatus(error instanceof Error ? error.message : "style tags failed");
    }
  };

  const addStyleTag = () => {
    const nextTag = styleTagDraft.trim();
    if (!nextTag || styleTags.includes(nextTag)) {
      setStyleTagDraft("");
      return;
    }
    const nextTags = [...styleTags, nextTag].slice(0, 12);
    setTimeline(prev =>
      dedupeMessages(
        [
          {
            id: `style-tags-${Date.now()}`,
            role: "status",
            text: `Style tags: ${nextTags.join(", ")}`,
            meta: "style tags",
          },
          ...prev,
        ],
        12
      )
    );
    setStyleTagDraft("");
    void postStyleTags(nextTags);
  };

  const removeStyleTag = (tag: string) => {
    const nextTags = styleTags.filter(candidate => candidate !== tag);
    void postStyleTags(nextTags);
  };

  const resetCue = async () => {
    if (resetInFlightRef.current) return;
    resetInFlightRef.current = true;
    setStatus("resetting");
    addStatusMessage("Reset requested.", "control");
    try {
      let resetPrompt = defaultPrompt;
      const resetTimeoutMs = Math.max(timeoutMs, 3000);
      if (!resetPrompt) {
        try {
          const liveAgent = await getCueSessionAgent({
            baseUrl,
            sessionId,
            timeoutMs: resetTimeoutMs,
          });
          if (isRecord(liveAgent)) {
            setAgent(liveAgent);
            resetPrompt = defaultPromptFromAgent(liveAgent);
          }
        } catch {
          // Surface the unavailable default instead of resetting the current scene.
        }
      }
      if (!resetPrompt) {
        setStatus("default prompt unavailable");
        addStatusMessage("Reset default prompt unavailable.", "control");
        return;
      }
      const seed = randomScopeSeed();
      try {
        const resetResult = await resetCueSession({
          baseUrl,
          sessionId,
          timeoutMs: resetTimeoutMs,
        });
        if (isRecord(resetResult.state)) {
          setState(previous => ({
            ...(previous ?? {}),
            sessionId,
            state: resetResult.state,
          }));
        }
      } catch {
        await postCueObservation({
          baseUrl,
          sessionId,
          timeoutMs,
          observation: controlObservation("reset", { reset: true }),
        });
      }
      pulseResetOutput();
      await sendRuntimeReset(resetPrompt, undefined, { seed });
      if (resetPrompt) setEventPrompt(resetPrompt);
      setStatus("reset");
    } catch (error) {
      setStatus(error instanceof Error ? error.message : "reset failed");
    } finally {
      resetInFlightRef.current = false;
    }
  };

  const stopGraphInteraction = (
    event: PointerEvent<HTMLButtonElement> | MouseEvent<HTMLButtonElement>
  ) => {
    event.stopPropagation();
  };

  const fireResetFromControl = (event: MouseEvent<HTMLButtonElement>) => {
    event.preventDefault();
    event.stopPropagation();
    void resetCue();
  };

  const updateMicDiagnostics = (patch: Partial<MicDiagnostics>) => {
    setMicDiagnostics(previous => ({ ...previous, ...patch }));
  };

  const setMicAlwaysOnPreference = (enabled: boolean) => {
    setMicAlwaysOn(enabled);
    writeInitialMicAlwaysOn(enabled);
  };

  const setMicPausedState = (paused: boolean) => {
    micPausedRef.current = paused;
    setMicPaused(paused);
    micStreamRef.current?.getAudioTracks().forEach(track => {
      track.enabled = !paused;
    });
    setStatus(paused ? "mic paused" : "mic live");
    updateMicDiagnostics({ phase: paused ? "paused" : "streaming" });
  };

  const setMicAlwaysOnMode = async (enabled: boolean) => {
    setMicAlwaysOnPreference(enabled);
    if (enabled) {
      if (!micActive) {
        await startMicStream();
      } else {
        setMicPausedState(false);
      }
      return;
    }

    if (micActive) {
      setMicPausedState(true);
    } else {
      micPausedRef.current = true;
      setMicPaused(true);
      setStatus("mic paused");
      updateMicDiagnostics({ phase: "paused" });
    }
  };

  const resumeMicStream = async () => {
    setMicAlwaysOnPreference(true);
    if (!micActive) {
      await startMicStream();
    } else {
      setMicPausedState(false);
    }
  };

  const toggleMicStream = async () => {
    if (micActive || micSocketRef.current || micRecorderRef.current) {
      setMicAlwaysOnPreference(false);
      setMicPopoverOpen(false);
      stopMicStream();
      return;
    }

    await resumeMicStream();
  };

  const startMicStream = async () => {
    if (micSocketRef.current || micRecorderRef.current) {
      setMicPausedState(false);
      return;
    }
    if (!navigator.mediaDevices?.getUserMedia || !window.MediaRecorder) {
      setStatus("mic unavailable");
      updateMicDiagnostics({
        phase: "unavailable",
        lastError: "Microphone capture is unavailable in this window.",
      });
      addStatusMessage("Microphone capture is unavailable in this window.", "mic");
      return;
    }

    try {
      setMicDiagnostics({ ...EMPTY_MIC_DIAGNOSTICS, phase: "requesting mic" });
      const stream = await navigator.mediaDevices.getUserMedia({ audio: true });
      const socket = new WebSocket(
        getCueSessionTranscriptionUrl({ baseUrl, sessionId, timeoutMs })
      );
      socket.binaryType = "arraybuffer";
      micStreamRef.current = stream;
      micSocketRef.current = socket;
      setMicActive(true);
      micPausedRef.current = false;
      setMicPaused(false);
      setStatus("mic");
      updateMicDiagnostics({ phase: "connecting" });

      socket.onopen = () => {
        updateMicDiagnostics({ phase: "waiting for transcriber" });
      };
      socket.onmessage = event => {
        const decoded = parseJsonObject(String(event.data));
        if (!decoded) return;
        if (decoded.type === "transcriber.ready") {
          updateMicDiagnostics({ phase: "transcriber ready" });
          startMicRecorder(stream, socket);
          return;
        }
        if (decoded.type === "transcript") {
          const text = transcriptTextFromEvent(decoded);
          setMicDiagnostics(previous => ({
            ...previous,
            phase: "transcript",
            transcripts: previous.transcripts + 1,
            lastTranscript: text || previous.lastTranscript,
          }));
        }
        applyTranscriptionPreview(decoded);
        const promptText = promptTextFromEvent(decoded);
        if (promptText) setEventPrompt(promptText);
        const nextMessage = messageFromCueEvent(decoded);
        if (nextMessage) {
          setTimeline(prev => dedupeMessages([nextMessage, ...prev], 12));
        }
        if (decoded.type === "error" && typeof decoded.error === "string") {
          setStatus(decoded.error);
          updateMicDiagnostics({ phase: "cue error", lastError: decoded.error });
          addStatusMessage(decoded.error, "mic");
        }
      };
      socket.onerror = () => {
        setStatus("mic error");
        updateMicDiagnostics({
          phase: "socket error",
          lastError: "Cue transcription socket reported an error.",
        });
        addStatusMessage("Cue transcription socket reported an error.", "mic");
      };
      socket.onclose = event => {
        const detail =
          event.code && event.code !== 1000
            ? ` (${event.code}${event.reason ? `: ${event.reason}` : ""})`
            : "";
        updateMicDiagnostics({
          phase: "closed",
          lastError: detail ? `Microphone stream closed${detail}.` : "",
        });
        if (detail) addStatusMessage(`Microphone stream closed${detail}.`, "mic");
        stopMicStream();
      };
    } catch (error) {
      stopMicStream();
      const message = error instanceof Error ? error.message : "mic failed";
      setStatus(message);
      updateMicDiagnostics({ phase: "mic failed", lastError: message });
      addStatusMessage(`Microphone failed: ${message}`, "mic");
    }
  };

  const startMicRecorder = (stream: MediaStream, socket: WebSocket) => {
    if (micRecorderRef.current || socket.readyState !== WebSocket.OPEN) return;
    try {
      const recorderOptions = MediaRecorder.isTypeSupported("audio/webm;codecs=opus")
        ? { mimeType: "audio/webm;codecs=opus" }
        : undefined;
      const recorder = new MediaRecorder(stream, recorderOptions);
      micRecorderRef.current = recorder;
      recorder.ondataavailable = event => {
        if (!event.data.size || socket.readyState !== WebSocket.OPEN || micPausedRef.current) {
          return;
        }
        const byteCount = event.data.size;
        setMicDiagnostics(previous => ({
          ...previous,
          phase: "sending",
          chunks: previous.chunks + 1,
          bytes: previous.bytes + byteCount,
        }));
        void event.data.arrayBuffer().then(buffer => {
          if (socket.readyState === WebSocket.OPEN) socket.send(buffer);
        });
      };
      recorder.onerror = event => {
        setStatus("recorder error");
        updateMicDiagnostics({
          phase: "recorder error",
          lastError: event.error.message,
        });
        addStatusMessage(`Microphone recorder error: ${event.error.message}`, "mic");
      };
      recorder.start(250);
      setMicPausedState(false);
    } catch (error) {
      const message = error instanceof Error ? error.message : "recorder failed";
      setStatus(message);
      updateMicDiagnostics({ phase: "recorder failed", lastError: message });
      addStatusMessage(`Could not start microphone recorder: ${message}`, "mic");
      stopMicStream();
    }
  };

  const stopMicStream = () => {
    const recorder = micRecorderRef.current;
    micRecorderRef.current = null;
    if (recorder && recorder.state !== "inactive") {
      try {
        recorder.stop();
      } catch {
        // Ignore recorder shutdown races.
      }
    }

    const socket = micSocketRef.current;
    micSocketRef.current = null;
    if (socket?.readyState === WebSocket.OPEN) {
      socket.send(JSON.stringify({ type: "finalize" }));
      socket.close();
    } else if (socket?.readyState === WebSocket.CONNECTING) {
      socket.close();
    }

    micStreamRef.current?.getTracks().forEach(track => track.stop());
    micStreamRef.current = null;
    micPausedRef.current = false;
    setMicActive(false);
    setMicPaused(false);
    setStatus("mic off");
    updateMicDiagnostics({ phase: "stopped" });
  };

  const startCameraFeedback = async () => {
    if (cameraSocketRef.current || cameraTimerRef.current) return;
    if (!navigator.mediaDevices?.getUserMedia) {
      setStatus("camera unavailable");
      return;
    }

    try {
      const stream = await navigator.mediaDevices.getUserMedia({
        video: {
          width: { ideal: 640 },
          height: { ideal: 360 },
          frameRate: { ideal: 15, max: 30 },
        },
        audio: false,
      });
      const socket = new WebSocket(getCueSessionVlmUrl({ baseUrl, sessionId, timeoutMs }));
      cameraStreamRef.current = stream;
      cameraSocketRef.current = socket;
      setCameraActive(true);
      setStatus("camera");

      const video = document.createElement("video");
      video.muted = true;
      video.playsInline = true;
      video.srcObject = stream;
      cameraVideoRef.current = video;
      await video.play();

      const canvas = document.createElement("canvas");
      const context = canvas.getContext("2d");
      if (!context) throw new Error("Could not create camera capture canvas.");

      let frameIndex = 0;
      const sendFrame = () => {
        if (socket.readyState !== WebSocket.OPEN || video.readyState < 2) return;
        const sourceWidth = video.videoWidth || 640;
        const sourceHeight = video.videoHeight || 360;
        const width = Math.min(sourceWidth, 640);
        const height = Math.max(1, Math.round((width / sourceWidth) * sourceHeight));
        canvas.width = width;
        canvas.height = height;
        context.drawImage(video, 0, 0, width, height);
        socket.send(
          JSON.stringify({
            imageUrl: canvas.toDataURL("image/jpeg", 0.72),
            timestamp: Date.now() / 1000,
            frameId: `scope-webcam-${++frameIndex}`,
            metadata: {
              frameProvider: {
                provider: "scope",
                name: "scope.webcam",
                streamId: "webcam",
              },
            },
          })
        );
      };

      socket.onopen = () => {
        setStatus("camera live");
        sendFrame();
        cameraTimerRef.current = setInterval(sendFrame, 1000);
      };
      socket.onmessage = event => {
        const decoded = parseJsonObject(String(event.data));
        if (!decoded) return;
        const promptText = promptTextFromEvent(decoded);
        if (promptText) setEventPrompt(promptText);
        const nextMessage = messageFromCueEvent(decoded);
        if (nextMessage) {
          setTimeline(prev => dedupeMessages([nextMessage, ...prev], 12));
        }
        if (decoded.type === "error" && typeof decoded.error === "string") {
          setStatus(decoded.error);
        }
      };
      socket.onerror = () => setStatus("camera error");
      socket.onclose = () => stopCameraFeedback();
    } catch (error) {
      stopCameraFeedback();
      setStatus(error instanceof Error ? error.message : "camera failed");
    }
  };

  const stopCameraFeedback = () => {
    if (cameraTimerRef.current) {
      clearInterval(cameraTimerRef.current);
      cameraTimerRef.current = null;
    }

    const socket = cameraSocketRef.current;
    cameraSocketRef.current = null;
    if (socket?.readyState === WebSocket.OPEN || socket?.readyState === WebSocket.CONNECTING) {
      socket.close();
    }

    cameraVideoRef.current?.pause();
    cameraVideoRef.current = null;
    cameraStreamRef.current?.getTracks().forEach(track => track.stop());
    cameraStreamRef.current = null;
    setCameraActive(false);
  };

  const loadCueFile = async (file: File) => {
    const text = await file.text();
    setParam("cue_file_path", file.name);
    setParam("cue_file_json", text);

    const manifest = manifestFromCueFile(text, file.name);
    if (!manifest) {
      setTimeline(prev =>
        dedupeMessages(
          [
            {
              id: `cue-file-error-${Date.now()}`,
              role: "status",
              text: "Could not read this .cue file as a workflow manifest.",
              meta: "cue file",
            },
            ...prev,
          ],
          12
        )
      );
      setStatus("cue file error");
      return;
    }

    setTimeline(prev =>
      dedupeMessages(
        [
          {
            id: `cue-file-${Date.now()}`,
            role: "status",
            text: `Loaded ${manifest.title}: ${manifest.inputs.length} inputs, ${manifest.tools.length} tools, ${manifest.outputs.length} outputs.`,
            meta: "cue file",
          },
          ...prev,
        ],
        12
      )
    );
    setDrawer("workflow");
    setStatus("workflow");

    const inferredInputs = manifest.inputs.map(item => item.name);
    if (inferredInputs.length > 0) {
      setParam(
        "input_mapping_json",
        prettyJson({
          ...inputMapping,
          ...Object.fromEntries(
            manifest.inputs.map(item => [item.name, inputCueType(item)])
          ),
        })
      );
    }

    const inferredOutputs = [...manifest.outputs, ...manifest.tools].map(item => item.name);
    if (inferredOutputs.length > 0) {
      const baseMapping = pruneStaleManifestMappings(outputMapping, manifest);
      setParam(
        "output_mapping_json",
        prettyJson({
          ...baseMapping,
          ...Object.fromEntries(
            [...manifest.outputs, ...manifest.tools].map(item => [
              item.name,
              outputCueType(item, baseMapping),
            ])
          ),
        })
      );
    }

    if (manifest.styleTags.length > 0) {
      setParam("style_tags_json", JSON.stringify(manifest.styleTags));
    }
  };

  const stateTranscriptText = transcriptTextFromState(state);
  const latestPrompt =
    eventPrompt || latestPromptFromState(state) || latestToolMessage?.toolCall?.prompt || "";
  useEffect(() => {
    if (!setOutputValues) return;
    const latestDecision = latestToolMessage?.text ?? "";
    const nextValues: Record<string, unknown> = {
      prompt: latestPrompt,
      transcript: stateTranscriptText,
      decision: latestDecision,
      action:
        latestToolMessage?.toolCall?.kind === "action" ? latestDecision : "",
      reset: resetPulseActive,
      status,
    };
    const signature = JSON.stringify(nextValues);
    if (publishedOutputSignatureRef.current === signature) return;
    publishedOutputSignatureRef.current = signature;
    setOutputValues(nextValues);
  }, [
    latestPrompt,
    latestToolMessage,
    resetPulseActive,
    setOutputValues,
    stateTranscriptText,
    status,
  ]);
  const visibleTranscriptionPreview = transcriptionPreview.unprocessed
    ? transcriptionPreview
    : {
        processed: tailText(transcriptionPreview.processed || stateTranscriptText, 220),
        unprocessed: "",
      };
  const transcriptionPreviewActive =
    visibleTranscriptionPreview.processed.length > 0 ||
    visibleTranscriptionPreview.unprocessed.length > 0;
  const micListening = micActive && !micPaused;
  const micStatusText = micListening ? "Listening" : micActive ? "Paused" : "Ready";
  const micDiagnosticText = `${micDiagnostics.phase} · ${micDiagnostics.chunks} chunks · ${formatBytes(
    micDiagnostics.bytes
  )}`;

  return (
    <div className="nodrag nowheel relative flex w-[380px] flex-col overflow-hidden rounded-md border border-[#1a1a1a] bg-black text-white shadow-[0_10px_40px_rgba(0,0,0,0.45)]">
      <div
        aria-hidden
        className="pointer-events-none absolute inset-0 z-0 opacity-100"
        style={{
          background:
            "repeating-linear-gradient(0deg, transparent, transparent 2px, rgba(0,0,0,0.03) 2px, rgba(0,0,0,0.03) 4px)",
        }}
      />
      <div className="relative z-[1] flex h-[560px] min-h-0 flex-col">
        <div className="flex items-center gap-1 border-b border-[#1a1a1a] bg-[#050505] px-2 py-2">
          <button
            type="button"
            className="rounded-md border border-[rgba(255,225,200,0.32)] bg-[rgba(255,220,190,0.12)] px-3 py-1.5 text-[11px] font-medium text-[rgba(255,235,215,0.9)] shadow-[0_0_8px_rgba(255,210,170,0.12)]"
          >
            Chat
          </button>
          <button
            type="button"
            className={`rounded-md px-3 py-1.5 text-[11px] text-[#777] transition hover:bg-[#111] hover:text-[#aaa] ${
              drawer === "workflow" ? "bg-[#111] text-[rgba(255,235,215,0.82)]" : ""
            }`}
            onClick={() => setDrawer(drawer === "workflow" ? null : "workflow")}
          >
            Workflow
          </button>
          <button
            type="button"
            className={`rounded-md px-3 py-1.5 text-[11px] text-[#777] transition hover:bg-[#111] hover:text-[#aaa] ${
              drawer === "tune" ? "bg-[#111] text-[rgba(255,235,215,0.82)]" : ""
            }`}
            onClick={() => setDrawer(drawer === "tune" ? null : "tune")}
          >
            Controls
          </button>
          <div className="ml-auto flex items-center gap-1">
            <StatusPill live={live} status={status} />
            <IconButton title="Load .cue file" onClick={() => fileInputRef.current?.click()}>
              <Upload size={13} />
            </IconButton>
            <IconButton
              title="Trace"
              active={drawer === "debug"}
              onClick={() => setDrawer(drawer === "debug" ? null : "debug")}
            >
              <Activity size={13} />
            </IconButton>
            <IconButton title="Clear history" onClick={() => setTimeline([])}>
              <Trash2 size={13} />
            </IconButton>
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

        <div
          ref={chatScrollRef}
          className="flex min-h-0 flex-1 touch-pan-y flex-col gap-3 overflow-y-auto overflow-x-hidden overscroll-contain bg-black py-4 pl-3 pr-4 [scrollbar-width:thin]"
          onScroll={updateChatStickiness}
          onPointerDownCapture={handleChatPointerDown}
          onWheelCapture={handleChatWheel}
        >
          {displayMessages.length === 0 ? (
            <div className="flex flex-1 flex-col items-center justify-center px-4 py-10 text-center">
              <div className="mb-2 text-[24px] leading-none text-[#333]">◉</div>
              <div className="font-mono text-[13px] tracking-[0.5px] text-[#333]">
                Speak or type to begin...
              </div>
            </div>
          ) : (
            displayMessages.map(message => (
              <MessageBubble key={message.id} message={message} />
            ))
          )}
        </div>

        {latestToolMessage?.toolCall && (
          <LatestDecisionPanel
            message={latestToolMessage}
            toolCall={latestToolMessage.toolCall}
          />
        )}

        <div
          className={`relative mx-4 mb-2 min-h-8 overflow-x-auto overflow-y-hidden whitespace-nowrap px-6 py-2 transition duration-200 [scrollbar-width:none] ${
            transcriptionPreviewActive
              ? "flex translate-y-0 items-center opacity-100"
              : "hidden translate-y-2 opacity-0"
          }`}
        >
          <div className="pointer-events-none absolute inset-y-0 left-0 z-[1] w-6 bg-gradient-to-r from-black to-transparent" />
          <div className="pointer-events-none absolute inset-y-0 right-0 z-[1] w-6 bg-gradient-to-l from-black to-transparent" />
          {visibleTranscriptionPreview.processed && (
            <span className="text-[13px] text-[#666] opacity-70">
              {visibleTranscriptionPreview.processed}
            </span>
          )}
          {visibleTranscriptionPreview.unprocessed && (
            <span className="text-[18px] font-medium text-white shadow-[0_0_20px_rgba(0,255,136,0.18)]">
              {visibleTranscriptionPreview.unprocessed}
            </span>
          )}
          <span className="ml-0.5 inline-block h-[1.2em] w-[3px] animate-pulse bg-[#00ff88] align-text-bottom" />
        </div>

        <div className="relative border-t border-[#1a1a1a] bg-gradient-to-b from-[#0a0a0a] to-black px-4 pb-5 pt-4">
          <div className="mb-3 flex items-center gap-2.5">
            <div className="relative flex items-center">
              <button
                type="button"
                className={`flex h-11 w-11 shrink-0 items-center justify-center rounded-l-full border border-r-0 transition ${
                  micListening
                    ? "border-[#222] bg-transparent text-[#ff3333] shadow-[0_0_0_7px_rgba(255,51,51,0.10)]"
                    : micActive || micAlwaysOn
                      ? "border-[rgba(255,225,200,0.28)] bg-transparent text-[rgba(255,235,215,0.9)] shadow-[0_0_8px_rgba(255,210,170,0.15)]"
                      : "border-[#222] bg-[#0d0d0d] text-[#666] hover:border-[#444] hover:bg-[#151515] hover:text-[#999]"
                }`}
                title={micActive ? "Turn off microphone" : "Turn on microphone"}
                onClick={() => void toggleMicStream()}
              >
                <Mic size={18} />
              </button>
              <button
                type="button"
                className={`flex h-11 w-[22px] shrink-0 items-center justify-center rounded-r-full border border-l border-[#1a1a1a] bg-[#0d0d0d] text-[#555] transition hover:border-[#444] hover:bg-[#151515] hover:text-[#999] ${
                  micActive || micAlwaysOn ? "border-[rgba(255,225,200,0.24)]" : ""
                }`}
                title="Microphone options"
                onClick={event => {
                  event.stopPropagation();
                  setMicPopoverOpen(!micPopoverOpen);
                }}
              >
                <ChevronUp size={10} />
              </button>

              <div
                className={`absolute bottom-[calc(100%+12px)] left-1/2 z-50 w-[260px] -translate-x-1/2 rounded-xl border border-[rgba(255,255,255,0.15)] bg-[rgba(18,18,18,0.94)] py-3 opacity-0 shadow-[0_8px_32px_rgba(0,0,0,0.5)] backdrop-blur-xl transition ${
                  micPopoverOpen ? "visible opacity-100" : "invisible"
                }`}
              >
                <div className="absolute -bottom-[6px] left-1/2 h-2.5 w-2.5 -translate-x-1/2 rotate-45 border-b border-r border-[rgba(255,255,255,0.15)] bg-[rgba(18,18,18,0.94)]" />
                <div className="flex items-center gap-2 px-4 pb-2 pt-1 text-[12px] text-white/60">
                  <span
                    className={`h-1.5 w-1.5 rounded-full ${
                      micListening ? "animate-pulse bg-[#00ff66]" : "bg-[#444]"
                    }`}
                  />
                  <span>{micStatusText}</span>
                </div>
                <div className="px-4 py-1.5">
                  <button
                    type="button"
                    className="flex w-full items-center justify-between text-[12px] text-white/70"
                    onClick={() => void setMicAlwaysOnMode(!micAlwaysOn)}
                  >
                    <span>Always-on</span>
                    <span
                      className={`relative h-[18px] w-8 rounded-full transition ${
                        micAlwaysOn ? "bg-[rgba(255,235,215,0.9)]" : "bg-[#333]"
                      }`}
                    >
                      <span
                        className={`absolute top-0.5 h-3.5 w-3.5 rounded-full bg-white transition ${
                          micAlwaysOn ? "translate-x-4" : "translate-x-0.5"
                        }`}
                      />
                    </span>
                  </button>
                </div>
                {micActive && (
                  <div className="px-4 py-1.5">
                    <button
                      type="button"
                      className="w-full rounded-md border border-[rgba(255,80,80,0.28)] bg-[rgba(255,40,40,0.08)] px-3 py-2 text-left text-[12px] text-[#ff8a8a] transition hover:border-[rgba(255,110,110,0.45)] hover:bg-[rgba(255,40,40,0.14)]"
                      onClick={() => {
                        setMicAlwaysOnPreference(false);
                        setMicPopoverOpen(false);
                        stopMicStream();
                      }}
                    >
                      Turn off microphone
                    </button>
                  </div>
                )}
                <div className="px-4 pb-2 pt-1 text-[11px] text-white/35">
                  Hold <kbd className="rounded border border-[rgba(255,225,200,0.2)] bg-[rgba(255,220,190,0.12)] px-1.5 py-0.5 text-[10px] text-[rgba(255,235,215,0.9)]">Space</kbd> to talk
                </div>
                <div className="my-1 h-px bg-[rgba(255,255,255,0.15)]" />
                <div className="flex items-center gap-2 px-4 py-2 text-[12px] text-[rgba(255,235,215,0.75)]">
                  <span className="w-3.5 text-[11px]">✓</span>
                  <span className="truncate">System microphone</span>
                </div>
              </div>
            </div>

            <input
              value={draft}
              onChange={event => setDraft(event.target.value)}
              onKeyDown={event => {
                if (event.key === "Enter" && !event.shiftKey) {
                  event.preventDefault();
                  void submitChat();
                }
              }}
              className="h-11 min-w-0 flex-1 rounded-full border border-[#222] bg-[#0d0d0d] px-5 text-[14px] text-white outline-none transition placeholder:text-[#444] focus:border-[rgba(255,225,200,0.4)] focus:shadow-[0_0_0_2px_rgba(255,220,190,0.12)]"
              placeholder="Type a prompt"
            />
            <button
              type="button"
              className="flex h-11 w-11 shrink-0 items-center justify-center rounded-full border-2 border-[rgba(255,225,200,0.4)] bg-transparent text-[rgba(255,235,215,0.9)] transition hover:scale-105 hover:bg-[rgba(255,220,190,0.12)] hover:shadow-[0_0_12px_rgba(255,210,170,0.25)] active:scale-95"
              title="Send"
              onClick={() => void submitChat()}
            >
              <Send size={18} />
            </button>
          </div>

          <div className="mb-2 flex min-h-5 items-center justify-between gap-3 px-1 font-mono text-[10px] text-[#555]">
            <span className={micListening ? "text-[#777]" : "text-[#555]"}>
              mic {micDiagnosticText}
            </span>
            <span className="truncate text-right text-[#444]">
              {micDiagnostics.lastTranscript ||
                micDiagnostics.lastError ||
                (micListening ? "sending audio to Cue" : "not sending audio")}
            </span>
          </div>

          <div className="mb-2 flex items-center gap-2">
            <div className="flex min-w-0 flex-1 items-center rounded-full border border-[#222] bg-[#0d0d0d] px-3 py-1.5 focus-within:border-[rgba(255,225,200,0.4)]">
              <input
                value={styleTagDraft}
                onChange={event => setStyleTagDraft(event.currentTarget.value)}
                onKeyDown={event => {
                  if (event.key === "Enter") {
                    event.preventDefault();
                    addStyleTag();
                  }
                }}
                className="min-w-0 flex-1 bg-transparent text-[12px] text-white outline-none placeholder:text-[#555]"
                placeholder="+ add tag"
              />
              <button
                type="button"
                className="text-[#555] hover:text-[rgba(255,235,215,0.9)]"
                title="Add style tag"
                onClick={addStyleTag}
              >
                <Plus size={13} />
              </button>
            </div>
            <button
              type="button"
              className="flex h-7 w-7 shrink-0 items-center justify-center rounded-full border border-[#222] bg-[#0d0d0d] text-[#666] transition hover:border-[#444] hover:bg-[#151515] hover:text-[#999]"
              title="Reference Image"
            >
              <Image size={14} />
            </button>
          </div>

          {styleTags.length > 0 && (
            <div className="mb-3 flex min-h-6 flex-wrap items-center gap-1.5">
              {styleTags.map(tag => (
                <button
                  key={tag}
                  type="button"
                  className="group/tag flex max-w-[120px] items-center gap-1 rounded-full border border-[rgba(255,225,200,0.18)] bg-[rgba(255,220,190,0.12)] px-2.5 py-1 text-[11px] text-[rgba(255,235,215,0.86)] hover:border-[rgba(255,225,200,0.4)]"
                  title={`Remove ${tag}`}
                  onClick={() => removeStyleTag(tag)}
                >
                  <span className="truncate">{tag}</span>
                  <X size={10} className="opacity-55 group-hover/tag:opacity-100" />
                </button>
              ))}
            </div>
          )}

          <div className="flex justify-center gap-2 pt-1">
            <button
              type="button"
              className="relative flex h-11 w-11 items-center justify-center rounded-full border border-[rgba(255,235,215,0.42)] bg-[radial-gradient(circle_at_50%_46%,rgba(255,235,215,0.18),rgba(13,13,13,0.94)_66%)] text-[11px] font-bold text-[rgba(255,247,234,0.88)] shadow-[0_0_10px_rgba(255,210,170,0.14)]"
              title="Drift"
            >
              60
            </button>
            <button
              type="button"
              className="nodrag nowheel flex h-11 min-w-[92px] items-center justify-center gap-1.5 rounded-full border border-[#222] bg-[#0d0d0d] px-3 text-[10px] font-semibold uppercase text-[#777] transition hover:-translate-y-0.5 hover:border-[#444] hover:bg-[#151515] hover:text-[#bbb]"
              title="Reset Cue scene to the default prompt"
              aria-label="Reset Cue scene"
              onPointerDown={stopGraphInteraction}
              onMouseDown={stopGraphInteraction}
              onClick={fireResetFromControl}
              onKeyDown={event => {
                if (event.key !== "Enter" && event.key !== " ") return;
                event.preventDefault();
                event.stopPropagation();
                void resetCue();
              }}
            >
              <RotateCcw size={15} />
              <span>Reset</span>
            </button>
            <button
              type="button"
              className={`flex h-11 w-11 items-center justify-center rounded-full border transition hover:-translate-y-0.5 ${
                cameraActive
                  ? "border-[#00ff66] bg-transparent text-[#00ff66] shadow-[0_0_15px_rgba(0,255,0,0.22)]"
                  : "border-[#222] bg-[#0d0d0d] text-[#666] hover:border-[#444] hover:bg-[#151515] hover:text-[#999]"
              }`}
              title={cameraActive ? "Stop camera feedback" : "Start camera feedback"}
              onClick={() => {
                if (cameraActive) stopCameraFeedback();
                else void startCameraFeedback();
              }}
            >
              <Eye size={18} />
            </button>
            <button
              type="button"
              className="flex h-11 w-11 items-center justify-center rounded-full border border-[#222] bg-[#0d0d0d] text-[#666] transition hover:-translate-y-0.5 hover:border-[#444] hover:bg-[#151515] hover:text-[#999]"
              title={profileLabel}
              onClick={() => setDrawer(drawer === "workflow" ? null : "workflow")}
            >
              <FileText size={18} />
            </button>
            <button
              type="button"
              className="flex h-11 w-11 items-center justify-center rounded-full border border-[#222] bg-[#0d0d0d] text-[#666] transition hover:-translate-y-0.5 hover:border-[#444] hover:bg-[#151515] hover:text-[#999]"
              title={`${sessionId} / ${baseUrl.replace(/^https?:\/\//, "")}`}
              onClick={() => setDrawer(drawer === "tune" ? null : "tune")}
            >
              <Settings2 size={18} />
            </button>
          </div>
        </div>
      </div>

      {drawer === "workflow" && <WorkflowDrawer manifest={cueManifest} />}

      {drawer === "tune" && (
        <TuneDrawer
          baseUrl={baseUrl}
          sessionId={sessionId}
          actionType={actionType}
          timeoutMs={timeoutMs}
          pollIntervalMs={pollIntervalMs}
          enabled={enabled}
          inputMapping={inputMapping}
          outputMapping={outputMapping}
          setParam={setParam}
        />
      )}

      {drawer === "debug" && (
        <DebugDrawer
          status={status}
          state={state}
          agent={agent}
          cueFileJson={cueFileJson}
          liveManifestLoaded={Boolean(liveCueManifest)}
        />
      )}
    </div>
  );
}

function StatusPill({ live, status }: { live: boolean; status: string }) {
  return (
    <span
      className={`inline-flex max-w-[82px] items-center gap-2 truncate text-[12px] ${
        live ? "text-[#888]" : "text-[#444]"
      }`}
      title={status}
    >
      <Radio
        size={8}
        className={`shrink-0 rounded-full ${
          live ? "fill-[#ff3b3b] text-[#ff3b3b]" : "fill-[#444] text-[#444]"
        }`}
      />
      <span className="truncate">{live ? "Live" : compactStatus(status)}</span>
    </span>
  );
}

function IconButton({
  title,
  active = false,
  children,
  onClick,
}: {
  title: string;
  active?: boolean;
  children: ReactNode;
  onClick: () => void;
}) {
  return (
    <button
      type="button"
      className={`rounded p-1.5 transition ${
        active
          ? "bg-[#ff3b3b]/15 text-[#ff3b3b]"
          : "text-[#444] hover:bg-[#1a1a1a] hover:text-[#888]"
      }`}
      title={title}
      onClick={onClick}
    >
      {children}
    </button>
  );
}

function MessageBubble({ message }: { message: ChatMessage }) {
  if (message.toolCall) {
    return <ToolCallBubble message={message} toolCall={message.toolCall} />;
  }

  const isVoice = message.role === "user";
  const isPrompt = message.role === "prompt" || message.role === "action";
  const isResponse = message.role === "cue" || message.role === "vision";
  const bubbleClass = isVoice
    ? "self-end rounded-br bg-gradient-to-br from-[rgba(255,220,190,0.2)] to-[rgba(255,220,190,0.12)] text-white shadow-[0_2px_12px_rgba(255,220,190,0.10)]"
    : isPrompt
      ? "self-start rounded-bl border border-[#1a1a1a] bg-gradient-to-br from-[#0d0d0d] to-[#0a0a0a] font-mono text-[12px] tracking-[0.2px] text-[#666]"
      : isResponse
        ? "self-start rounded-bl border border-[rgba(255,225,200,0.2)] bg-gradient-to-br from-[#1a1a2e] to-[#16213e] text-[rgba(255,235,215,0.9)]"
        : "self-start rounded-bl border border-[#1a1a1a] bg-[#0d0d0d] text-[#777]";
  const prefix = isPrompt ? ">" : isResponse ? "Cue" : "";

  return (
    <div
      className={`group relative max-w-[88%] rounded-2xl px-4 py-3 text-[13px] leading-[1.55] transition ${bubbleClass}`}
    >
      {isPrompt && (
        <span className="absolute -left-3 top-3 text-[10px] text-[#333]">{prefix}</span>
      )}
      {isResponse && (
        <div className="mb-1 text-[10px] uppercase tracking-[0.8px] text-[rgba(255,235,215,0.42)]">
          {prefix}
        </div>
      )}
      <div className="line-clamp-5">{message.text}</div>
      <div className="mt-2 flex items-center justify-between gap-3 opacity-60 transition group-hover:opacity-100">
        <span className="truncate text-[10px] text-[#555]">{message.meta}</span>
        <span className="shrink-0 text-[10px] text-[#444]">{roleLabel(message.role)}</span>
      </div>
    </div>
  );
}

function ToolCallBubble({
  message,
  toolCall,
}: {
  message: ChatMessage;
  toolCall: ToolCallSummary;
}) {
  const isPass = toolCall.kind === "pass";
  const confidence =
    typeof toolCall.confidence === "number"
      ? `${Math.round(toolCall.confidence * 100)}%`
      : "";
  const shellClass = isPass
    ? "border-[#1a1a1a] bg-[#080808] text-[#777]"
    : "border-[rgba(255,225,200,0.18)] bg-[rgba(255,220,190,0.07)] text-[rgba(255,235,215,0.78)]";
  const statusClass = isPass
    ? "border-[#242424] bg-[#111] text-[#777]"
    : "border-[rgba(0,255,136,0.22)] bg-[rgba(0,255,136,0.07)] text-[#86efac]";
  const [expanded, setExpanded] = useState(false);
  const promptPreview = toolCall.prompt ? compactText(toolCall.prompt, expanded ? 520 : 190) : "";
  const reasonPreview = toolCall.reason ? compactText(toolCall.reason, isPass ? 120 : 150) : "";
  const canExpand = Boolean(toolCall.reason || (toolCall.prompt && toolCall.prompt.length > 190));
  const primaryText = isPass ? reasonPreview : promptPreview || reasonPreview;

  return (
    <div
      className={`group relative w-[calc(100%-4px)] max-w-full min-w-0 self-start overflow-hidden rounded-lg border px-2 py-1.5 text-[10px] leading-[1.3] transition ${shellClass}`}
    >
      <div
        className={`absolute inset-y-2 left-0 w-0.5 rounded-r-full ${
          isPass ? "bg-[#2a2a2a]" : "bg-[rgba(255,225,200,0.58)]"
        }`}
      />
      <div className="flex min-w-0 items-center gap-1 pl-1">
        <span
          className={`inline-flex h-3.5 w-3.5 shrink-0 items-center justify-center rounded-full border ${
            isPass
              ? "border-[#242424] bg-[#111] text-[#666]"
              : "border-[rgba(255,225,200,0.24)] bg-[rgba(255,220,190,0.12)] text-[rgba(255,235,215,0.85)]"
          }`}
        >
          {isPass ? <Eye size={9} /> : <Activity size={9} />}
        </span>
        <span className={`rounded-full border px-1.5 py-px text-[7px] uppercase ${statusClass}`}>
          {isPass ? "Pass" : "Action"}
        </span>
        {toolCall.reset && (
          <span className="inline-flex items-center gap-0.5 rounded-full border border-[rgba(255,90,90,0.28)] bg-[rgba(255,40,40,0.08)] px-1.5 py-px text-[7px] uppercase text-[#ff8a8a]">
            <RotateCcw size={7} />
            reset
          </span>
        )}
        {confidence && (
          <span className="rounded-full border border-[#242424] bg-[#101010] px-1.5 py-px text-[7px] uppercase text-[#666]">
            {confidence}
          </span>
        )}
        {canExpand && (
          <button
            type="button"
            className="ml-auto shrink-0 rounded px-1 py-0.5 text-[7px] uppercase text-[#555] transition hover:bg-[#161616] hover:text-[#999]"
            onClick={() => setExpanded(value => !value)}
          >
            {expanded ? "Less" : "More"}
          </button>
        )}
      </div>

      {primaryText && (
        <div
          className={`mt-1 pl-1 ${
            isPass
              ? "line-clamp-1 text-[10px] text-[#9a9a9a]"
              : `font-mono text-[9px] leading-[1.3] text-[#858585] ${
                  expanded ? "line-clamp-none" : "line-clamp-2"
                }`
          }`}
        >
          {primaryText}
        </div>
      )}

      {expanded && toolCall.reason && !isPass && (
        <div className="mt-1.5 border-t border-[rgba(255,255,255,0.05)] pt-1.5 pl-1">
          <div className="mb-0.5 text-[8px] uppercase text-[#4d4d4d]">Rationale</div>
          <div className="break-words text-[9px] leading-snug text-[#777]">
            {toolCall.reason}
          </div>
        </div>
      )}

      {!primaryText && (
        <div className="mt-1 line-clamp-1 pl-1 text-[10px] text-[#777]">
          {message.text}
        </div>
      )}
    </div>
  );
}

function LatestDecisionPanel({
  message,
  toolCall,
}: {
  message: ChatMessage;
  toolCall: ToolCallSummary;
}) {
  const isPass = toolCall.kind === "pass";
  const confidence =
    typeof toolCall.confidence === "number"
      ? `${Math.round(toolCall.confidence * 100)}%`
      : "";
  const primaryText =
    (isPass ? toolCall.reason : toolCall.prompt || toolCall.reason) ||
    message.text ||
    toolCall.toolName;
  const label = isPass ? "Pass" : toolCall.reset ? "Action · reset" : "Action";
  return (
    <div
      className={`mx-4 mb-2 rounded-lg border px-3 py-2 shadow-[0_8px_24px_rgba(0,0,0,0.28)] ${
        isPass
          ? "border-[#242424] bg-[#090909] text-[#9a9a9a]"
          : "border-[rgba(255,225,200,0.24)] bg-[rgba(255,220,190,0.10)] text-[rgba(255,235,215,0.88)]"
      }`}
    >
      <div className="mb-1 flex min-w-0 items-center gap-1.5">
        <span
          className={`inline-flex h-4 w-4 shrink-0 items-center justify-center rounded-full border ${
            isPass
              ? "border-[#2a2a2a] bg-[#111] text-[#777]"
              : "border-[rgba(255,225,200,0.28)] bg-[rgba(255,220,190,0.12)] text-[rgba(255,235,215,0.92)]"
          }`}
        >
          {isPass ? <Eye size={10} /> : <Activity size={10} />}
        </span>
        <span className="truncate text-[10px] uppercase tracking-[0.4px]">
          {label}
        </span>
        {confidence && (
          <span className="ml-auto shrink-0 rounded-full border border-[#2a2a2a] bg-[#101010] px-1.5 py-px text-[8px] text-[#777]">
            {confidence}
          </span>
        )}
      </div>
      <div
        className={`line-clamp-3 break-words text-[12px] leading-[1.35] ${
          isPass ? "text-[#b0b0b0]" : "font-mono text-[#d0c7bf]"
        }`}
        title={primaryText}
      >
        {primaryText}
      </div>
    </div>
  );
}

function WorkflowDrawer({ manifest }: { manifest: CueWorkflowManifest | null }) {
  if (!manifest) {
    return (
      <div className="border-t border-[#1a1a1a] bg-[#050505] p-3 text-[11px] text-[#666]">
        Load a `.cue` manifest to inspect prompts, cues, tools, inputs, outputs,
        providers, runtime settings, and evals.
      </div>
    );
  }

  return (
    <div className="max-h-[310px] overflow-auto border-t border-[#1a1a1a] bg-[#050505] p-3">
      <div className="mb-3">
        <div className="flex items-start justify-between gap-2">
          <div className="min-w-0">
            <div className="truncate text-[13px] font-semibold text-white">
              {manifest.title}
            </div>
            <div className="mt-0.5 flex flex-wrap gap-1.5 text-[9px] uppercase text-[#666]">
              {manifest.version && <span>v {manifest.version}</span>}
              {manifest.model && <span>{manifest.model}</span>}
            </div>
          </div>
          <span className="rounded-full border border-[#1a1a1a] px-2 py-1 text-[9px] uppercase text-[#888]">
            .cue
          </span>
        </div>
        {manifest.description && (
          <p className="mt-2 line-clamp-3 text-[11px] leading-snug text-[#888]">
            {manifest.description}
          </p>
        )}
      </div>

      <div className="mb-3 grid grid-cols-4 gap-1.5">
        <WorkflowStat label="Inputs" value={manifest.inputs.length} />
        <WorkflowStat label="Tools" value={manifest.tools.length} />
        <WorkflowStat label="Outputs" value={manifest.outputs.length} />
        <WorkflowStat label="Prompts" value={manifest.prompts.length} />
      </div>

      <div className="space-y-2">
        <WorkflowSection title="Prompts" items={manifest.prompts} />
        <WorkflowSection title="Cues" items={manifest.cues} />
        <WorkflowSection title="Tools" items={manifest.tools} />
        <WorkflowSection title="Inputs" items={manifest.inputs} />
        <WorkflowSection title="Outputs" items={manifest.outputs} />
        <WorkflowSection title="Providers" items={manifest.providers} />
        <WorkflowSection title="Runtime" items={manifest.runtime} />
        <WorkflowSection
          title="Style Tags"
          items={manifest.styleTags.map(tag => ({ name: tag }))}
        />
        <WorkflowSection title="Evals" items={manifest.evals} />
      </div>
    </div>
  );
}

function WorkflowStat({ label, value }: { label: string; value: number }) {
  return (
    <div className="rounded-md border border-[#1a1a1a] bg-black px-2 py-1.5">
      <div className="text-[13px] font-semibold text-white">{value}</div>
      <div className="text-[8px] uppercase text-[#666]">{label}</div>
    </div>
  );
}

function WorkflowSection({
  title,
  items,
}: {
  title: string;
  items: ManifestItem[];
}) {
  if (items.length === 0) return null;
  return (
    <section className="rounded-md border border-[#1a1a1a] bg-black p-2">
      <div className="mb-1.5 text-[9px] font-semibold uppercase text-[#888]">
        {title}
      </div>
      <div className="space-y-1.5">
        {items.slice(0, 8).map(item => (
          <div
            key={`${title}-${item.name}-${item.kind ?? ""}`}
            className="rounded border border-[#111] bg-[#080808] px-2 py-1.5"
          >
            <div className="flex items-center justify-between gap-2">
              <span className="truncate text-[11px] text-white">{item.name}</span>
              {item.kind && (
                <span className="shrink-0 rounded-full bg-[#1a1a1a] px-1.5 py-0.5 text-[8px] uppercase text-[#888]">
                  {item.kind}
                </span>
              )}
            </div>
            {item.detail && (
              <div className="mt-1 line-clamp-2 text-[10px] leading-snug text-[#777]">
                {item.detail}
              </div>
            )}
            {item.preview && (
              <div className="mt-1 line-clamp-3 font-mono text-[9px] leading-snug text-[#555]">
                {item.preview}
              </div>
            )}
          </div>
        ))}
        {items.length > 8 && (
          <div className="text-[9px] text-[#555]">+{items.length - 8} more</div>
        )}
      </div>
    </section>
  );
}

function TuneDrawer({
  baseUrl,
  sessionId,
  actionType,
  timeoutMs,
  pollIntervalMs,
  enabled,
  inputMapping,
  outputMapping,
  setParam,
}: {
  baseUrl: string;
  sessionId: string;
  actionType: string;
  timeoutMs: number;
  pollIntervalMs: number;
  enabled: boolean;
  inputMapping: Record<string, string>;
  outputMapping: Record<string, string>;
  setParam: (name: string, value: unknown) => void;
}) {
  return (
    <div className="rounded-md border border-white/5 bg-[#101010] p-2">
      <div className="mb-2 flex items-center gap-1 text-[9px] font-semibold uppercase text-[#8b949e]">
        <SlidersHorizontal size={11} />
        Tune
      </div>
      <div className="grid grid-cols-2 gap-1.5">
        <MiniField label="Base URL" value={baseUrl} onChange={v => setParam("cue_base_url", v)} />
        <MiniField label="Session" value={sessionId} onChange={v => setParam("session_id", v)} />
        <MiniField label="Action" value={actionType} onChange={v => setParam("action_type", v)} />
        <MiniField
          label="Timeout"
          value={String(timeoutMs)}
          onChange={v => setParam("timeout_ms", Number(v) || timeoutMs)}
        />
        <MiniField
          label="Poll"
          value={String(pollIntervalMs)}
          onChange={v => setParam("poll_interval_ms", Number(v) || pollIntervalMs)}
        />
        <label className="flex items-end gap-1.5 text-[9px] text-[#9ca3af]">
          <input
            type="checkbox"
            checked={enabled}
            onChange={event => setParam("enabled", event.currentTarget.checked)}
          />
          Enabled
        </label>
      </div>
      <div className="mt-2 grid grid-cols-2 gap-1.5">
        <MappingTextarea
          label="Observes"
          value={prettyJson(inputMapping)}
          onChange={v => setParam("input_mapping_json", v)}
        />
        <MappingTextarea
          label="Emits"
          value={prettyJson(outputMapping)}
          onChange={v => setParam("output_mapping_json", v)}
        />
      </div>
    </div>
  );
}

function MiniField({
  label,
  value,
  onChange,
}: {
  label: string;
  value: string;
  onChange: (value: string) => void;
}) {
  return (
    <label className="min-w-0 text-[9px] text-[#8b949e]">
      <span>{label}</span>
      <input
        value={value}
        onChange={event => onChange(event.currentTarget.value)}
        className="mt-0.5 w-full rounded border border-white/5 bg-[#1d1d1d] px-1.5 py-1 text-[9px] text-white outline-none focus:border-emerald-400/50"
      />
    </label>
  );
}

function MappingTextarea({
  label,
  value,
  onChange,
}: {
  label: string;
  value: string;
  onChange: (value: string) => void;
}) {
  return (
    <label className="min-w-0 text-[9px] text-[#8b949e]">
      <span>{label}</span>
      <textarea
        value={value}
        onChange={event => onChange(event.currentTarget.value)}
        className="mt-0.5 h-[74px] w-full resize-none rounded border border-white/5 bg-[#1d1d1d] px-1.5 py-1 font-mono text-[8px] text-white outline-none focus:border-emerald-400/50"
      />
    </label>
  );
}

function DebugDrawer({
  status,
  state,
  agent,
  cueFileJson,
  liveManifestLoaded,
}: {
  status: string;
  state: JsonRecord | null;
  agent: JsonRecord | null;
  cueFileJson: string;
  liveManifestLoaded: boolean;
}) {
  const payload = {
    status,
    state: state ? summarizeState(state) : null,
    agent: agent ? summarizeAgent(agent) : null,
    liveManifestLoaded,
    cueFileLoaded: Boolean(cueFileJson),
  };
  return (
    <pre className="max-h-[150px] overflow-auto rounded-md border border-white/5 bg-[#0b0b0b] p-2 text-[8px] leading-snug text-[#a3a3a3]">
      {JSON.stringify(payload, null, 2)}
    </pre>
  );
}

function manifestFromCueFile(
  raw: string,
  filename = ""
): CueWorkflowManifest | null {
  const decoded = parseJsonObject(raw);
  if (!decoded) return null;
  return manifestFromRecord(decoded, filename);
}

function manifestFromAgent(agent: JsonRecord | null): CueWorkflowManifest | null {
  if (!agent) return null;
  const liveManifest = workflowManifestFromAgent(agent);
  return liveManifest ? manifestFromRecord(liveManifest, "Cue server") : null;
}

function defaultPromptFromCueFile(raw: string): string {
  const decoded = parseJsonObject(raw);
  return decoded ? defaultPromptFromWorkflowManifest(decoded) : "";
}

function defaultPromptFromAgent(agent: JsonRecord | null): string {
  if (!agent) return "";

  const extensions = isRecord(agent.extensions) ? agent.extensions : {};
  const etherea = isRecord(extensions.ethereaPromptEngine)
    ? extensions.ethereaPromptEngine
    : {};
  const extensionPrompt = firstStringFromFields(etherea, [
    "defaultPrompt",
    "default_prompt",
    "initialPrompt",
    "seedPrompt",
  ]);
  if (extensionPrompt) return extensionPrompt;

  const liveManifest = workflowManifestFromAgent(agent);
  return liveManifest ? defaultPromptFromWorkflowManifest(liveManifest) : "";
}

function defaultPromptFromWorkflowManifest(manifest: JsonRecord): string {
  const root = manifestRoot(manifest);
  const direct = firstStringFromFields(root, [
    "defaultPrompt",
    "default_prompt",
    "initialPrompt",
    "seedPrompt",
  ]);
  if (direct) return direct;

  const runtimeProfile = isRecord(root.runtimeProfile) ? root.runtimeProfile : {};
  const defaults = isRecord(runtimeProfile.defaults) ? runtimeProfile.defaults : {};
  const profilePrompt = firstStringFromFields(defaults, [
    "defaultPrompt",
    "default_prompt",
    "initialPrompt",
    "seedPrompt",
  ]);
  if (profilePrompt) return profilePrompt;

  const runtime = isRecord(root.runtime) ? root.runtime : {};
  const runtimeDefaults = isRecord(runtime.defaults) ? runtime.defaults : {};
  return firstStringFromFields(runtimeDefaults, [
    "defaultPrompt",
    "default_prompt",
    "initialPrompt",
    "seedPrompt",
  ]) ?? "";
}

function workflowManifestFromAgent(agent: JsonRecord): JsonRecord | null {
  const direct = agent.workflowManifest;
  if (isRecord(direct)) return direct;
  if (typeof direct === "string") return parseJsonObject(direct);

  const extensions = agent.extensions;
  if (isRecord(extensions)) {
    const extensionManifest = extensions.workflowManifest ?? extensions.workflow;
    if (isRecord(extensionManifest)) return extensionManifest;
    if (typeof extensionManifest === "string") return parseJsonObject(extensionManifest);
  }

  return null;
}

function manifestFromRecord(
  manifest: JsonRecord,
  filename = ""
): CueWorkflowManifest {
  const root = manifestRoot(manifest);
  const title =
    stringFromFields(root, ["title", "label", "name", "id"]) || filename || "Cue Workflow";
  const prompts = collectPromptItems(root);
  const tools = normalizeManifestItems([
    ...collectManifestItems(root, ["tools"], "tool"),
    ...collectManifestItems(root, ["actions"], "action"),
    ...collectProgramTools(root),
  ]);

  return {
    title,
    version: stringFromFields(root, ["version", "promptVersion", "schemaVersion"]),
    description: stringFromFields(root, ["description", "summary"]),
    model: modelLabel(root),
    inputs: collectManifestItems(root, ["inputs", "sources"], "input"),
    outputs: collectManifestItems(root, ["outputs"], "output"),
    tools,
    cues: normalizeManifestItems([
      ...collectManifestItems(root, ["cues"], "cue"),
      ...collectManifestItems(root, ["triggers", "heartbeats"], "trigger"),
    ]),
    prompts,
    evals: collectManifestItems(root, ["evals", "evaluations", "tests"], "eval"),
    providers: collectProviderItems(root),
    runtime: collectRuntimeItems(root),
    styleTags: collectStyleTags(root),
  };
}

function manifestRoot(root: JsonRecord): JsonRecord {
  for (const key of ["workflow", "manifest"]) {
    if (isRecord(root[key])) return root[key];
  }
  return root;
}

function collectManifestItems(
  root: JsonRecord,
  fields: string[],
  fallbackKind: string
): ManifestItem[] {
  const items: ManifestItem[] = [];
  for (const field of fields) {
    const value = root[field];
    if (value === undefined) continue;
    items.push(...itemsFromUnknown(value, fallbackKind));
  }
  return normalizeManifestItems(items);
}

function collectPromptItems(root: JsonRecord): ManifestItem[] {
  const items: ManifestItem[] = [];
  for (const field of ["prompts", "promptSet", "prompt_set", "llmPrompt"]) {
    const value = root[field];
    if (value !== undefined) items.push(...promptItemsFromUnknown(value, field));
  }

  for (const field of ["system", "systemPrompt", "userTemplate", "prompt"]) {
    const value = root[field];
    if (typeof value === "string" && value.trim()) {
      items.push({ name: field, kind: "prompt", preview: compactText(value, 340) });
    }
  }

  for (const field of ["llm", "model", "provider"]) {
    const value = root[field];
    if (isRecord(value)) items.push(...promptItemsFromUnknown(value, field));
  }

  const programs = root.programs;
  if (Array.isArray(programs)) {
    for (const program of programs) {
      if (!isRecord(program)) continue;
      items.push(...promptItemsFromUnknown(program, stringFromFields(program, ["name", "id"]) || "program"));
    }
  }

  return normalizeManifestItems(items);
}

function promptItemsFromUnknown(value: unknown, fallbackName: string): ManifestItem[] {
  if (typeof value === "string" && value.trim()) {
    return [{ name: fallbackName, kind: "prompt", preview: compactText(value, 340) }];
  }
  if (Array.isArray(value)) {
    return value.flatMap((item, index) =>
      promptItemsFromUnknown(item, `${fallbackName}.${index + 1}`)
    );
  }
  if (!isRecord(value)) return [];

  const direct: ManifestItem[] = [];
  for (const field of ["system", "systemPrompt", "user", "userTemplate", "template", "prompt"]) {
    const text = value[field];
    if (typeof text === "string" && text.trim()) {
      direct.push({
        name: field,
        kind: "prompt",
        detail: stringFromFields(value, ["description", "label"]),
        preview: compactText(text, 340),
      });
    }
  }
  if (direct.length > 0) return direct;

  return Object.entries(value).flatMap(([key, item]) => {
    if (typeof item === "string" && item.trim()) {
      return [{ name: key, kind: "prompt", preview: compactText(item, 340) }];
    }
    if (isRecord(item)) {
      const text = firstStringFromFields(item, [
        "system",
        "systemPrompt",
        "user",
        "userTemplate",
        "template",
        "prompt",
        "content",
        "text",
      ]);
      return [
        {
          name: stringFromFields(item, ["name", "id", "title"]) || key,
          kind: stringFromFields(item, ["kind", "type"]) || "prompt",
          detail: stringFromFields(item, ["description", "label"]),
          preview: text ? compactText(text, 340) : compactText(JSON.stringify(item), 240),
        },
      ];
    }
    return [];
  });
}

function collectProgramTools(root: JsonRecord): ManifestItem[] {
  const programs = root.programs;
  if (!Array.isArray(programs)) return [];
  const items: ManifestItem[] = [];
  for (const program of programs) {
    if (!isRecord(program)) continue;
    const programName = stringFromFields(program, ["name", "id"]) || "program";
    for (const field of ["allowedTools", "tools", "actions"]) {
      for (const item of itemsFromUnknown(program[field], "tool")) {
        items.push({
          ...item,
          detail: item.detail || `Used by ${programName}`,
        });
      }
    }
  }
  return items;
}

function collectProviderItems(root: JsonRecord): ManifestItem[] {
  const items: ManifestItem[] = [];
  for (const field of [
    "model",
    "llm",
    "transcriptionProvider",
    "transcription",
    "vlmProvider",
    "visionProvider",
    "frameProviders",
    "outputProviders",
    "providers",
  ]) {
    const value = root[field];
    if (value === undefined) continue;
    items.push(...itemsFromUnknown(value, "provider"));
  }
  return normalizeManifestItems(items);
}

function collectRuntimeItems(root: JsonRecord): ManifestItem[] {
  const items: ManifestItem[] = [];
  for (const field of ["runtime", "runtimeProfile", "profile", "parameters", "config"]) {
    const value = root[field];
    if (value === undefined) continue;
    if (isRecord(value)) {
      for (const [key, item] of Object.entries(value)) {
        items.push({
          name: key,
          kind: "runtime",
          preview: compactText(JSON.stringify(item), 160),
        });
      }
    } else {
      items.push(...itemsFromUnknown(value, "runtime"));
    }
  }
  return normalizeManifestItems(items);
}

function collectStyleTags(root: JsonRecord): string[] {
  const values: unknown[] = [];
  for (const field of ["styleTags", "style_tags", "tags"]) {
    values.push(root[field]);
  }
  if (isRecord(root.runtime)) {
    values.push(root.runtime.styleTags, root.runtime.style_tags);
  }
  const tags = new Set<string>();
  for (const value of values) {
    const list = Array.isArray(value) ? value : typeof value === "string" ? value.split(",") : [];
    for (const item of list) {
      if (typeof item !== "string") continue;
      const tag = item.trim();
      if (tag) tags.add(tag);
    }
  }
  return [...tags].slice(0, 12);
}

function itemsFromUnknown(value: unknown, fallbackKind: string): ManifestItem[] {
  if (value === undefined || value === null) return [];
  if (typeof value === "string") {
    const name = value.trim();
    return name ? [{ name, kind: fallbackKind }] : [];
  }
  if (Array.isArray(value)) {
    return value.flatMap((item, index) => itemFromUnknown(item, fallbackKind, index));
  }
  if (isRecord(value)) {
    return Object.entries(value).flatMap(([key, item], index) =>
      itemFromUnknown(item, fallbackKind, index, key)
    );
  }
  return [];
}

function itemFromUnknown(
  value: unknown,
  fallbackKind: string,
  index: number,
  key?: string
): ManifestItem[] {
  if (typeof value === "string") {
    const name = value.trim();
    return name ? [{ name, kind: fallbackKind }] : [];
  }
  if (!isRecord(value)) {
    return key ? [{ name: key, kind: fallbackKind, preview: String(value) }] : [];
  }
  return [
    {
      name:
        stringFromFields(value, ["name", "id", "title", "tool", "type", "provider"]) ||
        key ||
        `${fallbackKind}-${index + 1}`,
      kind: stringFromFields(value, ["kind", "type", "provider", "event", "transport"]) || fallbackKind,
      detail: stringFromFields(value, ["description", "label", "summary"]),
      preview: previewForManifestRecord(value),
    },
  ];
}

function normalizeManifestItems(items: ManifestItem[]): ManifestItem[] {
  const seen = new Set<string>();
  const result: ManifestItem[] = [];
  for (const item of items) {
    const name = item.name.trim();
    if (!name) continue;
    const key = `${name}:${item.kind ?? ""}`;
    if (seen.has(key)) continue;
    seen.add(key);
    result.push({ ...item, name });
  }
  return result.slice(0, 40);
}

function manifestSignature(manifest: CueWorkflowManifest): string {
  return JSON.stringify({
    title: manifest.title,
    version: manifest.version,
    inputs: manifest.inputs.map(item => item.name),
    outputs: manifest.outputs.map(item => item.name),
    tools: manifest.tools.map(item => item.name),
  });
}

function modelLabel(root: JsonRecord): string | undefined {
  for (const field of ["model", "llm"]) {
    const value = root[field];
    if (typeof value === "string" && value.trim()) return value.trim();
    if (isRecord(value)) {
      const provider = stringFromFields(value, ["provider", "name", "id"]);
      const model = stringFromFields(value, ["model", "modelId"]);
      if (provider && model) return `${provider} / ${model}`;
      if (provider || model) return provider || model;
    }
  }
  if (isRecord(root.providers) && isRecord(root.providers.llm)) {
    const provider = stringFromFields(root.providers.llm, ["provider", "type", "name", "id"]);
    const modelField = root.providers.llm.model;
    const model =
      typeof modelField === "string"
        ? modelField
        : isRecord(modelField)
          ? stringFromFields(modelField, ["default", "model", "id"])
          : undefined;
    if (provider && model) return `${provider} / ${model}`;
    if (provider || model) return provider || model;
  }
  return undefined;
}

function previewForManifestRecord(value: JsonRecord): string | undefined {
  const text = firstStringFromFields(value, [
    "prompt",
    "system",
    "systemPrompt",
    "userTemplate",
    "template",
    "content",
    "text",
  ]);
  if (text) return compactText(text, 220);
  const schema = value.inputSchema ?? value.schema ?? value.parameters;
  if (schema !== undefined) return compactText(JSON.stringify(schema), 180);
  return undefined;
}

function firstStringFromFields(root: JsonRecord, fields: string[]): string | undefined {
  for (const field of fields) {
    const value = root[field];
    if (typeof value === "string" && value.trim()) return value.trim();
  }
  return undefined;
}

function stringFromFields(root: JsonRecord, fields: string[]): string | undefined {
  return firstStringFromFields(root, fields);
}

function numberFromUnknown(value: unknown): number | undefined {
  return typeof value === "number" && Number.isFinite(value) ? value : undefined;
}

function compactText(value: string, maxLength: number): string {
  const compacted = value.replace(/\s+/g, " ").trim();
  if (compacted.length <= maxLength) return compacted;
  return `${compacted.slice(0, Math.max(maxLength - 3, 0))}...`;
}

function inputCueType(item: ManifestItem): string {
  const text = `${item.name} ${item.kind ?? ""} ${item.detail ?? ""}`.toLowerCase();
  if (text.includes("transcript") || text.includes("speech") || text.includes("chat")) {
    return "transcript.segment";
  }
  if (text.includes("vision") || text.includes("frame") || text.includes("image")) {
    return "vision.description";
  }
  if (text.includes("signal")) return "signal.value";
  if (text.includes("control") || text.includes("reset")) return "scope.control";
  return "scope.context";
}

function outputCueType(
  item: ManifestItem,
  currentMapping: Record<string, string>
): string {
  const text = `${item.name} ${item.kind ?? ""} ${item.detail ?? ""}`.toLowerCase();
  if (item.kind === "tool" || item.kind === "action") return "cue.action";
  if (text.includes("action") || text.includes("tool")) return "cue.action";
  if (currentMapping[item.name]) return currentMapping[item.name];
  if (text.includes("reset")) return "longlive.reset_cache";
  if (text.includes("prompt") || text.includes("longlive") || text.includes("wan")) {
    return "longlive.prompt";
  }
  if (text.includes("transcript")) return "chat.transcript";
  if (text.includes("shader")) return "scope.shader";
  if (text.includes("param")) return "scope.params";
  if (text.includes("video")) return "scope.video";
  return item.name;
}

function pruneStaleManifestMappings(
  mapping: Record<string, string>,
  manifest: CueWorkflowManifest
): Record<string, string> {
  const activeToolNames = new Set(manifest.tools.map(item => item.name));
  const result: Record<string, string> = {};
  for (const [key, value] of Object.entries(mapping)) {
    if (LEGACY_PROMPT_TOOL_NAMES.has(key) && !activeToolNames.has(key)) continue;
    result[key] = value;
  }
  return result;
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

function stringListParam(
  data: FlowNodeData,
  params: NodeParamDef[],
  name: string
): string[] {
  const value = data.customNodeParams?.[name];
  const def = params.find(param => param.name === name)?.default;
  const decoded =
    typeof value === "string"
      ? parseJsonArray(value)
      : Array.isArray(value)
        ? value
        : typeof def === "string"
          ? parseJsonArray(def)
          : Array.isArray(def)
            ? def
            : [];
  const tags = new Set<string>();
  for (const item of decoded) {
    if (typeof item !== "string") continue;
    const tag = item.trim();
    if (tag) tags.add(tag);
  }
  return [...tags].slice(0, 12);
}

function parseMapping(value: string, fallback: Record<string, string>) {
  const decoded = parseJsonObject(value);
  if (!decoded) return fallback;
  const entries = Object.entries(decoded)
    .filter(([, v]) => typeof v === "string")
    .map(([k, v]) => [k, String(v)] as const);
  return entries.length > 0 ? Object.fromEntries(entries) : fallback;
}

function resetRuntimeTargets(
  outputMapping: Record<string, string>,
  toolCall?: ToolCallSummary
): RuntimeParameterTarget[] {
  const keys = [
    "reset",
    toolCall?.actionType,
    toolCall?.toolName,
    "video.update_prompt",
    "video.create_scene",
  ].filter((key): key is string => Boolean(key));
  const targets = keys
    .map(key => outputMapping[key])
    .filter((value): value is string => Boolean(value))
    .map(parseRuntimeParameterTarget)
    .filter((target): target is RuntimeParameterTarget => Boolean(target))
    .filter(target => target.paramName === "reset_cache");

  return dedupeRuntimeParameterTargets(
    targets.length > 0
      ? targets
      : [parseRuntimeParameterTarget(DEFAULT_RESET_RUNTIME_TARGET)].filter(
          (target): target is RuntimeParameterTarget => Boolean(target)
        )
  );
}

function promptRuntimeTargets(
  outputMapping: Record<string, string>,
  toolCall?: ToolCallSummary
): RuntimeParameterTarget[] {
  const keys = [
    toolCall?.actionType,
    toolCall?.toolName,
    "video.update_prompt",
    "video.update_scene",
    "prompt",
  ].filter((key): key is string => Boolean(key));
  const targets = keys
    .map(key => outputMapping[key])
    .filter((value): value is string => Boolean(value))
    .map(parseRuntimeParameterTarget)
    .filter((target): target is RuntimeParameterTarget => Boolean(target))
    .filter(target => target.paramName === "prompt" || target.paramName === "prompts");

  return dedupeRuntimeParameterTargets(
    targets.length > 0
      ? targets
      : [parseRuntimeParameterTarget("longlive.prompt")].filter(
          (target): target is RuntimeParameterTarget => Boolean(target)
        )
  );
}

function parseRuntimeParameterTarget(value: string): RuntimeParameterTarget | null {
  const trimmed = value.trim();
  const separator = trimmed.indexOf(".");
  if (separator <= 0 || separator >= trimmed.length - 1) return null;
  return {
    nodeId: trimmed.slice(0, separator),
    paramName: trimmed.slice(separator + 1),
  };
}

function dedupeRuntimeParameterTargets(
  targets: RuntimeParameterTarget[]
): RuntimeParameterTarget[] {
  const seen = new Set<string>();
  const result: RuntimeParameterTarget[] = [];
  for (const target of targets) {
    const key = `${target.nodeId}\0${target.paramName}`;
    if (seen.has(key)) continue;
    seen.add(key);
    result.push(target);
  }
  return result;
}

function randomScopeSeed(): number {
  return Math.floor(Math.random() * 1_000_000) + 1;
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

function parseJsonArray(value: string): unknown[] {
  if (!value.trim()) return [];
  try {
    const decoded = JSON.parse(value);
    return Array.isArray(decoded) ? decoded : [];
  } catch {
    return [];
  }
}

function prettyJson(value: Record<string, unknown>): string {
  return JSON.stringify(value, null, 2);
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

function styleTagsObservation(tags: string[]): Record<string, unknown> {
  return {
    type: "scope.style_tags",
    source: "scope.chat",
    timestamp: Date.now() / 1000,
    payload: {
      tags,
    },
  };
}

function controlObservation(
  control: string,
  payload: Record<string, unknown> = {}
): Record<string, unknown> {
  return {
    type: "scope.control",
    source: "scope.chat",
    timestamp: Date.now() / 1000,
    payload: {
      control,
      ...payload,
    },
  };
}

function messagesFromState(state: JsonRecord | null): ChatMessage[] {
  if (!state) return [];
  const transcript = transcriptTextFromState(state);
  const latestAction = latestPromptAction(state);
  const promptPayload = isRecord(latestAction?.payload) ? latestAction.payload : {};
  const prompt = typeof promptPayload.prompt === "string" ? promptPayload.prompt : "";
  const decisionMessages = decisionMessagesFromState(state, 4);
  const messages: ChatMessage[] = [];

  if (prompt && decisionMessages.length === 0) {
    messages.push({
      id: `prompt-${hashText(prompt)}`,
      role: "prompt",
      text: prompt,
      meta: "prompt",
    });
  }
  messages.push(...decisionMessages);
  if (transcript) {
    messages.push({
      id: `transcript-${hashText(transcript)}`,
      role: "user",
      text: transcript,
      meta: "transcript",
    });
  }
  return messages;
}

function transcriptTextFromState(state: JsonRecord | null): string {
  if (!state) return "";
  const snapshot = isRecord(state.state) ? state.state : {};
  return typeof snapshot.transcript === "string" ? snapshot.transcript : "";
}

function latestPromptFromState(state: JsonRecord | null): string {
  if (!state) return "";
  const latestAction = latestPromptAction(state);
  const promptPayload = isRecord(latestAction?.payload) ? latestAction.payload : {};
  return typeof promptPayload.prompt === "string" ? promptPayload.prompt : "";
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

function decisionMessagesFromState(state: JsonRecord, limit: number): ChatMessage[] {
  for (const collectionName of ["decisionHistory", "decisionTrace"]) {
    const collection = state[collectionName];
    if (!Array.isArray(collection) || collection.length === 0) continue;
    const messages: ChatMessage[] = [];
    for (const item of [...collection].reverse()) {
      if (messages.length >= limit) break;
      if (!isRecord(item)) continue;
      const message = decisionMessageFromItem(item);
      if (message) messages.push(message);
    }
    return messages;
  }
  return [];
}

function decisionMessageFromItem(item: JsonRecord): ChatMessage | null {
  const call = isRecord(item.call) ? item.call : {};
  const result = isRecord(item.result) ? item.result : {};
  const tool = firstStringFromFields(call, ["tool", "name"]) ||
    firstStringFromFields(item, ["tool", "toolName"]) ||
    firstStringFromFields(result, ["tool"]);
  const actions = Array.isArray(result.actions) ? result.actions.filter(isRecord) : [];
  const pass = isRecord(result.data) && isRecord(result.data.pass) ? result.data.pass : {};
  const argumentsRecord = isRecord(call.arguments) ? call.arguments : {};
  const action = actions[actions.length - 1];
  const actionPayload = isRecord(action?.payload) ? action.payload : {};
  const actionTypes = actions
    .map(candidate => (typeof candidate.type === "string" ? candidate.type : "action"))
    .join(", ");
  const toolName = tool || actionTypes || "tool call";
  const prompt =
    firstStringFromFields(argumentsRecord, ["prompt"]) ||
    firstStringFromFields(actionPayload, ["prompt"]);
  const reason =
    firstStringFromFields(argumentsRecord, ["rationale", "reason"]) ||
    firstStringFromFields(pass, ["reason"]) ||
    firstStringFromFields(item, ["reason"]);
  const reset = Boolean(argumentsRecord.reset === true || actionPayload.reset === true);
  const confidence = numberFromUnknown(call.confidence ?? pass.confidence);
  const isPass = toolName.includes("observe.pass") || (actions.length === 0 && Boolean(reason));
  const details: string[] = [];

  if (reason) details.push(reason);
  if (prompt) details.push(compactText(prompt, 360));
  if (!details.length && actionTypes) details.push(`emitted ${actionTypes}`);
  if (!details.length && !toolName) return null;

  return {
    id: toolCallMessageId({
      kind: isPass ? "pass" : "action",
      toolName,
      actionType: actionTypes || undefined,
      reason,
      prompt,
      reset,
    }),
    role: isPass ? "pass" : "action",
    text: details.join("\n") || toolName,
    meta: "tool call",
    toolCall: {
      toolName,
      kind: isPass ? "pass" : "action",
      actionType: actionTypes || undefined,
      reset,
      reason,
      prompt,
      actionCount: actions.length,
      confidence,
    },
  };
}

function decisionMessageFromHarnessResult(result: JsonRecord): ChatMessage | null {
  const toolResults = Array.isArray(result.toolResults) ? result.toolResults.filter(isRecord) : [];
  const toolCalls = Array.isArray(result.toolCalls) ? result.toolCalls.filter(isRecord) : [];
  const toolResult = toolResults[toolResults.length - 1];
  if (!toolResult) return null;
  const callId = typeof toolResult.callId === "string" ? toolResult.callId : "";
  const call =
    toolCalls.find(candidate => candidate.id === callId) ??
    toolCalls[toolCalls.length - 1] ??
    {};
  return decisionMessageFromItem({
    call,
    result: {
      tool: toolResult.tool,
      actions: Array.isArray(toolResult.actions) ? toolResult.actions : [],
      data: toolResult.data,
    },
  });
}

function actionMessageFromAction(action: JsonRecord): ChatMessage | null {
  const actionType = typeof action.type === "string" ? action.type : "action";
  const payload = isRecord(action.payload) ? action.payload : {};
  const prompt = firstStringFromFields(payload, ["prompt"]);
  const rationale = firstStringFromFields(payload, ["rationale", "reason"]);
  const reset = payload.reset === true;
  const details = [rationale, prompt ? compactText(prompt, 360) : ""].filter(Boolean);
  return {
    id: toolCallMessageId({
      kind: "action",
      toolName: actionType,
      actionType,
      reason: rationale,
      prompt,
      reset,
    }),
    role: "action",
    text: details.join("\n") || actionType,
    meta: "action emitted",
    toolCall: {
      toolName: actionType,
      kind: "action",
      actionType,
      reset,
      reason: rationale,
      prompt,
      actionCount: 1,
    },
  };
}

function toolCallMessageId(input: {
  kind: "action" | "pass";
  toolName: string;
  actionType?: string;
  reason?: string;
  prompt?: string;
  reset?: boolean;
}): string {
  if (input.kind === "action") {
    return `tool-action-${hashText(
      JSON.stringify({
        actionType: input.actionType ?? input.toolName,
        prompt: input.prompt ?? "",
        reason: input.reason ?? "",
        reset: input.reset === true,
      })
    )}`;
  }

  return `tool-pass-${hashText(
    JSON.stringify({
      toolName: input.toolName,
      reason: input.reason ?? "",
    })
  )}`;
}

function transcriptionPreviewFromEvent(
  event: JsonRecord
): { value: TranscriptionPreviewState; final: boolean } | null {
  if (event.type !== "transcript") return null;
  const text = transcriptTextFromEvent(event);
  if (!text) return null;

  const final = isFinalTranscriptEvent(event);
  if (!final) {
    return {
      value: {
        processed: "",
        unprocessed: text,
      },
      final: false,
    };
  }

  const fullTranscript =
    typeof event.fullTranscript === "string" ? event.fullTranscript.trim() : "";
  return {
    value: {
      processed: tailText(fullTranscript || text, 220),
      unprocessed: "",
    },
    final: true,
  };
}

function transcriptTextFromEvent(event: JsonRecord): string {
  return (
    typeof event.text === "string"
      ? event.text
      : typeof event.transcript === "string"
        ? event.transcript
        : ""
  ).trim();
}

function promptTextFromEvent(event: JsonRecord): string {
  if (event.type !== "prompt") return "";
  return typeof event.prompt === "string" ? event.prompt.trim() : "";
}

function isFinalTranscriptEvent(event: JsonRecord): boolean {
  if (event.isFinal === false && event.speechFinal !== true) return false;
  return event.isFinal === true || event.speechFinal === true || event.isFinal === undefined;
}

function tailText(value: string, maxChars: number): string {
  if (value.length <= maxChars) return value;
  return `...${value.slice(value.length - maxChars).trimStart()}`;
}

function messageFromCueEvent(event: JsonRecord): ChatMessage | null {
  const type = typeof event.type === "string" ? event.type : "";
  if (type === "transcript") {
    if (!isFinalTranscriptEvent(event)) return null;
    const text = transcriptTextFromEvent(event);
    if (!text) return null;
    return {
      id: `event-transcript-${hashText(text)}`,
      role: "user",
      text,
      meta: typeof event.speaker === "string" ? event.speaker : "voice",
    };
  }
  if (type === "transcriber.ready" || type === "vlm.ready") {
    return null;
  }
  if (type === "transcriber.lifecycle") {
    return null;
  }
  if (type === "prompt") {
    return null;
  }
  if (type === "actions" && Array.isArray(event.actions)) {
    const action = [...event.actions].reverse().find(isRecord);
    return action ? actionMessageFromAction(action) : null;
  }
  if (type === "action" && isRecord(event.action)) {
    return actionMessageFromAction(event.action);
  }
  if (type === "harness.result" && isRecord(event.result)) {
    return decisionMessageFromHarnessResult(event.result);
  }
  if (type === "harness.results" && Array.isArray(event.results)) {
    const result = [...event.results].reverse().find(isRecord);
    return result ? decisionMessageFromHarnessResult(result) : null;
  }
  if (type === "vision.description") {
    const text = typeof event.text === "string" ? event.text : "";
    if (!text) return null;
    return {
      id: `event-vision-${hashText(text + String(event.timestamp ?? ""))}`,
      role: "vision",
      text,
      meta: "vision",
    };
  }
  if (type === "moondream.result") {
    const answer = typeof event.answer === "string" ? event.answer : "";
    return {
      id: `event-moondream-${hashText(JSON.stringify(event))}`,
      role: "vision",
      text: answer || "Moondream result received",
      meta: "moondream",
    };
  }
  if (type === "signal") {
    const name = typeof event.name === "string" ? event.name : "signal";
    return {
      id: `event-signal-${hashText(name + String(event.value ?? ""))}`,
      role: "vision",
      text: `${name}: ${String(event.value ?? "")}`,
      meta: "signal",
    };
  }
  if (type === "output.available" || type === "source.available") {
    return null;
  }
  return null;
}

function namesFromAgent(agent: JsonRecord | null, fields: string[]): string[] {
  if (!agent) return [];
  const names = new Set<string>();
  for (const field of fields) {
    const value = agent[field];
    if (Array.isArray(value)) {
      for (const item of value) addName(names, item);
    } else if (isRecord(value)) {
      for (const [key, item] of Object.entries(value)) {
        names.add(key);
        addName(names, item);
      }
    }
  }
  return [...names].slice(0, 6);
}

function addName(names: Set<string>, item: unknown) {
  if (typeof item === "string") names.add(item);
  if (isRecord(item)) {
    const name = item.name ?? item.id ?? item.type ?? item.provider;
    if (typeof name === "string") names.add(name);
  }
}

function agentLabel(agent: JsonRecord | null): string {
  if (!agent) return "";
  for (const key of ["name", "programName", "sessionId"]) {
    if (typeof agent[key] === "string") return String(agent[key]);
  }
  return "";
}

function summarizeState(state: JsonRecord): JsonRecord {
  const snapshot = isRecord(state.state) ? state.state : {};
  return {
    transcript: snapshot.transcript,
    decisionCount: snapshot.decisionCount,
    observationCount: snapshot.observationCount,
    latestPrompt: latestPromptFromState(state),
  };
}

function summarizeAgent(agent: JsonRecord): JsonRecord {
  return {
    sessionId: agent.sessionId,
    workflowManifest: Boolean(workflowManifestFromAgent(agent)),
    tools: namesFromAgent(agent, ["tools"]),
    outputs: namesFromAgent(agent, ["outputs"]),
    endpoints: isRecord(agent.endpoints) ? Object.keys(agent.endpoints) : [],
  };
}

function compactStatus(status: string): string {
  if (!status || status === "waiting") return "Idle";
  if (status.length <= 10) return status;
  return "Check";
}

function formatBytes(bytes: number): string {
  if (bytes < 1000) return `${bytes} B`;
  if (bytes < 1_000_000) return `${Math.round(bytes / 1000)} KB`;
  return `${(bytes / 1_000_000).toFixed(1)} MB`;
}

function readInitialMicAlwaysOn(): boolean {
  try {
    const stored = window.localStorage.getItem(MIC_ALWAYS_ON_STORAGE_KEY);
    if (stored === "false") return false;
    if (stored === "true") return true;
  } catch {
    // Fall back to the live-show default when local storage is unavailable.
  }
  return true;
}

function writeInitialMicAlwaysOn(enabled: boolean): void {
  try {
    window.localStorage.setItem(MIC_ALWAYS_ON_STORAGE_KEY, enabled ? "true" : "false");
  } catch {
    // The in-memory React state still updates if persistence is unavailable.
  }
}

function roleLabel(role: ChatMessage["role"]): string {
  if (role === "prompt") return "prompt";
  if (role === "action") return "action";
  if (role === "pass") return "pass";
  if (role === "vision") return "vision";
  if (role === "user") return "you";
  return "cue";
}

function isTextEntryTarget(target: EventTarget | null): boolean {
  if (!(target instanceof HTMLElement)) return false;
  if (target.isContentEditable) return true;
  return ["INPUT", "TEXTAREA", "SELECT"].includes(target.tagName);
}

function dedupeMessages(messages: ChatMessage[], limit: number): ChatMessage[] {
  const seen = new Set<string>();
  const result: ChatMessage[] = [];
  for (const message of messages) {
    if (seen.has(message.id)) continue;
    seen.add(message.id);
    result.push(message);
    if (result.length >= limit) break;
  }
  return result;
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
