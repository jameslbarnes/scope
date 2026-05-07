import {
  Handle,
  Position,
  useEdges,
  useNodes,
  useUpdateNodeInternals,
} from "@xyflow/react";
import type { NodeProps, Node } from "@xyflow/react";
import { useCallback, useEffect, useMemo, useRef } from "react";
import type { FlowNodeData } from "../../../lib/graphUtils";
import {
  customNodeInputHandleId,
  customNodeOutputHandleId,
  parseHandleId,
  stripCustomNodeDirection,
} from "../../../lib/graphUtils";
import type { NodeParamDef, NodePortDef } from "../../../lib/api";
import { useNodeData } from "../hooks/node/useNodeData";
import { useNodeCollapse } from "../hooks/node/useNodeCollapse";
import { useHandlePositions } from "../hooks/node/useHandlePositions";
import { getAnyValueFromNode } from "../utils/getValueFromNode";
import {
  NodeCard,
  NodeHeader,
  NodeBody,
  NodeParamRow,
  NodePill,
  NodePillInput,
  NodePillSelect,
  NodePillToggle,
  collapsedHandleStyle,
} from "../ui";
import { CueSessionWidget } from "./CueSessionWidget";

const PARAM_PUSH_DEBOUNCE_MS = 100;

type CustomNodeType = Node<FlowNodeData, "custom_node">;

const PORT_COLORS: Record<string, string> = {
  audio: "#22c55e",
  video: "#eeeeee",
  number: "#38bdf8",
  string: "#fbbf24",
  boolean: "#34d399",
  trigger: "#f97316",
  latent: "#a855f7",
  model: "#f59e0b",
  vae: "#f59e0b",
  clip: "#f59e0b",
  conditioning: "#3b82f6",
  semantic_hints: "#06b6d4",
  config: "#6b7280",
  curve: "#ec4899",
  mask: "#ef4444",
  lora: "#f472b6",
};

const CUE_SESSION_INPUTS: NodePortDef[] = [
  {
    name: "refresh",
    port_type: "trigger",
    description: "Force an immediate Cue state poll",
  },
  {
    name: "transcript",
    port_type: "string",
    description: "Forward upstream speech or chat text into Cue",
  },
  {
    name: "vision",
    port_type: "string",
    description: "Forward visual descriptions or VLM output into Cue",
  },
  {
    name: "signal",
    port_type: "string",
    description: "Forward signal JSON into Cue",
  },
  {
    name: "context",
    port_type: "string",
    description: "Forward upstream structured context into Cue",
  },
  {
    name: "control",
    port_type: "string",
    description: "Forward control observations into Cue",
  },
];

const CUE_SESSION_OUTPUTS: NodePortDef[] = [
  {
    name: "prompt",
    port_type: "string",
    description: "Latest matching Cue prompt action payload",
  },
  {
    name: "reset",
    port_type: "boolean",
    description: "Latest matching Cue prompt reset flag",
  },
  {
    name: "action",
    port_type: "string",
    description: "Latest Cue action serialized as JSON",
  },
  {
    name: "param_patch",
    port_type: "string",
    description: "Latest parameter patch action payload serialized as JSON",
  },
  {
    name: "shader_patch",
    port_type: "string",
    description: "Latest shader patch action payload serialized as JSON",
  },
  {
    name: "transcript",
    port_type: "string",
    description: "Current Cue transcript snapshot",
  },
  {
    name: "source",
    port_type: "string",
    description: "Latest Cue output/source availability serialized as JSON",
  },
  {
    name: "status",
    port_type: "string",
    description: "Cue polling status",
  },
  {
    name: "tick",
    port_type: "trigger",
    description: "Increments when observed Cue state changes",
  },
  {
    name: "decision",
    port_type: "string",
    description: "Latest Cue decision summary serialized as JSON",
  },
];

function portColor(portType: string): string {
  return PORT_COLORS[portType] ?? "#9ca3af";
}

function cueSessionPortLabel(name: string): string {
  if (name === "reset") return "reset -> reset_cache";
  if (name === "param_patch") return "params";
  if (name === "shader_patch") return "shader";
  return name;
}

interface ParamWidgetProps {
  param: NodeParamDef;
  value: unknown;
  connected: boolean;
  onChange: (value: unknown) => void;
}

function ParamWidget({ param, value, connected, onChange }: ParamWidgetProps) {
  if (connected) {
    return (
      <NodePill className="opacity-50" title={String(value)}>
        {String(value) || "—"}
      </NodePill>
    );
  }
  if (param.param_type === "select" && Array.isArray(param.ui?.options)) {
    return (
      <NodePillSelect
        value={String(value)}
        onChange={onChange}
        options={(param.ui.options as string[]).map(o => ({
          value: o,
          label: o,
        }))}
      />
    );
  }
  if (param.param_type === "boolean") {
    return <NodePillToggle checked={Boolean(value)} onChange={onChange} />;
  }
  if (param.param_type === "number") {
    return (
      <NodePillInput
        type="number"
        value={Number(value)}
        min={param.ui?.min as number | undefined}
        max={param.ui?.max as number | undefined}
        step={param.ui?.step as number | undefined}
        onChange={v => onChange(Number(v))}
      />
    );
  }
  return (
    <NodePillInput type="text" value={String(value)} onChange={onChange} />
  );
}

export function CustomNode({ id, data, selected }: NodeProps<CustomNodeType>) {
  const { updateData } = useNodeData(id);
  const { collapsed, toggleCollapse } = useNodeCollapse();
  const updateNodeInternals = useUpdateNodeInternals();
  const edges = useEdges();
  const allNodes = useNodes() as Node<FlowNodeData>[];

  const isCueSession = data.customNodeTypeId === "cue.session";
  const inputs =
    isCueSession && (data.customNodeInputs?.length ?? 0) === 0
      ? CUE_SESSION_INPUTS
      : (data.customNodeInputs ?? []);
  const outputs =
    isCueSession && (data.customNodeOutputs?.length ?? 0) === 0
      ? CUE_SESSION_OUTPUTS
      : (data.customNodeOutputs ?? []);
  const params = data.customNodeParamDefs ?? [];
  const visibleParams = isCueSession
    ? params.filter(p => p.ui?.widget !== "cue_hidden")
    : params;

  // ComfyUI-style widget→input linkage: an input port whose name matches
  // a param renders on the same row as the param, and the wire's
  // upstream value overrides the widget's stored value.
  const paramNames = new Set(params.map(p => p.name));
  const linkedInputs = inputs.filter(p => paramNames.has(p.name));
  const unlinkedInputs = inputs.filter(p => !paramNames.has(p.name));

  const upstreamByPort = useMemo(() => {
    const map = new Map<string, unknown>();
    for (const e of edges) {
      if (e.target !== id) continue;
      const parsed = parseHandleId(e.targetHandle);
      if (!parsed || parsed.kind !== "stream") continue;
      const portName = stripCustomNodeDirection(parsed.name);
      const sourceNode = allNodes.find(n => n.id === e.source);
      const value = sourceNode
        ? getAnyValueFromNode(sourceNode, e.sourceHandle)
        : undefined;
      map.set(portName, value);
    }
    return map;
  }, [edges, allNodes, id]);

  // Per-param debounce so slider drags don't flood the data channel
  // between local widget edits and backend update_parameters calls.
  const onParamChangeRef = useRef(data.onCustomNodeParamChange);
  onParamChangeRef.current = data.onCustomNodeParamChange;
  const pushTimersRef = useRef<Record<string, ReturnType<typeof setTimeout>>>(
    {}
  );
  useEffect(() => {
    const timers = pushTimersRef.current;
    return () => {
      for (const t of Object.values(timers)) clearTimeout(t);
    };
  }, []);
  const setParam = (name: string, value: unknown) => {
    updateData({
      customNodeParams: { ...data.customNodeParams, [name]: value },
    });
    const existing = pushTimersRef.current[name];
    if (existing) clearTimeout(existing);
    pushTimersRef.current[name] = setTimeout(() => {
      delete pushTimersRef.current[name];
      onParamChangeRef.current?.(name, value);
    }, PARAM_PUSH_DEBOUNCE_MS);
  };
  const setOutputValues = useCallback(
    (values: Record<string, unknown>) => {
      updateData({ customNodeOutputValues: values });
    },
    [updateData]
  );
  const displayName =
    data.customTitle ||
    data.customNodeDisplayName ||
    data.customNodeTypeId ||
    "Custom Node";

  const { setRowRef, rowPositions } = useHandlePositions([
    collapsed,
    isCueSession ? "cue-ports-bottom" : "standard-ports",
    unlinkedInputs.map(p => p.name).join("|"),
    outputs.map(p => p.name).join("|"),
    visibleParams.map(p => p.name).join("|"),
  ]);
  useEffect(() => {
    requestAnimationFrame(() => updateNodeInternals(id));
  }, [id, rowPositions, updateNodeInternals]);

  const cuePortRows = isCueSession ? (
    <div className="grid grid-cols-2 gap-2 rounded-md border border-white/5 bg-[#101010] p-1.5 text-[9px] text-[#9ca3af]">
      <div className="min-w-0">
        <div className="mb-1 px-1 uppercase text-[#737373]">In</div>
        {unlinkedInputs.map(p => (
          <div
            key={`in-${p.name}`}
            ref={setRowRef(`in:${p.name}`)}
            className="flex h-5 min-w-0 items-center rounded px-1"
          >
            <span className="truncate" title={p.description || p.name}>
              {cueSessionPortLabel(p.name)}
            </span>
          </div>
        ))}
      </div>
      <div className="min-w-0 text-right">
        <div className="mb-1 px-1 uppercase text-[#737373]">Out</div>
        {outputs.map(p => (
          <div
            key={`out-${p.name}`}
            ref={setRowRef(`out:${p.name}`)}
            className="flex h-5 min-w-0 items-center justify-end rounded px-1"
          >
            <span className="truncate" title={p.description || p.name}>
              {cueSessionPortLabel(p.name)}
            </span>
          </div>
        ))}
      </div>
    </div>
  ) : null;

  return (
    <NodeCard
      selected={selected}
      collapsed={collapsed}
      autoMinHeight={!collapsed}
      minWidth={isCueSession ? 380 : 240}
    >
      <NodeHeader
        title={displayName}
        onTitleChange={t => updateData({ customTitle: t })}
        collapsed={collapsed}
        onCollapseToggle={toggleCollapse}
      />
      {!collapsed && (
        <NodeBody withGap>
          {!isCueSession && (
            <>
              {unlinkedInputs.map(p => (
                <div key={`in-${p.name}`} ref={setRowRef(`in:${p.name}`)}>
                  <NodeParamRow label={p.name}>
                    <NodePill>{p.name}</NodePill>
                  </NodeParamRow>
                </div>
              ))}

              {outputs.map(p => (
                <div key={`out-${p.name}`} ref={setRowRef(`out:${p.name}`)}>
                  <NodeParamRow label={p.name}>
                    <NodePill>{p.name}</NodePill>
                  </NodeParamRow>
                </div>
              ))}
            </>
          )}

          {isCueSession && (
            <CueSessionWidget
              data={data}
              params={params}
              setParam={setParam}
              setOutputValues={setOutputValues}
            />
          )}

          {visibleParams.map(p => {
            const connected = upstreamByPort.has(p.name);
            const val = connected
              ? (upstreamByPort.get(p.name) ?? "")
              : (data.customNodeParams?.[p.name] ?? p.default ?? "");
            return (
              <div key={`param-${p.name}`} ref={setRowRef(`param:${p.name}`)}>
                <NodeParamRow label={p.description || p.name}>
                  <ParamWidget
                    param={p}
                    value={val}
                    connected={connected}
                    onChange={v => setParam(p.name, v)}
                  />
                </NodeParamRow>
              </div>
            );
          })}

          {cuePortRows}
        </NodeBody>
      )}

      {unlinkedInputs.map(p => (
        <Handle
          key={`in-${p.name}`}
          type="target"
          position={Position.Left}
          id={customNodeInputHandleId(p.name)}
          className="!w-2.5 !h-2.5 !border-0"
          style={
            collapsed
              ? collapsedHandleStyle("left")
              : {
                  backgroundColor: portColor(p.port_type),
                  top: rowPositions[`in:${p.name}`] ?? 0,
                  left: 0,
                }
          }
        />
      ))}

      {/* Linked-input handles ride on the param row, not on a row of
          their own — that's where the widget they override is rendered. */}
      {linkedInputs.map(p => (
        <Handle
          key={`in-linked-${p.name}`}
          type="target"
          position={Position.Left}
          id={customNodeInputHandleId(p.name)}
          className="!w-2.5 !h-2.5 !border-0"
          style={
            collapsed
              ? collapsedHandleStyle("left")
              : {
                  backgroundColor: portColor(p.port_type),
                  top: rowPositions[`param:${p.name}`] ?? 0,
                  left: 0,
                }
          }
        />
      ))}

      {outputs.map(p => (
        <Handle
          key={`out-${p.name}`}
          type="source"
          position={Position.Right}
          id={customNodeOutputHandleId(p.name)}
          className="!w-2.5 !h-2.5 !border-0"
          style={
            collapsed
              ? collapsedHandleStyle("right")
              : {
                  backgroundColor: portColor(p.port_type),
                  top: rowPositions[`out:${p.name}`] ?? 0,
                  right: 0,
                }
          }
        />
      ))}
    </NodeCard>
  );
}
