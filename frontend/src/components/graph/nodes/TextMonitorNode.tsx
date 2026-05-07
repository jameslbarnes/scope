import { Handle, Position } from "@xyflow/react";
import type { NodeProps, Node } from "@xyflow/react";
import { FileText } from "lucide-react";
import type { WheelEvent } from "react";
import type { FlowNodeData } from "../../../lib/graphUtils";
import { buildHandleId } from "../../../lib/graphUtils";
import { useNodeData } from "../hooks/node/useNodeData";
import { useNodeCollapse } from "../hooks/node/useNodeCollapse";
import { PARAM_TYPE_COLORS } from "../nodeColors";
import { NodeBody, NodeCard, NodeHeader, collapsedHandleStyle } from "../ui";

type TextMonitorNodeType = Node<FlowNodeData, "text_monitor">;

export function TextMonitorNode({
  id,
  data,
  selected,
}: NodeProps<TextMonitorNodeType>) {
  const { updateData } = useNodeData(id);
  const { collapsed, toggleCollapse } = useNodeCollapse();
  const text =
    typeof data.textMonitorText === "string" ? data.textMonitorText : "";
  const hasText = text.trim().length > 0;
  const color = PARAM_TYPE_COLORS.string;

  const handleWheel = (event: WheelEvent<HTMLDivElement>) => {
    event.stopPropagation();
  };

  return (
    <NodeCard
      selected={selected}
      collapsed={collapsed}
      autoMinHeight
      minWidth={260}
      minHeight={150}
    >
      <NodeHeader
        title={data.customTitle || "Text Monitor"}
        onTitleChange={newTitle => updateData({ customTitle: newTitle })}
        collapsed={collapsed}
        onCollapseToggle={toggleCollapse}
      />

      {!collapsed && (
        <NodeBody>
          <div className="flex items-center gap-1.5 px-1 text-[9px] uppercase text-[#666]">
            <FileText className="h-3 w-3 text-[#777]" />
            <span>Live string</span>
            <span className="ml-auto tabular-nums">{text.length} chars</span>
          </div>
          <div
            className="nodrag nowheel mt-1 max-h-[220px] min-h-[104px] overflow-y-auto whitespace-pre-wrap break-words rounded-md border border-white/5 bg-black/45 p-2.5 font-mono text-[10px] leading-relaxed text-[#d6d3d1] [scrollbar-width:thin]"
            onWheel={handleWheel}
            onPointerDown={event => event.stopPropagation()}
          >
            {hasText ? (
              text
            ) : (
              <span className="text-[#555]">
                Connect a string output to display it here.
              </span>
            )}
          </div>
          <div className="mt-1.5 flex items-center justify-between px-1 text-[9px] uppercase text-[#555]">
            <span>In text</span>
            <span>Out text</span>
          </div>
        </NodeBody>
      )}

      <Handle
        type="target"
        position={Position.Left}
        id={buildHandleId("param", "text")}
        className="!h-2.5 !w-2.5 !border-0"
        style={
          collapsed
            ? collapsedHandleStyle("left")
            : { top: 82, left: 0, backgroundColor: color }
        }
      />
      <Handle
        type="source"
        position={Position.Right}
        id={buildHandleId("param", "text")}
        className="!h-2.5 !w-2.5 !border-0"
        style={
          collapsed
            ? collapsedHandleStyle("right")
            : { top: 82, right: 0, backgroundColor: color }
        }
      />
    </NodeCard>
  );
}
