import { useEffect, useMemo, useRef, useState } from "react";
import { Image } from "lucide-react";

interface ShaderClawPreviewCanvasProps {
  shadersDir: string;
  shader: string;
  parameters: Record<string, unknown>;
  width: number;
  height: number;
  fps?: number;
  nonce?: number;
  className?: string;
  onError?: (error: string | null) => void;
}

function shaderClawWebSocketUrl({
  shadersDir,
  shader,
  width,
  height,
  fps,
  nonce,
}: {
  shadersDir: string;
  shader: string;
  width: number;
  height: number;
  fps: number;
  nonce: number;
}): string {
  const params = new URLSearchParams();
  if (shadersDir) params.set("shaders_dir", shadersDir);
  params.set("shader", shader);
  params.set("width", String(width));
  params.set("height", String(height));
  params.set("fps", String(fps));
  params.set("reload", String(nonce));
  const protocol = window.location.protocol === "https:" ? "wss:" : "ws:";
  return `${protocol}//${window.location.host}/api/v1/shaderclaw/ws?${params.toString()}`;
}

export function ShaderClawPreviewCanvas({
  shadersDir,
  shader,
  parameters,
  width,
  height,
  fps = 60,
  nonce = 0,
  className = "relative aspect-video overflow-hidden rounded-md bg-black",
  onError,
}: ShaderClawPreviewCanvasProps) {
  const canvasRef = useRef<HTMLCanvasElement | null>(null);
  const workerRef = useRef<Worker | null>(null);
  const workerEnabledRef = useRef(false);
  const contextRef = useRef<CanvasRenderingContext2D | null>(null);
  const imageDataRef = useRef<ImageData | null>(null);
  const frameRef = useRef<ArrayBuffer | null>(null);
  const rafRef = useRef<number | null>(null);
  const socketRef = useRef<WebSocket | null>(null);
  const parametersRef = useRef(parameters);
  const [connected, setConnected] = useState(false);
  const [localError, setLocalError] = useState<string | null>(null);
  const parametersJson = useMemo(() => JSON.stringify(parameters), [parameters]);
  const previewUrl = useMemo(
    () =>
      shaderClawWebSocketUrl({
        shadersDir,
        shader,
        width,
        height,
        fps,
        nonce,
      }),
    [fps, height, nonce, shader, shadersDir, width]
  );

  const reportError = (message: string | null) => {
    setLocalError(message);
    onError?.(message);
  };

  useEffect(() => {
    const canvas = canvasRef.current;
    if (
      !canvas ||
      typeof Worker === "undefined" ||
      typeof canvas.transferControlToOffscreen !== "function"
    ) {
      return;
    }

    const worker = new Worker(
      new URL("./shaderClawPreview.worker.ts", import.meta.url),
      { type: "module" }
    );
    workerRef.current = worker;
    workerEnabledRef.current = true;

    worker.onmessage = event => {
      const message = event.data as {
        type?: string;
        connected?: boolean;
        error?: string | null;
      };
      if (message.type === "status") {
        setConnected(Boolean(message.connected));
      } else if (message.type === "error") {
        reportError(message.error || null);
      }
    };

    const offscreen = canvas.transferControlToOffscreen();
    worker.postMessage({ type: "init", canvas: offscreen }, [offscreen]);

    return () => {
      worker.postMessage({ type: "dispose" });
      worker.terminate();
      workerRef.current = null;
      workerEnabledRef.current = false;
    };
  }, []);

  useEffect(() => {
    const worker = workerRef.current;
    if (!workerEnabledRef.current || !worker) return;
    worker.postMessage({
      type: "connect",
      url: previewUrl,
      width,
      height,
      parameters,
    });
  }, [height, parameters, parametersJson, previewUrl, width]);

  const sendParameters = (socket: WebSocket | null) => {
    if (!socket || socket.readyState !== WebSocket.OPEN) return;
    socket.send(
      JSON.stringify({
        type: "parameters",
        parameters: parametersRef.current,
      })
    );
  };

  useEffect(() => {
    parametersRef.current = parameters;
    const worker = workerRef.current;
    if (workerEnabledRef.current && worker) {
      worker.postMessage({ type: "parameters", parameters });
      return;
    }
    sendParameters(socketRef.current);
  }, [parameters, parametersJson]);

  useEffect(() => {
    if (workerEnabledRef.current) return;

    let cancelled = false;
    const socket = new WebSocket(previewUrl);
    socket.binaryType = "arraybuffer";
    socketRef.current = socket;
    setConnected(false);
    reportError(null);

    const drawLatestFrame = () => {
      rafRef.current = null;
      const frame = frameRef.current;
      const canvas = canvasRef.current;
      if (!frame || !canvas) return;

      if (canvas.width !== width || canvas.height !== height) {
        canvas.width = width;
        canvas.height = height;
        contextRef.current = null;
        imageDataRef.current = null;
      }

      const context =
        contextRef.current ||
        canvas.getContext("2d", {
          alpha: false,
          desynchronized: true,
        });
      if (!context) return;
      contextRef.current = context;

      const imageData =
        imageDataRef.current || context.createImageData(width, height);
      imageDataRef.current = imageData;
      imageData.data.set(new Uint8ClampedArray(frame));
      context.putImageData(imageData, 0, 0);
    };

    socket.onopen = () => {
      if (cancelled) return;
      setConnected(true);
      reportError(null);
      sendParameters(socket);
    };

    socket.onmessage = event => {
      if (cancelled) return;
      if (typeof event.data === "string") {
        try {
          const message = JSON.parse(event.data) as {
            type?: string;
            error?: string;
          };
          if (message.type === "error") {
            reportError(message.error || "Preview stream unavailable");
          }
        } catch {
          // Ignore non-control text frames.
        }
        return;
      }
      if (!(event.data instanceof ArrayBuffer)) return;
      frameRef.current = event.data;
      if (rafRef.current === null) {
        rafRef.current = window.requestAnimationFrame(drawLatestFrame);
      }
    };

    socket.onerror = () => {
      if (!cancelled) reportError("Preview stream unavailable");
    };

    socket.onclose = () => {
      if (cancelled) return;
      setConnected(false);
    };

    return () => {
      cancelled = true;
      if (socketRef.current === socket) socketRef.current = null;
      if (rafRef.current !== null) {
        window.cancelAnimationFrame(rafRef.current);
        rafRef.current = null;
      }
      socket.close();
    };
  }, [height, previewUrl, width]);

  return (
    <div className={className}>
      <canvas
        ref={canvasRef}
        width={width}
        height={height}
        className="h-full w-full object-cover"
      />
      {!connected && (
        <div className="pointer-events-none absolute inset-0 flex items-center justify-center text-muted-foreground">
          <Image className="h-5 w-5" />
        </div>
      )}
      {localError && (
        <div className="pointer-events-none absolute bottom-1 left-1 right-1 truncate rounded bg-black/70 px-1.5 py-0.5 text-[10px] text-amber-300">
          {localError}
        </div>
      )}
    </div>
  );
}
