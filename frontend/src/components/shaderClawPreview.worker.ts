type WorkerMessage =
  | {
      type: "init";
      canvas: OffscreenCanvas;
    }
  | {
      type: "connect";
      url: string;
      width: number;
      height: number;
      parameters: Record<string, unknown>;
    }
  | {
      type: "parameters";
      parameters: Record<string, unknown>;
    }
  | {
      type: "dispose";
    };

const workerSelf = self as unknown as {
  postMessage: (message: unknown) => void;
  onmessage: ((event: MessageEvent<WorkerMessage>) => void) | null;
  close: () => void;
};

let canvas: OffscreenCanvas | null = null;
let context: OffscreenCanvasRenderingContext2D | null = null;
let imageData: ImageData | null = null;
let gl: WebGL2RenderingContext | WebGLRenderingContext | null = null;
let glProgram: WebGLProgram | null = null;
let glTexture: WebGLTexture | null = null;
let glPositionBuffer: WebGLBuffer | null = null;
let glTexCoordBuffer: WebGLBuffer | null = null;
let socket: WebSocket | null = null;
let currentWidth = 0;
let currentHeight = 0;
let currentParameters: Record<string, unknown> = {};

function postStatus(connected: boolean) {
  workerSelf.postMessage({ type: "status", connected });
}

function postError(error: string | null) {
  workerSelf.postMessage({ type: "error", error });
}

function sendParameters() {
  if (!socket || socket.readyState !== WebSocket.OPEN) return;
  socket.send(
    JSON.stringify({
      type: "parameters",
      parameters: currentParameters,
    })
  );
}

function compileShader(
  nextGl: WebGL2RenderingContext | WebGLRenderingContext,
  type: number,
  source: string
) {
  const shader = nextGl.createShader(type);
  if (!shader) return null;
  nextGl.shaderSource(shader, source);
  nextGl.compileShader(shader);
  if (!nextGl.getShaderParameter(shader, nextGl.COMPILE_STATUS)) {
    nextGl.deleteShader(shader);
    return null;
  }
  return shader;
}

function initWebGl() {
  if (!canvas) return false;
  const nextGl =
    canvas.getContext("webgl2", {
      alpha: false,
      antialias: false,
      depth: false,
      desynchronized: true,
      preserveDrawingBuffer: false,
      stencil: false,
    }) ||
    canvas.getContext("webgl", {
      alpha: false,
      antialias: false,
      depth: false,
      desynchronized: true,
      preserveDrawingBuffer: false,
      stencil: false,
    });
  if (!nextGl) return false;

  const vertex = compileShader(
    nextGl,
    nextGl.VERTEX_SHADER,
    `
      attribute vec2 a_position;
      attribute vec2 a_texcoord;
      varying vec2 v_texcoord;
      void main() {
        gl_Position = vec4(a_position, 0.0, 1.0);
        v_texcoord = a_texcoord;
      }
    `
  );
  const fragment = compileShader(
    nextGl,
    nextGl.FRAGMENT_SHADER,
    `
      precision mediump float;
      varying vec2 v_texcoord;
      uniform sampler2D u_texture;
      void main() {
        gl_FragColor = texture2D(u_texture, v_texcoord);
      }
    `
  );
  const program = nextGl.createProgram();
  if (!vertex || !fragment || !program) return false;

  nextGl.attachShader(program, vertex);
  nextGl.attachShader(program, fragment);
  nextGl.linkProgram(program);
  nextGl.deleteShader(vertex);
  nextGl.deleteShader(fragment);
  if (!nextGl.getProgramParameter(program, nextGl.LINK_STATUS)) {
    nextGl.deleteProgram(program);
    return false;
  }

  const positionBuffer = nextGl.createBuffer();
  const texCoordBuffer = nextGl.createBuffer();
  const texture = nextGl.createTexture();
  if (!positionBuffer || !texCoordBuffer || !texture) return false;

  nextGl.bindBuffer(nextGl.ARRAY_BUFFER, positionBuffer);
  nextGl.bufferData(
    nextGl.ARRAY_BUFFER,
    new Float32Array([-1, -1, 1, -1, -1, 1, -1, 1, 1, -1, 1, 1]),
    nextGl.STATIC_DRAW
  );

  nextGl.bindBuffer(nextGl.ARRAY_BUFFER, texCoordBuffer);
  nextGl.bufferData(
    nextGl.ARRAY_BUFFER,
    new Float32Array([0, 1, 1, 1, 0, 0, 0, 0, 1, 1, 1, 0]),
    nextGl.STATIC_DRAW
  );

  nextGl.bindTexture(nextGl.TEXTURE_2D, texture);
  nextGl.texParameteri(nextGl.TEXTURE_2D, nextGl.TEXTURE_MIN_FILTER, nextGl.LINEAR);
  nextGl.texParameteri(nextGl.TEXTURE_2D, nextGl.TEXTURE_MAG_FILTER, nextGl.LINEAR);
  nextGl.texParameteri(nextGl.TEXTURE_2D, nextGl.TEXTURE_WRAP_S, nextGl.CLAMP_TO_EDGE);
  nextGl.texParameteri(nextGl.TEXTURE_2D, nextGl.TEXTURE_WRAP_T, nextGl.CLAMP_TO_EDGE);

  gl = nextGl;
  glProgram = program;
  glTexture = texture;
  glPositionBuffer = positionBuffer;
  glTexCoordBuffer = texCoordBuffer;
  return true;
}

function drawFrameWebGl(frame: ArrayBuffer) {
  if (!canvas) return false;
  if (!gl && !initWebGl()) return false;
  if (
    !gl ||
    !glProgram ||
    !glTexture ||
    !glPositionBuffer ||
    !glTexCoordBuffer
  ) {
    return false;
  }

  if (canvas.width !== currentWidth || canvas.height !== currentHeight) {
    canvas.width = currentWidth;
    canvas.height = currentHeight;
  }

  gl.viewport(0, 0, currentWidth, currentHeight);
  gl.useProgram(glProgram);
  gl.activeTexture(gl.TEXTURE0);
  gl.bindTexture(gl.TEXTURE_2D, glTexture);
  gl.texImage2D(
    gl.TEXTURE_2D,
    0,
    gl.RGBA,
    currentWidth,
    currentHeight,
    0,
    gl.RGBA,
    gl.UNSIGNED_BYTE,
    new Uint8Array(frame)
  );

  const positionLocation = gl.getAttribLocation(glProgram, "a_position");
  gl.bindBuffer(gl.ARRAY_BUFFER, glPositionBuffer);
  gl.enableVertexAttribArray(positionLocation);
  gl.vertexAttribPointer(positionLocation, 2, gl.FLOAT, false, 0, 0);

  const texCoordLocation = gl.getAttribLocation(glProgram, "a_texcoord");
  gl.bindBuffer(gl.ARRAY_BUFFER, glTexCoordBuffer);
  gl.enableVertexAttribArray(texCoordLocation);
  gl.vertexAttribPointer(texCoordLocation, 2, gl.FLOAT, false, 0, 0);

  const textureLocation = gl.getUniformLocation(glProgram, "u_texture");
  gl.uniform1i(textureLocation, 0);
  gl.drawArrays(gl.TRIANGLES, 0, 6);
  gl.flush();
  return true;
}

function ensureContext(width: number, height: number) {
  if (!canvas) return null;
  if (canvas.width !== width || canvas.height !== height) {
    canvas.width = width;
    canvas.height = height;
    context = null;
    imageData = null;
  }
  if (!context) {
    context = canvas.getContext("2d", {
      alpha: false,
      desynchronized: true,
    });
  }
  if (context && (!imageData || imageData.width !== width || imageData.height !== height)) {
    imageData = context.createImageData(width, height);
  }
  return context;
}

function drawFrame(frame: ArrayBuffer) {
  if (frame.byteLength !== currentWidth * currentHeight * 4) return;
  if (drawFrameWebGl(frame)) return;
  const nextContext = ensureContext(currentWidth, currentHeight);
  if (!nextContext || !imageData) return;
  imageData.data.set(new Uint8ClampedArray(frame));
  nextContext.putImageData(imageData, 0, 0);
}

function closeSocket() {
  if (!socket) return;
  const closing = socket;
  socket = null;
  closing.onopen = null;
  closing.onmessage = null;
  closing.onerror = null;
  closing.onclose = null;
  closing.close();
}

function connect({
  url,
  width,
  height,
  parameters,
}: {
  url: string;
  width: number;
  height: number;
  parameters: Record<string, unknown>;
}) {
  closeSocket();
  currentWidth = width;
  currentHeight = height;
  currentParameters = parameters;
  postStatus(false);
  postError(null);
  ensureContext(width, height);

  const nextSocket = new WebSocket(url);
  nextSocket.binaryType = "arraybuffer";
  socket = nextSocket;

  nextSocket.onopen = () => {
    if (socket !== nextSocket) return;
    postStatus(true);
    postError(null);
    sendParameters();
  };

  nextSocket.onmessage = event => {
    if (socket !== nextSocket) return;
    if (typeof event.data === "string") {
      try {
        const message = JSON.parse(event.data) as {
          type?: string;
          error?: string;
        };
        if (message.type === "error") {
          postError(message.error || "Preview stream unavailable");
        }
      } catch {
        // Ignore non-control text frames.
      }
      return;
    }
    if (event.data instanceof ArrayBuffer) drawFrame(event.data);
  };

  nextSocket.onerror = () => {
    if (socket === nextSocket) postError("Preview stream unavailable");
  };

  nextSocket.onclose = () => {
    if (socket === nextSocket) postStatus(false);
  };
}

workerSelf.onmessage = event => {
  const message = event.data;
  if (message.type === "init") {
    canvas = message.canvas;
    return;
  }
  if (message.type === "connect") {
    connect(message);
    return;
  }
  if (message.type === "parameters") {
    currentParameters = message.parameters;
    sendParameters();
    return;
  }
  if (message.type === "dispose") {
    closeSocket();
    workerSelf.close();
  }
};

export {};
