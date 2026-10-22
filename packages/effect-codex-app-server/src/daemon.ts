import * as NodeSocket from "@effect/platform-node/NodeSocket";
import * as Effect from "effect/Effect";
import * as PlatformError from "effect/PlatformError";
import * as Sink from "effect/Sink";
import * as Stdio from "effect/Stdio";
import * as Stream from "effect/Stream";
import * as Socket from "effect/socket/Socket";

import * as CodexError from "./errors.ts";

const encoder = new TextEncoder();

const websocketUrl = (endpoint: string): string => {
  if (/[\0\r\n]/u.test(endpoint)) throw new Error("Invalid Codex daemon endpoint.");
  if (endpoint.startsWith("unix://")) {
    const socketPath = endpoint.slice("unix://".length);
    if (!socketPath.startsWith("/") || /[:?#]/u.test(socketPath)) {
      throw new Error("Codex daemon Unix socket path must be absolute.");
    }
    return `ws+unix://${socketPath}:/`;
  }
  const url = new URL(endpoint);
  if (!["ws:", "wss:"].includes(url.protocol) || url.username || url.password || url.hash) {
    throw new Error("Codex daemon endpoint must be unix://, ws:// or wss://.");
  }
  return url.href;
};

const platformError = (cause: unknown) =>
  PlatformError.systemError({
    _tag: "Unknown",
    module: "CodexDaemon",
    method: "transport",
    cause,
  });

/** Adapt one JSON message per WebSocket frame to the client's existing JSONL protocol. */
export const makeStdio = Effect.fn("CodexDaemon.makeStdio")(function* (endpoint: string) {
  const url = yield* Effect.try({
    try: () => websocketUrl(endpoint),
    catch: (cause) =>
      new CodexError.CodexAppServerTransportError({ operation: "read-input-stream", cause }),
  });
  const socket = yield* Socket.makeWebSocket(url, { openTimeout: "10 seconds" }).pipe(
    Effect.provide(NodeSocket.layerWebSocketConstructorWS),
    Effect.mapError(
      (cause) =>
        new CodexError.CodexAppServerTransportError({ operation: "read-input-stream", cause }),
    ),
  );
  const reader = yield* socket.reader.pipe(
    Effect.mapError(
      (cause) =>
        new CodexError.CodexAppServerTransportError({ operation: "read-input-stream", cause }),
    ),
  );
  const writer = yield* socket.writer;
  return Stdio.make({
    args: Effect.succeed([]),
    stdin: Stream.fromPull(Effect.succeed(reader.pull)).pipe(
      Stream.map((frame) => {
        const message = typeof frame === "string" ? frame : new TextDecoder().decode(frame);
        return encoder.encode(`${message}\n`);
      }),
      Stream.mapError(platformError),
    ),
    stdout: () =>
      Sink.forEach((chunk: string | Uint8Array) =>
        writer
          .write(
            typeof chunk === "string" ? chunk.trimEnd() : new TextDecoder().decode(chunk).trimEnd(),
          )
          .pipe(Effect.mapError(platformError)),
      ),
    stderr: () => Sink.drain,
  });
});
