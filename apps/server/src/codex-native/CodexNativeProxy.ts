import * as NodeOS from "node:os";
import * as NodePath from "@effect/platform-node/NodePath";
import * as NodeSocket from "@effect/platform-node/NodeSocket";
import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Path from "effect/Path";
import * as Schema from "effect/Schema";
import type * as Scope from "effect/Scope";
import * as Socket from "effect/socket/Socket";

export class CodexNativeEndpointError extends Schema.TaggedError<CodexNativeEndpointError>()(
  "CodexNativeEndpointError",
  {},
) {
  override get message() {
    return "Configure T3CODE_CODEX_NATIVE_ENDPOINT with an absolute unix:// socket path or a ws:// or wss:// endpoint.";
  }
}

type Connection = { readonly socket: Socket.Socket; readonly reader: Socket.Reader };

export class CodexNativeProxy extends Context.Service<
  CodexNativeProxy,
  {
    readonly connect: Effect.Effect<
      Connection,
      CodexNativeEndpointError | Socket.SocketError,
      Scope.Scope
    >;
    readonly bridge: (
      client: Socket.Socket,
      upstream: Connection,
    ) => Effect.Effect<void, Socket.SocketError, Scope.Scope>;
  }
>()("t3/codex-native/CodexNativeProxy") {}

export const resolveEndpoint = (
  endpoint: string | undefined,
  codexHome: string | undefined,
  pathApi: Path.Path,
) => {
  const value =
    endpoint ??
    `unix://${pathApi.join(codexHome ?? pathApi.join(NodeOS.homedir(), ".codex"), "app-server-control", "app-server-control.sock")}`;
  if (["\0", "\r", "\n"].some((character) => value.includes(character))) {
    throw new CodexNativeEndpointError({});
  }
  if (value.startsWith("unix://")) {
    const path = value.slice("unix://".length);
    if (!pathApi.isAbsolute(path) || /[:?#]/u.test(path)) {
      throw new CodexNativeEndpointError({});
    }
    return `ws+unix://${path}:/`;
  }
  let url: URL;
  try {
    url = new URL(value);
  } catch {
    throw new CodexNativeEndpointError({});
  }
  if (
    !["ws:", "wss:"].includes(url.protocol) ||
    !url.hostname ||
    url.username ||
    url.password ||
    url.hash
  ) {
    throw new CodexNativeEndpointError({});
  }
  return url.href;
};

// Preserve text/binary frame identity; tracing every pull would record an unbounded stream.
const pump = Effect.fnUntraced(function* (reader: Socket.Reader, writer: Socket.Writer) {
  while (true) {
    yield* writer.writeAll(yield* reader.pull);
  }
});

const make = Effect.gen(function* () {
  const pathApi = yield* Path.Path;
  const endpoint = process.env.T3CODE_CODEX_NATIVE_ENDPOINT;
  const codexHome = process.env.CODEX_HOME;
  const connect = Effect.gen(function* () {
    const url = yield* Effect.try({
      try: () => resolveEndpoint(endpoint, codexHome, pathApi),
      catch: () => new CodexNativeEndpointError({}),
    });
    const socket = yield* Socket.makeWebSocket(url, { openTimeout: "10 seconds" }).pipe(
      // Node's global WebSocket does not implement ws+unix.
      Effect.provide(NodeSocket.layerWebSocketConstructorWS),
    );
    const reader = yield* socket.reader;
    return { socket, reader };
  });
  const bridge = Effect.fn("CodexNativeProxy.bridge")(function* (
    client: Socket.Socket,
    upstream: Connection,
  ) {
    const reader = yield* client.reader;
    const clientWriter = yield* client.writer;
    const upstreamWriter = yield* upstream.socket.writer;
    return yield* Effect.raceFirst(
      pump(upstream.reader, clientWriter),
      pump(reader, upstreamWriter),
    );
  });
  return CodexNativeProxy.of({ connect, bridge });
});

export const layer = Layer.effect(CodexNativeProxy, make).pipe(Layer.provide(NodePath.layer));
