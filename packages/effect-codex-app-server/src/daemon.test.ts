// @effect-diagnostics-next-line nodeBuiltinImport:off -- The fixture hosts a WebSocket upgrade server, not an HTTP client.
import * as NodeHttp from "node:http";
import * as FileSystem from "effect/FileSystem";
import * as Path from "effect/Path";

import * as NodeServices from "@effect/platform-node/NodeServices";
import { assert, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import { WebSocketServer } from "ws";

import * as Client from "./client.ts";
import * as Daemon from "./daemon.ts";

it.live.each(["unix", "tcp"] as const)(
  "speaks framed JSON over %s and reconnects after its peer disconnects",
  (transport) =>
    Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem;
      const path = yield* Path.Path;
      const directory = yield* fs.makeTempDirectoryScoped({ prefix: "codex-ws-" });
      const server = NodeHttp.createServer();
      const websocket = new WebSocketServer({ server });
      yield* Effect.addFinalizer(() =>
        Effect.sync(() => {
          for (const client of websocket.clients) client.terminate();
          websocket.close();
          server.close();
        }),
      );
      websocket.on("connection", (peer) =>
        peer.on("message", (raw) => {
          const request = JSON.parse(raw.toString());
          peer.send(JSON.stringify({ id: request.id, result: { echo: request.params } }));
        }),
      );
      const socketPath = path.join(directory, "daemon.sock");
      yield* Effect.callback<void, Error>((resume) => {
        server.once("error", (error) => resume(Effect.fail(error)));
        if (transport === "unix") server.listen(socketPath, () => resume(Effect.void));
        else server.listen(0, "127.0.0.1", () => resume(Effect.void));
      });
      const address = server.address();
      const endpoint =
        transport === "unix"
          ? `unix://${socketPath}`
          : `ws://127.0.0.1:${typeof address === "object" && address !== null ? address.port : 0}`;
      const readOnce = Effect.scoped(
        Effect.gen(function* () {
          const client = yield* Client.make(yield* Daemon.makeStdio(endpoint));
          return yield* client.raw.request("echo", { text: "你好\nsecond line" });
        }),
      );
      assert.deepEqual(yield* readOnce, { echo: { text: "你好\nsecond line" } });
      // A second connection to the same server still works after the first
      // client's scope closes; transport cleanup must never own the daemon.
      assert.deepEqual(yield* readOnce, { echo: { text: "你好\nsecond line" } });
      const disconnected = yield* Client.make(yield* Daemon.makeStdio(endpoint));
      yield* disconnected.raw.request("echo", { connected: true });
      const peer = [...websocket.clients].at(-1);
      if (peer === undefined || disconnected.closed === undefined)
        return yield* Effect.die("Expected a connected daemon client with termination reporting");
      peer.terminate();
      const failure = yield* disconnected.closed;
      assert.equal(failure._tag, "CodexAppServerTransportError");
      // The listening daemon remains authoritative; recovery opens a new client,
      // rather than replaying the interrupted client's native request.
      assert.deepEqual(yield* readOnce, { echo: { text: "你好\nsecond line" } });
    }).pipe(Effect.scoped, Effect.provide(NodeServices.layer)),
);
