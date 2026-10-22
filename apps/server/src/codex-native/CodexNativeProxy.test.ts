import { NodeWS } from "@effect/platform-node/NodeSocket";
import { expect, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Path from "effect/Path";
import { HttpRouter, HttpServer } from "effect/http";
import * as Context from "effect/Context";
import * as NodeHttpServer from "@effect/platform-node/NodeHttpServer";
import { AuthOrchestrationOperateScope, AuthSessionId } from "@t3tools/contracts";
import { vi } from "vite-plus/test";

import * as EnvironmentAuth from "../auth/EnvironmentAuth.ts";
import * as CodexNativeProxy from "./CodexNativeProxy.ts";
import * as NativeHttp from "./http.ts";

it.effect(
  "resolves the daemon socket without accepting other transports or embedded credentials",
  () =>
    Effect.gen(function* () {
      const pathApi = yield* Path.Path.pipe(Effect.provide(Path.layer));
      expect(CodexNativeProxy.resolveEndpoint(undefined, "/tmp/codex-home", pathApi)).toBe(
        "ws+unix:///tmp/codex-home/app-server-control/app-server-control.sock:/",
      );
      expect(CodexNativeProxy.resolveEndpoint("wss://localhost:4500/", undefined, pathApi)).toBe(
        "wss://localhost:4500/",
      );
      for (const endpoint of [
        "unix://relative",
        "http://localhost",
        "ws://user:secret@localhost",
        "unix:///tmp/socket?x",
        "",
      ]) {
        expect(() => CodexNativeProxy.resolveEndpoint(endpoint, undefined, pathApi)).toThrow(
          CodexNativeProxy.CodexNativeEndpointError,
        );
      }
    }),
);

it.live(
  "forwards text and binary frames and releases the daemon connection when the client closes",
  () =>
    Effect.gen(function* () {
      const hub = yield* Effect.acquireRelease(
        Effect.sync(() => new NodeWS.WebSocketServer({ port: 0 })),
        (server) =>
          Effect.promise(
            () =>
              new Promise<void>((resolve) => {
                for (const socket of server.clients) socket.terminate();
                server.close(() => resolve());
              }),
          ),
      );
      if (!hub.address())
        yield* Effect.promise(() => new Promise<void>((resolve) => hub.once("listening", resolve)));
      const address = hub.address();
      if (typeof address !== "object" || !address) throw new Error("Missing upstream port");
      const disconnected = Promise.withResolvers<void>();
      const upstreamFrames: { text: string; binary: boolean }[] = [];
      hub.on("connection", (socket) => {
        socket.on("message", (data, binary) => {
          upstreamFrames.push({ text: data.toString(), binary });
          socket.send(data, { binary });
        });
        socket.on("close", () => disconnected.resolve());
      });
      vi.stubEnv("T3CODE_CODEX_NATIVE_ENDPOINT", `ws://127.0.0.1:${address.port}`);
      yield* Effect.addFinalizer(() => Effect.sync(() => vi.unstubAllEnvs()));
      const services = yield* Layer.build(
        HttpRouter.serve(NativeHttp.layer, { disableListenLog: true }).pipe(
          Layer.provideMerge(NodeHttpServer.layerTest),
          Layer.provide(CodexNativeProxy.layer),
          Layer.provide(
            Layer.succeed(EnvironmentAuth.EnvironmentAuth, {
              authenticateWebSocketUpgrade: () =>
                Effect.succeed({
                  sessionId: AuthSessionId.make("test"),
                  subject: "test",
                  method: "bearer-access-token",
                  scopes: [AuthOrchestrationOperateScope],
                }),
            } as unknown as EnvironmentAuth.EnvironmentAuth["Service"]),
          ),
        ),
      );
      const origin = HttpServer.formatAddress(
        Context.get(services, HttpServer.HttpServer).address,
      ).replace(/^http/, "ws");
      const client = yield* Effect.acquireRelease(
        Effect.sync(
          () => new NodeWS.WebSocket(`${origin}/api/codex-native/ws?endpoint=ws://ignored.invalid`),
        ),
        (socket) => Effect.sync(() => socket.close()),
      );
      const done = Promise.withResolvers<void>();
      const frames: { text: string; binary: boolean }[] = [];
      client.once("open", () => {
        client.send('{"id":1,"method":"initialize"}');
        client.send(Uint8Array.of(1, 2, 3));
      });
      client.once("error", done.reject);
      client.on("message", (data, binary) => {
        frames.push({ text: data.toString(), binary });
        if (frames.length === 2) {
          client.close();
          done.resolve();
        }
      });
      yield* Effect.promise(() => done.promise);
      yield* Effect.promise(() => disconnected.promise);
      expect(frames).toEqual(upstreamFrames);
      expect(frames.map((frame) => frame.binary)).toEqual([false, true]);
      expect(frames[0]?.text).toBe('{"id":1,"method":"initialize"}');
    }),
);
