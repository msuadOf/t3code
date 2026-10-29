// @effect-diagnostics nodeBuiltinImport:off globalFetch:off globalTimers:off - Exercises owned child processes and actual loopback SSE transport.
import * as NodeFSP from "node:fs/promises";
import * as NodePath from "node:path";
import * as NodeOS from "node:os";
import * as NodeHttp from "node:http";
import * as NodeURL from "node:url";
import { expect } from "vite-plus/test";
import { it } from "@effect/vitest";
import * as NodeServices from "@effect/platform-node/NodeServices";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as PubSub from "effect/PubSub";
import * as Stream from "effect/Stream";
import {
  ProviderInstanceId,
  ProviderThreadId,
  ThreadId,
  type ModelActivityFrame,
  type OrchestrationV2AppThread,
  type OrchestrationV2ProviderThread,
} from "@t3tools/contracts";
import type { ProviderInstance } from "@t3tools/provider-core/server/driver";
import * as HostProcess from "@t3tools/shared/HostProcess";
import * as EventSink from "../orchestration-v2/EventSink.ts";
import * as ProjectionStore from "../orchestration-v2/ProjectionStore.ts";
import * as ServerConfig from "../config.ts";
import * as ServerSettings from "../serverSettings.ts";
import * as ProviderInstanceRegistry from "./ProviderInstanceRegistry.ts";
import * as NativeModelActivityService from "./NativeModelActivityService.ts";
import { ensureClaudeModelObserver } from "./ClaudeModelObserver.ts";
import {
  prepareClaudeModelObserverSettings,
  writeClaudeModelObserverBinding,
} from "./ClaudeModelObserverBindings.ts";
import {
  emptyModelActivityState,
  reduceModelActivity,
  type ModelActivityState,
} from "../../../../packages/client-runtime/src/state/modelActivity.ts";

const deferred = () => {
  let resolve!: () => void;
  const promise = new Promise<void>((ready) => {
    resolve = ready;
  });
  return { promise, resolve };
};
const alive = (pid: number) => {
  try {
    process.kill(pid, 0);
    return true;
  } catch (error) {
    return (error as NodeJS.ErrnoException).code === "EPERM";
  }
};
const waitStopped = async (pid: number) => {
  await expect.poll(() => alive(pid)).toBe(false);
};

it.live(
  "reconnects the real service to a restarted observer without metadata events and preserves disconnected UI activity",
  () =>
    Effect.promise(async () => {
      const stateDir = await NodeFSP.mkdtemp(
        NodePath.join(NodeOS.tmpdir(), "t3-observer-service-restart-"),
      );
      const configDirectory = NodePath.join(stateDir, "native");
      const cwd = NodePath.join(stateDir, "project");
      const nativeSessionId = "01234567-89ab-cdef-0123-456789abcdef";
      const instanceId = ProviderInstanceId.make("claude-observer-restart-e2e");
      const threadId = ThreadId.make("observer-restart-thread");
      const providerThreadId = ProviderThreadId.make("observer-restart-provider-thread");
      const capturedPids: number[] = [];
      const pendingResponses = new Set<NodeHttp.ServerResponse>();
      const server = NodeHttp.createServer(async (request, response) => {
        let raw = "";
        for await (const chunk of request) raw += String(chunk);
        const tag = JSON.parse(raw).messages[0].content as string;
        pendingResponses.add(response);
        response.once("close", () => pendingResponses.delete(response));
        response.writeHead(200, { "content-type": "text/event-stream" });
        const emit = (type: string, data: object = {}) =>
          response.write(`event: ${type}\ndata: ${JSON.stringify({ type, ...data })}\n\n`);
        emit("message_start", { message: { id: `msg_${tag}` } });
        emit("content_block_start", {
          index: 0,
          content_block: { type: "thinking", thinking: "" },
        });
        emit("content_block_delta", {
          index: 0,
          delta: { type: "thinking_delta", thinking: `${tag}: thinking 字` },
        });
        emit("content_block_stop", { index: 0 });
        emit("content_block_start", { index: 1, content_block: { type: "text", text: "" } });
        emit("content_block_delta", {
          index: 1,
          delta: { type: "text_delta", text: `${tag}: body 文` },
        });
        if (tag === "after") {
          emit("content_block_stop", { index: 1 });
          emit("message_stop");
          response.end();
        }
      });
      await new Promise<void>((resolve) => server.listen(0, "127.0.0.1", resolve));
      const address = server.address();
      if (!address || typeof address === "string") throw new Error("No fixture upstream");
      const upstreamUrl = `http://127.0.0.1:${address.port}/`;
      const bin = NodeURL.fileURLToPath(new URL("../bin.ts", import.meta.url));
      const withBinEntry = async <A>(body: () => Promise<A>): Promise<A> => {
        const previous = process.argv[1];
        process.argv[1] = bin;
        try {
          return await body();
        } finally {
          if (previous === undefined) process.argv.splice(1, 1);
          else process.argv[1] = previous;
        }
      };
      const capturePid = async (observerStateDir: string) => {
        const directory = NodePath.join(observerStateDir, "claude-model-observers");
        const [upstreamHash] = await NodeFSP.readdir(directory);
        const registration = JSON.parse(
          await NodeFSP.readFile(
            NodePath.join(directory, upstreamHash!, "registration.json"),
            "utf8",
          ),
        ) as { pid: number };
        capturedPids.push(registration.pid);
        return registration.pid;
      };
      let state: ModelActivityState = emptyModelActivityState;
      const frames: ModelActivityFrame[] = [];
      const connected = deferred();
      const partial = deferred();
      const disconnected = deferred();
      const reconnected = deferred();
      const completed = deferred();
      let firstEpoch = "";
      let disconnectedState: ModelActivityState | undefined;
      let proof: unknown;
      const cleanup = async () => {
        for (const pid of capturedPids) if (alive(pid)) process.kill(pid, "SIGTERM");
        for (const pid of capturedPids) await waitStopped(pid);
        for (const response of pendingResponses) response.destroy();
        server.closeAllConnections();
        await new Promise<void>((resolve) => server.close(() => resolve()));
        await NodeFSP.rm(stateDir, { recursive: true, force: true });
      };
      try {
        await NodeFSP.mkdir(cwd);
        const prepared = await withBinEntry(() =>
          prepareClaudeModelObserverSettings({ stateDir, instanceId, upstreamUrl }),
        );
        const firstPid = await capturePid(prepared.observerStateDir);
        await writeClaudeModelObserverBinding({
          ...prepared,
          stateDir,
          instanceId,
          nativeSessionId,
          configDirectory,
        });
        const jobPath = NodePath.join(
          configDirectory,
          "jobs",
          nativeSessionId.slice(0, 8),
          "state.json",
        );
        await NodeFSP.mkdir(NodePath.dirname(jobPath), { recursive: true });
        await NodeFSP.writeFile(
          jobPath,
          JSON.stringify({
            sessionId: nativeSessionId,
            state: "working",
            respawnFlags: ["--settings", prepared.settingsPath],
          }),
        );
        const firstObserver = await withBinEntry(() =>
          ensureClaudeModelObserver({ stateDir: prepared.observerStateDir, upstreamUrl }),
        );
        const thread = {
          id: threadId,
          providerInstanceId: instanceId,
          activeProviderThreadId: providerThreadId,
        } as OrchestrationV2AppThread;
        const native = {
          id: providerThreadId,
          providerInstanceId: instanceId,
          driver: "claudeAgent",
          nativeThreadRef: { nativeId: nativeSessionId },
        } as OrchestrationV2ProviderThread;
        const instance = {
          instanceId,
          driverKind: "claudeAgent",
          enabled: true,
          nativeSessions: {
            identityDirectory: configDirectory,
            list: Effect.succeed([
              {
                id: nativeSessionId,
                cwd,
                title: "Native fixture",
                createdAt: 0,
                updatedAt: 0,
                archived: false,
                active: true,
              },
            ]),
            confirmedModelMessageIds: () => Effect.succeed([]),
          },
        } as unknown as ProviderInstance;
        const layer = NativeModelActivityService.layer.pipe(
          Layer.provide(
            Layer.mergeAll(
              NodeServices.layer,
              Layer.mock(EventSink.EventSinkV2)({
                latestSequence: () => Effect.succeed(0),
                stream: () => Stream.never,
              }),
              Layer.succeed(HostProcess.HomeDirectory, stateDir),
              Layer.mock(ServerConfig.ServerConfig)({
                stateDir,
              } as ServerConfig.ServerConfig["Service"]),
              ServerSettings.layerTest({
                providerInstances: {
                  [instanceId]: { driver: "claudeAgent", config: { sharedSessionRoot: cwd } },
                },
              }),
              Layer.mock(ProjectionStore.ProjectionStoreV2)({
                getThread: () => Effect.succeed(thread),
                getThreadProviderContext: () =>
                  Effect.succeed({ thread, providerThreads: [native], providerSessions: [] }),
              }),
              Layer.mock(ProviderInstanceRegistry.ProviderInstanceRegistry)({
                getInstance: () => Effect.succeed(instance),
                subscribeChanges: PubSub.unbounded<void>().pipe(Effect.flatMap(PubSub.subscribe)),
              }),
            ),
          ),
        );
        const modelRequest = (baseUrl: string, tag: string) =>
          fetch(`${baseUrl}/v1/messages`, {
            method: "POST",
            body: JSON.stringify({
              metadata: { user_id: JSON.stringify({ session_id: nativeSessionId }) },
              messages: [{ role: "user", content: tag }],
            }),
          });
        const program = Effect.gen(function* () {
          const service = yield* NativeModelActivityService.NativeModelActivityService;
          yield* service.subscribe({ threadId }).pipe(
            Stream.runForEach((frame) =>
              Effect.sync(() => {
                frames.push(frame);
                state = reduceModelActivity(state, frame);
                if (frame.kind === "snapshot" && frame.available) {
                  if (!firstEpoch) {
                    firstEpoch = frame.epoch;
                    connected.resolve();
                  } else if (frame.epoch !== firstEpoch) reconnected.resolve();
                }
                if (
                  state.requests.some(
                    (item) =>
                      item.messageId === "msg_before" &&
                      item.blocks.some(
                        (block) => block.kind === "thinking" && block.text.includes("字"),
                      ) &&
                      item.blocks.some(
                        (block) => block.kind === "text" && block.text.includes("文"),
                      ),
                  )
                )
                  partial.resolve();
                if (frame.kind === "disconnected") {
                  disconnectedState = state;
                  disconnected.resolve();
                }
                if (
                  state.requests.some(
                    (item) =>
                      item.messageId === "msg_after" &&
                      item.status === "completed" &&
                      item.blocks.some((block) => block.text.includes("after: body")),
                  )
                )
                  completed.resolve();
              }),
            ),
            Effect.forkScoped,
          );
          yield* Effect.promise(() => connected.promise);
          const firstResponse = yield* Effect.promise(() =>
            modelRequest(firstObserver.modelBaseUrl, "before"),
          );
          const interruptedBody = firstResponse.text().then(
            () => "ended",
            () => "interrupted",
          );
          yield* Effect.promise(() => partial.promise);
          const partialState = state;
          process.kill(firstPid, "SIGKILL");
          yield* Effect.promise(() => disconnected.promise);
          expect(disconnectedState?.connectionState).toBe("disconnected");
          expect(disconnectedState?.requests).toEqual(partialState.requests);
          yield* Effect.promise(() => waitStopped(firstPid));
          expect(yield* Effect.promise(() => interruptedBody)).toBe("interrupted");
          // Recovery is explicitly requested by this experiment; read subscriptions never spawn an observer.
          const restarted = yield* Effect.promise(() =>
            withBinEntry(() =>
              ensureClaudeModelObserver({ stateDir: prepared.observerStateDir, upstreamUrl }),
            ),
          );
          const secondPid = yield* Effect.promise(() => capturePid(prepared.observerStateDir));
          expect(secondPid).not.toBe(firstPid);
          expect(restarted.url).toBe(firstObserver.url);
          expect(restarted.token).toBe(firstObserver.token);
          expect(restarted.epoch).not.toBe(firstObserver.epoch);
          yield* Effect.promise(() => reconnected.promise);
          expect(state.connectionState).toBe("connected");
          expect(state.gap).toBe(true);
          expect(state.requests).toHaveLength(0);
          const response = yield* Effect.promise(() =>
            modelRequest(restarted.modelBaseUrl, "after"),
          );
          yield* Effect.promise(() => response.text());
          yield* Effect.promise(() => completed.promise);
          expect(state.epoch).not.toBe(firstEpoch);
          expect(
            state.requests
              .find((item) => item.messageId === "msg_after")
              ?.blocks.map((block) => block.kind),
          ).toEqual(["thinking", "text"]);
          proof = {
            realObserverProcesses: true,
            transportMocked: false,
            threadAndSettingsEvents: 0,
            firstPid,
            secondPid,
            firstEpoch,
            secondEpoch: state.epoch,
            samePortAndToken: true,
            disconnectedState,
            finalState: state,
            frameKinds: frames.map((frame) => frame.kind),
            recoveryTriggeredByExplicitEnsure: true,
          };
        }).pipe(Effect.scoped, Effect.provide(layer));
        return program.pipe(
          Effect.ensuring(Effect.promise(cleanup)),
          Effect.andThen(
            Effect.promise(async () => {
              if (process.env.T3_MODEL_ACTIVITY_E2E_PROOF)
                await NodeFSP.writeFile(
                  process.env.T3_MODEL_ACTIVITY_E2E_PROOF,
                  JSON.stringify(
                    {
                      ...(proof as Record<string, unknown>),
                      capturedPIDCleanup: capturedPids.map((pid) => ({ pid, exited: !alive(pid) })),
                      isolatedStateRemoved: true,
                    },
                    null,
                    2,
                  ),
                );
            }),
          ),
        );
      } catch (error) {
        await cleanup();
        throw error;
      }
    }).pipe(Effect.flatten),
);
