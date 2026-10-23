import * as NodeServices from "@effect/platform-node/NodeServices";
import { NodeWS } from "@effect/platform-node/NodeSocket";
import { assert, it } from "@effect/vitest";
import {
  ProjectId,
  EventId,
  ProviderDriverKind,
  ProviderInstanceId,
  type Project,
} from "@t3tools/contracts";
import type { ProviderInstance } from "@t3tools/provider-core/server/driver";
import * as IdAllocator from "@t3tools/provider-core/server/IdAllocator";
import type * as ProviderAdapter from "@t3tools/provider-core/server/ProviderAdapter";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as SqlClient from "effect/sql/SqlClient";
import type * as CodexSchema from "effect-codex-app-server/schema";

import * as Sqlite from "../persistence/Sqlite.ts";
import * as ProjectService from "../project/ProjectService.ts";
import * as ServerSettings from "../serverSettings.ts";
import * as ExternalNativeRuns from "../orchestration-v2/ExternalNativeRuns.ts";
import * as EventSink from "../orchestration-v2/EventSink.ts";
import * as EventStore from "../orchestration-v2/EventStore.ts";
import * as ProjectionStore from "../orchestration-v2/ProjectionStore.ts";
import {
  buildActiveShellSnapshot,
  loadShellSnapshotParts,
} from "../orchestration-v2/ShellStream.ts";
import * as ProviderSessionManager from "../orchestration-v2/ProviderSessionManager.ts";
import * as ProviderInstanceRegistry from "./ProviderInstanceRegistry.ts";
import * as NativeCodexSessionSync from "./NativeCodexSessionSync.ts";

const instanceId = ProviderInstanceId.make("shared-codex");
const driver = ProviderDriverKind.make("codex");

const nativeThread = (id: string, cwd = "/repo"): CodexSchema.V2ThreadListResponse__Thread => ({
  id,
  cwd,
  cliVersion: "0.164.0",
  createdAt: 10,
  updatedAt: 20,
  ephemeral: false,
  modelProvider: "openai",
  model: "test-model",
  preview: id,
  name: id,
  projectId: null,
  sessionId: id,
  source: "cli",
  status: { type: "idle" },
  turns: [],
});

const providerInstance = (): ProviderInstance => ({
  instanceId,
  driverKind: driver,
  enabled: true,
  displayName: undefined,
  continuationIdentity: { driverKind: driver, continuationKey: "shared-codex" },
  get orchestrationAdapter(): never {
    throw new Error("Discovery must not start an agent turn.");
  },
  get snapshot(): never {
    throw new Error("Discovery must not probe provider model status.");
  },
  get textGeneration(): never {
    throw new Error("Discovery must not generate a title.");
  },
});

it.live(
  "discovers paginated native sessions through the real protocol and attaches only loaded threads",
  () =>
    Effect.gen(function* () {
      let newThreadAvailable = false;
      const requests: Array<{
        readonly method: string;
        readonly params?: Record<string, unknown>;
      }> = [];
      const opened: string[] = [];
      const resumed: string[] = [];
      const detached: string[] = [];
      const server = yield* Effect.acquireRelease(
        Effect.sync(() => new NodeWS.WebSocketServer({ port: 0, host: "127.0.0.1" })),
        (server) =>
          Effect.promise(
            () =>
              new Promise<void>((resolve) => {
                for (const client of server.clients) client.terminate();
                server.close(() => resolve());
              }),
          ),
      );
      if (server.address() === null)
        yield* Effect.callback<void>((resume) => {
          server.once("listening", () => resume(Effect.void));
        });
      const address = server.address();
      if (address === null || typeof address === "string")
        return yield* Effect.die("Missing fixture address");
      server.on("connection", (peer) =>
        peer.on("message", (raw) => {
          const request = JSON.parse(raw.toString()) as {
            id?: string | number;
            method: string;
            params?: Record<string, unknown>;
          };
          requests.push({
            method: request.method,
            ...(request.params === undefined ? {} : { params: request.params }),
          });
          if (request.id === undefined) return;
          const params = request.params ?? {};
          let result: unknown;
          switch (request.method) {
            case "initialize":
              result = {
                codexHome: "/fixture-codex",
                platformFamily: "unix",
                platformOs: "linux",
                userAgent: "fixture",
              };
              break;
            case "thread/list":
              result =
                params.archived === true
                  ? { data: [], nextCursor: null }
                  : params.cursor === "sessions-2"
                    ? {
                        data: [
                          nativeThread("cold", "/repo/nested"),
                          nativeThread("outside", "/repo-other"),
                          ...(newThreadAvailable ? [nativeThread("new-cli")] : []),
                        ],
                        nextCursor: null,
                      }
                    : { data: [nativeThread("live")], nextCursor: "sessions-2" };
              break;
            case "thread/loaded/list":
              result =
                params.cursor === "loaded-2"
                  ? { data: newThreadAvailable ? ["new-cli"] : [], nextCursor: null }
                  : { data: ["live"], nextCursor: "loaded-2" };
              break;
            case "thread/turns/list":
              result =
                params.cursor === "history-2"
                  ? {
                      data: [
                        {
                          id: `${params.threadId}-answer`,
                          status: "completed",
                          startedAt: 12,
                          completedAt: 13,
                          items: [
                            {
                              id: `${params.threadId}-assistant`,
                              type: "agentMessage",
                              text: `answer ${params.threadId}`,
                            },
                          ],
                        },
                      ],
                      nextCursor: null,
                    }
                  : {
                      data: [
                        {
                          id: `${params.threadId}-question`,
                          status: "completed",
                          startedAt: 10,
                          completedAt: 11,
                          items: [
                            {
                              id: `${params.threadId}-user`,
                              type: "userMessage",
                              content: [{ type: "text", text: `question ${params.threadId}` }],
                            },
                          ],
                        },
                      ],
                      nextCursor: "history-2",
                    };
              break;
            default:
              peer.send(
                JSON.stringify({
                  id: request.id,
                  error: { code: -32601, message: "Unexpected fixture method" },
                }),
              );
              return;
          }
          peer.send(JSON.stringify({ id: request.id, result }));
        }),
      );
      const projects = new Map<string, Project>();
      const persistence = Layer.mergeAll(EventStore.layer, ProjectionStore.layer).pipe(
        Layer.provideMerge(Sqlite.layerMemory),
      );
      const sink = EventSink.layer.pipe(Layer.provideMerge(persistence));
      const dependencies = Layer.mergeAll(
        sink,
        NodeServices.layer,
        IdAllocator.layer,
        ServerSettings.layerTest({
          providerInstances: {
            [instanceId]: {
              driver,
              config: {
                daemonEndpoint: `ws://127.0.0.1:${address.port}`,
                sharedSessionRoot: "/repo",
              },
            },
          },
        }),
        Layer.mock(ProviderInstanceRegistry.ProviderInstanceRegistry)({
          listInstances: Effect.succeed([providerInstance()]),
        }),
        Layer.mock(ProjectService.ProjectService)({
          getByWorkspaceRoot: (root) => Effect.succeed(Option.fromUndefinedOr(projects.get(root))),
          bootstrap: (input) =>
            Effect.sync(() => {
              const project: Project = {
                id: ProjectId.make(input.projectId),
                title: input.title ?? "Repository",
                workspaceRoot: input.workspaceRoot,
                defaultModelSelection: null,
                scripts: [],
                createdAt: "2026-10-22T00:00:00.000Z",
                updatedAt: "2026-10-22T00:00:00.000Z",
                deletedAt: null,
              };
              projects.set(input.workspaceRoot, project);
              return { project, created: true };
            }),
        }),
        Layer.mock(ExternalNativeRuns.ExternalNativeRuns)({ reconcileSnapshot: () => Effect.void }),
        Layer.mock(ProviderSessionManager.ProviderSessionManagerV2)({
          detach: (input) =>
            Effect.sync(() => {
              detached.push(input.threadId);
            }),
          open: (input) =>
            Effect.sync(() => {
              opened.push(input.initialNativeThreadId ?? "missing");
              return {
                resumeThread: ({
                  providerThread,
                }: Parameters<
                  ProviderAdapter.ProviderAdapterV2SessionRuntime["resumeThread"]
                >[0]) =>
                  Effect.sync(() => {
                    resumed.push(providerThread.nativeThreadRef?.nativeId ?? "missing");
                    return providerThread;
                  }),
              } as unknown as ProviderAdapter.ProviderAdapterV2SessionRuntime;
            }),
        }),
      );
      const context = yield* Layer.build(
        NativeCodexSessionSync.layer.pipe(Layer.provideMerge(dependencies)),
      );
      yield* Effect.gen(function* () {
        const sync = yield* NativeCodexSessionSync.NativeCodexSessionSync;
        const store = yield* ProjectionStore.ProjectionStoreV2;
        const sink = yield* EventSink.EventSinkV2;
        const ids = yield* IdAllocator.IdAllocatorV2;
        const settings = yield* ServerSettings.ServerSettingsService;
        const sql = yield* SqlClient.SqlClient;
        const clientSnapshot = Effect.fn("NativeCodexSessionSyncTest.clientSnapshot")(function* () {
          const parts = yield* loadShellSnapshotParts({
            sql,
            readThreads: store.readShellSnapshot(),
            listProjects: Effect.succeed([]),
            latestSequence: sink.latestSequence(),
          });
          return {
            active: buildActiveShellSnapshot(parts).threads,
            archived: parts.threads.archivedThreads,
          };
        });
        const changeRoot = (sharedSessionRoot: string) =>
          settings.updateProviderInstance({
            operation: "upsert",
            instanceId,
            instance: {
              driver,
              config: { daemonEndpoint: `ws://127.0.0.1:${address.port}`, sharedSessionRoot },
            },
          });
        const appThreadId = (nativeThreadId: string) =>
          ids.derive.threadFromProviderThread({
            driver,
            nativeThreadId,
            providerInstanceId: instanceId,
          });
        yield* sync.sync;
        const live = yield* store.getThreadRecords(appThreadId("live"), ["messages"]);
        assert.deepEqual(
          live.messages.map((message) => message.text),
          ["question live", "answer live"],
        );
        const cold = yield* store.getThreadRecords(appThreadId("cold"), ["messages"]);
        assert.equal(cold.messages.length, 2);
        assert.isNull(yield* store.getThreadShell(appThreadId("outside")));
        assert.deepEqual(opened, ["live"]);
        assert.deepEqual(resumed, ["live"]);
        const before = yield* sink.latestSequence();
        yield* sync.sync;
        assert.equal(yield* sink.latestSequence(), before);
        newThreadAvailable = true;
        yield* sync.sync;
        const added = yield* store.getThreadRecords(appThreadId("new-cli"), ["messages"]);
        assert.deepEqual(
          added.messages.map((message) => message.text),
          ["question new-cli", "answer new-cli"],
        );
        const coldThread = yield* store.getThread(appThreadId("cold"));
        const archivedAt = yield* DateTime.now;
        yield* sink.write({
          events: [
            {
              id: EventId.make("manual-cold-archive"),
              type: "thread.archived",
              threadId: coldThread.id,
              occurredAt: archivedAt,
              payload: { ...coldThread, archivedAt },
            },
          ],
        });
        const beforeScope = yield* clientSnapshot();
        assert.equal(
          beforeScope.active.some((thread) => thread.id === appThreadId("live")),
          true,
        );
        assert.deepEqual(
          beforeScope.archived.map((thread) => thread.id),
          [appThreadId("cold")],
        );
        yield* changeRoot("/repo/nested");
        yield* sync.sync;
        const hidden = yield* store.getThread(appThreadId("live"));
        assert.isNull(hidden.archivedAt);
        assert.equal(hidden.nativeSessionScopeHidden, true);
        assert.equal(detached.includes(appThreadId("live")), true);
        assert.equal(detached.includes(appThreadId("new-cli")), true);
        assert.equal(
          (yield* store.getThreadShell(appThreadId("live")))?.nativeSessionScopeHidden,
          true,
        );
        const nestedScope = yield* clientSnapshot();
        assert.equal(
          nestedScope.active.some((thread) => thread.id === appThreadId("live")),
          false,
        );
        assert.deepEqual(
          nestedScope.archived.map((thread) => thread.id),
          [appThreadId("cold")],
        );
        yield* changeRoot("/repo/not-shared");
        yield* sync.sync;
        const hiddenArchive = yield* store.getThreadShell(appThreadId("cold"));
        assert.equal(hiddenArchive?.nativeSessionScopeHidden, true);
        assert.equal(
          DateTime.toEpochMillis(hiddenArchive!.archivedAt!),
          DateTime.toEpochMillis(archivedAt),
        );
        const hiddenLists = yield* clientSnapshot();
        assert.deepEqual(hiddenLists.active, []);
        assert.deepEqual(hiddenLists.archived, []);
        yield* changeRoot("/repo");
        yield* sync.sync;
        const restored = yield* store.getThread(appThreadId("live"));
        assert.isNull(restored.archivedAt);
        assert.equal(restored.nativeSessionScopeHidden, false);
        const manualArchive = yield* store.getThread(appThreadId("cold"));
        assert.equal(
          DateTime.toEpochMillis(manualArchive.archivedAt!),
          DateTime.toEpochMillis(archivedAt),
        );
        const restoredLists = yield* clientSnapshot();
        assert.equal(
          restoredLists.active.some((thread) => thread.id === appThreadId("live")),
          true,
        );
        assert.deepEqual(
          restoredLists.archived.map((thread) => thread.id),
          [appThreadId("cold")],
        );
        assert.equal(opened.includes("cold"), false);
        assert.equal(opened.includes("new-cli"), true);
        assert.equal(
          requests.some(
            (request) =>
              request.method === "thread/list" && request.params?.cursor === "sessions-2",
          ),
          true,
        );
        assert.equal(
          requests.some(
            (request) =>
              request.method === "thread/loaded/list" && request.params?.cursor === "loaded-2",
          ),
          true,
        );
        assert.equal(
          requests.some(
            (request) =>
              request.method === "thread/turns/list" && request.params?.cursor === "history-2",
          ),
          true,
        );
        assert.equal(
          requests.some(
            (request) => request.method === "turn/start" || request.method === "thread/start",
          ),
          false,
        );
      }).pipe(Effect.provide(context));
    }).pipe(Effect.scoped),
);
