import * as NodeServices from "@effect/platform-node/NodeServices";
import { assert, it } from "@effect/vitest";
import {
  ProjectId,
  EnvironmentId,
  RunId,
  MessageId,
  type OrchestrationV2Run,
  EventId,
  ProviderDriverKind,
  ProviderInstanceId,
  type Project,
  ThreadId,
  ProviderThreadId,
} from "@t3tools/contracts";
import type { ProviderInstance } from "@t3tools/provider-core/server/driver";
import type {
  NativeHistoryTurn,
  NativeSession,
  NativeSessionSource,
} from "@t3tools/provider-core/server/nativeSessions";
import * as McpProviderSessions from "@t3tools/provider-core/server/McpProviderSessions";
import * as FileSystem from "effect/FileSystem";
import * as Path from "effect/Path";
import {
  NativeClaudeMcpBindings,
  readClaudeMcpRelayCredential,
} from "./NativeClaudeMcpBindings.ts";
import { openNativeClaudeMcpHistory } from "./NativeClaudeMcpSharing.ts";
import * as IdAllocator from "@t3tools/provider-core/server/IdAllocator";
import type * as ProviderAdapter from "@t3tools/provider-core/server/ProviderAdapter";
import { ProviderDriverError } from "@t3tools/provider-core/server/errors";
import * as HostProcess from "@t3tools/shared/HostProcess";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Deferred from "effect/Deferred";
import * as Fiber from "effect/Fiber";
import * as Option from "effect/Option";
import * as DateTime from "effect/DateTime";
import * as Layer from "effect/Layer";
import * as Logger from "effect/Logger";
import * as SqlClient from "effect/sql/SqlClient";
import * as Sqlite from "../persistence/Sqlite.ts";
import * as ProjectService from "../project/ProjectService.ts";
import * as ServerSettings from "../serverSettings.ts";
import * as ExternalNativeRuns from "../orchestration-v2/ExternalNativeRuns.ts";
import * as McpSessionRegistry from "../mcp/McpSessionRegistry.ts";
import * as EventSink from "../orchestration-v2/EventSink.ts";
import * as EventStore from "../orchestration-v2/EventStore.ts";
import * as ProjectionStore from "../orchestration-v2/ProjectionStore.ts";
import {
  buildActiveShellSnapshot,
  loadShellSnapshotParts,
} from "../orchestration-v2/ShellStream.ts";
import * as ProviderSessionManager from "../orchestration-v2/ProviderSessionManager.ts";
import * as ProviderInstanceRegistry from "./ProviderInstanceRegistry.ts";
import * as NativeSessionSync from "./NativeSessionSync.ts";

const claude = ProviderInstanceId.make("shared-claude");
const opencode = ProviderInstanceId.make("shared-opencode");
const claudeDriver = ProviderDriverKind.make("claudeAgent");
const opencodeDriver = ProviderDriverKind.make("opencode");

const native = (id: string, cwd = "/host/repo", active = false): NativeSession => ({
  id,
  cwd,
  title: id,
  createdAt: 10000,
  updatedAt: 20000,
  archived: false,
  active,
});

const provider = (
  instanceId: ProviderInstanceId,
  driverKind: ProviderDriverKind,
  source: NativeSessionSource,
  enabled = true,
): ProviderInstance => ({
  instanceId,
  driverKind,
  enabled,
  displayName: undefined,
  continuationIdentity: { driverKind, continuationKey: instanceId },
  nativeSessions: source,
  get orchestrationAdapter(): never {
    throw new Error("Discovery cannot start a provider adapter");
  },
  get snapshot(): never {
    throw new Error("Discovery cannot probe model status");
  },
  get textGeneration(): never {
    throw new Error("Discovery cannot send a title prompt");
  },
});

const fixture = (
  instances: ReadonlyArray<ProviderInstance>,
  mintMcp = false,
  mcpStore?: McpProviderSessions.McpProviderSessions["Service"],
) => {
  const opened: Array<{ instanceId: ProviderInstanceId; nativeId: string }> = [];
  const resumed: Array<{ instanceId: ProviderInstanceId; nativeId: string }> = [];
  const resumedStatuses: string[] = [];
  const revoked: string[] = [];
  const nativeMcpBindings: Array<{ threadId: ThreadId; sessionId: string }> = [];
  const detached: Array<{ threadId: ThreadId; preserveNativeWork: boolean | undefined }> = [];
  const reconciled: Array<
    Parameters<ExternalNativeRuns.ExternalNativeRuns["Service"]["reconcileSnapshot"]>[0]
  > = [];
  const bootstrapped: Array<{
    workspaceRoot: string;
    allowMissingWorkspaceRoot: boolean | undefined;
  }> = [];
  const projects = new Map<string, Project>();
  const persistence = Layer.mergeAll(EventStore.layer, ProjectionStore.layer).pipe(
    Layer.provideMerge(Sqlite.layerMemory),
  );
  const dependencies = Layer.mergeAll(
    EventSink.layer.pipe(Layer.provideMerge(persistence)),
    NodeServices.layer,
    mcpStore
      ? Layer.succeed(McpProviderSessions.McpProviderSessions, mcpStore)
      : McpProviderSessions.layer,
    Layer.succeed(HostProcess.HomeDirectory, "/host"),
    IdAllocator.layer,
    ServerSettings.layerTest({
      providerInstances: {
        [claude]: { driver: claudeDriver, config: { sharedSessionRoot: "~/repo" } },
        [opencode]: { driver: opencodeDriver, config: { sharedSessionRoot: "~/repo" } },
      },
    }),
    Layer.mock(ProviderInstanceRegistry.ProviderInstanceRegistry)({
      listInstances: Effect.succeed(instances),
      getInstance: (id) => Effect.succeed(instances.find((instance) => instance.instanceId === id)),
    }),
    Layer.mock(ProjectService.ProjectService)({
      getById: (id) =>
        Effect.succeed(
          Option.fromNullishOr([...projects.values()].find((project) => project.id === id)),
        ),
      bootstrap: (input) =>
        Effect.sync(() => {
          bootstrapped.push({
            workspaceRoot: input.workspaceRoot,
            allowMissingWorkspaceRoot: input.allowMissingWorkspaceRoot,
          });
          const existing = projects.get(input.workspaceRoot);
          const project: Project = existing ?? {
            id: ProjectId.make(input.projectId),
            title: input.title ?? "Repository",
            workspaceRoot: input.workspaceRoot,
            defaultModelSelection: null,
            scripts: [],
            createdAt: "2026-10-23T00:00:00.000Z",
            updatedAt: "2026-10-23T00:00:00.000Z",
            deletedAt: null,
          };
          projects.set(input.workspaceRoot, project);
          return { project, created: existing === undefined };
        }),
    }),
    Layer.mock(ExternalNativeRuns.ExternalNativeRuns)({
      reconcileSnapshot: (input) =>
        Effect.sync(() => {
          reconciled.push(input);
        }),
    }),
    Layer.mock(McpSessionRegistry.McpSessionRegistry)({
      revokeProviderSession: (id) =>
        Effect.sync(() => {
          revoked.push(id);
        }),
    }),
    Layer.mock(ProviderSessionManager.ProviderSessionManagerV2)({
      detach: (input) =>
        Effect.gen(function* () {
          detached.push({ threadId: input.threadId, preserveNativeWork: input.preserveNativeWork });
          if (input.revokeMcpCredential) {
            revoked.push(`detached:${input.threadId}`);
            if (mcpStore) yield* mcpStore.clear(input.threadId);
          }
        }),
      open: (input) =>
        Effect.gen(function* () {
          if (mintMcp && mcpStore) {
            yield* mcpStore.set({
              environmentId: EnvironmentId.make("sharing-test"),
              threadId: input.threadId,
              providerInstanceId: input.modelSelection.instanceId,
              providerSessionId: `credential:${input.threadId}:${opened.length}`,
              endpoint: "http://127.0.0.1:1234/mcp",
              authorizationHeader: "Bearer history-credential",
              browserToolsAvailable: false,
              bindNativeSession: ({ sessionId }) =>
                Effect.sync(() => {
                  nativeMcpBindings.push({ threadId: input.threadId, sessionId });
                  return true;
                }),
            });
          }
          const identity = {
            instanceId: input.modelSelection.instanceId,
            nativeId: input.initialNativeThreadId ?? "missing",
          };
          opened.push(identity);
          return {
            providerSessionId: input.providerSessionId,
            resumeThread: ({
              providerThread,
            }: Parameters<ProviderAdapter.ProviderAdapterV2SessionRuntime["resumeThread"]>[0]) =>
              Effect.sync(() => {
                resumed.push(identity);
                resumedStatuses.push(providerThread.status);
                return providerThread;
              }),
          } as unknown as ProviderAdapter.ProviderAdapterV2SessionRuntime;
        }),
    }),
  );
  return {
    layer: NativeSessionSync.layer.pipe(Layer.provideMerge(dependencies)),
    opened,
    resumed,
    resumedStatuses,
    revoked,
    nativeMcpBindings,
    detached,
    reconciled,
    bootstrapped,
  };
};

const history = (sessionId: string, driver: string): ReadonlyArray<NativeHistoryTurn> => [
  {
    id: `${sessionId}-turn`,
    status: "completed",
    startedAt: 11000,
    completedAt: 12000,
    items: [
      {
        id: `${sessionId}-user`,
        type: "user",
        text: `question ${driver} ${sessionId}`,
        status: "completed",
      },
      {
        id: `${sessionId}-assistant`,
        type: "assistant",
        text: `answer ${driver} ${sessionId}`,
        status: "completed",
      },
    ],
  },
];

it.effect(
  "discovers isolated provider histories, filters native paths and passively attaches only active OpenCode",
  () =>
    Effect.gen(function* () {
      let rootSessions = [
        native("same-id", "/host/repo", true),
        native("cold", "/host/repo/nested"),
        { ...native("archived", "/host/repo/nested"), archived: true },
        native("outside", "/host/repo-other"),
        native("missing", "/host/repo/missing-directory"),
      ];
      const histories: string[] = [];
      let disabledCalls = 0;
      const sharedSource = (driver: string): NativeSessionSource => ({
        list: Effect.sync(() => rootSessions),
        history: (sessionId) =>
          Effect.sync(() => {
            histories.push(`${driver}:${sessionId}`);
            return history(sessionId, driver);
          }),
      });
      const f = fixture([
        provider(claude, claudeDriver, sharedSource("claude")),
        provider(opencode, opencodeDriver, sharedSource("opencode")),
        provider(
          ProviderInstanceId.make("disabled"),
          claudeDriver,
          {
            list: Effect.sync(() => {
              disabledCalls++;
              return [];
            }),
            history: () => Effect.succeed([]),
          },
          false,
        ),
      ]);
      const context = yield* Layer.build(f.layer);
      yield* Effect.gen(function* () {
        const sync = yield* NativeSessionSync.NativeSessionSync;
        const ids = yield* IdAllocator.IdAllocatorV2;
        const store = yield* ProjectionStore.ProjectionStoreV2;
        const sink = yield* EventSink.EventSinkV2;
        const settings = yield* ServerSettings.ServerSettingsService;
        const sql = yield* SqlClient.SqlClient;
        const threadId = (instanceId: ProviderInstanceId, nativeThreadId: string) =>
          ids.derive.threadFromProviderThread({
            driver: instanceId === claude ? claudeDriver : opencodeDriver,
            providerInstanceId: instanceId,
            nativeThreadId,
          });
        const clientSnapshot = Effect.fn("NativeSessionSyncTest.clientSnapshot")(function* () {
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
        const changeRoot = (instanceId: ProviderInstanceId, sharedSessionRoot: string) =>
          settings.updateProviderInstance({
            operation: "upsert",
            instanceId,
            instance: {
              driver: instanceId === claude ? claudeDriver : opencodeDriver,
              config: { sharedSessionRoot },
            },
          });
        yield* sync.sync;
        assert.equal(disabledCalls, 0);
        for (const instance of [claude, opencode]) {
          const records = yield* store.getThreadRecords(threadId(instance, "same-id"), [
            "messages",
          ]);
          assert.deepEqual(
            records.messages.map((message) => message.text),
            [
              `question ${instance === claude ? "claude" : "opencode"} same-id`,
              `answer ${instance === claude ? "claude" : "opencode"} same-id`,
            ],
          );
          assert.isNull(yield* store.getThreadShell(threadId(instance, "outside")));
          assert.isNotNull(yield* store.getThreadShell(threadId(instance, "missing")));
        }
        assert.notEqual(threadId(claude, "same-id"), threadId(opencode, "same-id"));
        assert.deepEqual(f.opened, [{ instanceId: opencode, nativeId: "same-id" }]);
        assert.deepEqual(f.resumed, f.opened);
        assert.isTrue(f.bootstrapped.every((input) => input.allowMissingWorkspaceRoot === true));
        const archivedBefore = (yield* store.getThreadShell(threadId(claude, "archived")))
          ?.archivedAt;
        const before = yield* sink.latestSequence();
        const historyCount = histories.length;
        yield* sync.sync;
        assert.equal(yield* sink.latestSequence(), before);
        assert.equal(histories.length - historyCount, 2);
        rootSessions = [...rootSessions, native("new-cli", "/host/repo/nested")];
        yield* sync.sync;
        assert.equal(
          (yield* store.getThreadRecords(threadId(claude, "new-cli"), ["messages"])).messages
            .length,
          2,
        );
        yield* changeRoot(claude, "/host/repo/nested");
        yield* sync.sync;
        assert.equal(
          (yield* store.getThread(threadId(claude, "same-id"))).nativeSessionScopeHidden,
          true,
        );
        assert.equal(
          (yield* store.getThread(threadId(opencode, "same-id"))).nativeSessionScopeHidden,
          undefined,
        );
        assert.isTrue(
          f.detached.some(
            (input) =>
              input.threadId === threadId(claude, "same-id") && input.preserveNativeWork === true,
          ),
        );
        const detachCount = f.detached.length;
        yield* sync.sync;
        assert.equal(f.detached.length, detachCount);
        yield* changeRoot(claude, "/host/repo/not-shared");
        yield* sync.sync;
        const hiddenSnapshot = yield* clientSnapshot();
        assert.isFalse(
          [...hiddenSnapshot.active, ...hiddenSnapshot.archived].some(
            (thread) => thread.providerInstanceId === claude,
          ),
        );
        yield* changeRoot(claude, "~/repo");
        yield* sync.sync;
        assert.equal(
          (yield* store.getThread(threadId(claude, "same-id"))).nativeSessionScopeHidden,
          false,
        );
        const restored = yield* clientSnapshot();
        assert.isTrue(restored.active.some((thread) => thread.id === threadId(claude, "same-id")));
        assert.isTrue(
          restored.archived.some((thread) => thread.id === threadId(claude, "archived")),
        );
        assert.deepEqual(
          (yield* store.getThreadShell(threadId(claude, "archived")))?.archivedAt,
          archivedBefore,
        );
        yield* changeRoot(claude, "");
        yield* sync.sync;
        assert.isNotNull(yield* store.getThreadShell(threadId(claude, "outside")));
        assert.isTrue(
          f.reconciled.every(
            (input) =>
              input.driver ===
              (input.providerInstanceId === claude ? claudeDriver : opencodeDriver),
          ),
        );
        assert.isFalse(f.opened.some((input) => input.instanceId === claude));
      }).pipe(Effect.provide(context));
    }).pipe(Effect.scoped),
);

it.effect("discovers every native source result without a fixed item cap", () =>
  Effect.gen(function* () {
    const all = Array.from({ length: 105 }, (_, i) => native(`session-${i}`));
    const f = fixture([
      provider(claude, claudeDriver, {
        list: Effect.succeed(all),
        history: () => Effect.succeed([]),
      }),
    ]);
    const context = yield* Layer.build(f.layer);
    yield* Effect.gen(function* () {
      yield* (yield* NativeSessionSync.NativeSessionSync).sync;
      const shell = yield* (yield* ProjectionStore.ProjectionStoreV2).getShellSnapshot();
      assert.equal(shell.threads.length, 105);
      assert.equal(f.bootstrapped.length, 105);
      assert.deepEqual(f.opened, []);
    }).pipe(Effect.provide(context));
  }).pipe(Effect.scoped),
);

it.effect(
  "contains discovery errors without leaking source payloads or blocking another provider",
  () => {
    const logged: unknown[] = [];
    const capture = Logger.make<unknown, void>((options) => {
      logged.push(Logger.formatStructured.log(options));
    });
    return Effect.gen(function* () {
      const secret = "private-transcript-secret-and-pair-token";
      const failure = new ProviderDriverError({
        driver: claudeDriver,
        instanceId: claude,
        detail: secret,
        cause: new Error(secret),
      });
      const f = fixture([
        provider(claude, claudeDriver, {
          list: Effect.fail(failure),
          history: () => Effect.succeed([]),
        }),
        provider(opencode, opencodeDriver, {
          list: Effect.succeed([native("healthy")]),
          history: (id) => Effect.succeed(history(id, "opencode")),
        }),
      ]);
      const context = yield* Layer.build(f.layer);
      yield* Effect.gen(function* () {
        yield* (yield* NativeSessionSync.NativeSessionSync).sync;
        const shell = yield* (yield* ProjectionStore.ProjectionStoreV2).getShellSnapshot();
        assert.equal(shell.threads.length, 1);
        assert.equal(shell.threads[0]?.providerInstanceId, opencode);
      }).pipe(Effect.provide(context));
      assert.notInclude(JSON.stringify(logged), secret);
      assert.include(JSON.stringify(logged), "Native session discovery unavailable");
    }).pipe(Effect.scoped, Effect.provide(Logger.layer([capture], { mergeWithExisting: false })));
  },
);

it.effect.each([
  { instanceId: opencode, driver: opencodeDriver, missingWorker: false },
  { instanceId: claude, driver: claudeDriver, missingWorker: false },
  { instanceId: claude, driver: claudeDriver, missingWorker: true },
])(
  "reattaches an idle $driver session with a previously running T3 run (missing=$missingWorker)",
  ({ instanceId, driver, missingWorker }) =>
    Effect.gen(function* () {
      let missing = false;
      let session = { ...native("finished-while-offline"), canObserveActive: true };
      const f = fixture([
        provider(instanceId, driver, {
          list: Effect.sync(() => (missing ? [] : [session])),
          history: () => Effect.succeed(history(session.id, "opencode")),
        }),
      ]);
      const context = yield* Layer.build(f.layer);
      yield* Effect.gen(function* () {
        const sync = yield* NativeSessionSync.NativeSessionSync;
        const store = yield* ProjectionStore.ProjectionStoreV2;
        const sink = yield* EventSink.EventSinkV2;
        const ids = yield* IdAllocator.IdAllocatorV2;
        const id = ids.derive.threadFromProviderThread({
          driver,
          providerInstanceId: instanceId,
          nativeThreadId: session.id,
        });
        yield* sync.sync;
        const records = yield* store.getThreadRecords(id, ["runs", "providerThreads"]);
        const run = records.runs[0]!;
        const providerThread = records.providerThreads[0]!;
        const now = yield* DateTime.now;
        yield* sink.write({
          events: [
            {
              id: EventId.make("offline-run-active"),
              type: "run.updated",
              threadId: id,
              runId: run.id,
              occurredAt: now,
              payload: { ...run, status: "waiting", completedAt: null },
            },
            {
              id: EventId.make("offline-thread-active"),
              type: "provider-thread.updated",
              threadId: id,
              occurredAt: now,
              payload: { ...providerThread, status: "active" },
            },
          ],
        });
        session = { ...session, updatedAt: session.updatedAt + 1 };
        yield* sync.sync;
        assert.deepEqual(f.resumed, []);
        yield* sink.write({
          events: [
            {
              id: EventId.make("offline-run-running"),
              type: "run.updated",
              threadId: id,
              runId: run.id,
              occurredAt: now,
              payload: { ...run, status: "running", completedAt: null },
            },
            {
              id: EventId.make("external-thread-still-idle"),
              type: "provider-thread.updated",
              threadId: id,
              occurredAt: now,
              payload: { ...providerThread, status: "idle" },
            },
          ],
        });
        missing = missingWorker;
        session = { ...session, updatedAt: session.updatedAt + 1 };
        yield* sync.sync;
        assert.deepEqual(f.resumed, [{ instanceId, nativeId: session.id }]);
        assert.deepEqual(f.resumedStatuses, ["active"]);
      }).pipe(Effect.provide(context));
    }).pipe(Effect.scoped),
);

it.effect(
  "opens a passive observer for an active Claude foreground session without starting a writer",
  () =>
    Effect.gen(function* () {
      const session = { ...native("foreground-cli", "/host/repo", true), canObserveActive: true };
      const f = fixture([
        provider(claude, claudeDriver, {
          list: Effect.succeed([session]),
          history: () => Effect.succeed([]),
        }),
      ]);
      const context = yield* Layer.build(f.layer);
      yield* Effect.gen(function* () {
        yield* (yield* NativeSessionSync.NativeSessionSync).sync;
        assert.deepEqual(f.opened, [{ instanceId: claude, nativeId: session.id }]);
        assert.deepEqual(f.resumed, f.opened);
      }).pipe(Effect.provide(context));
    }).pipe(Effect.scoped),
);

it.effect(
  "applies native metadata edges while preserving unchanged-source local titles and archives",
  () =>
    Effect.gen(function* () {
      let session = native("metadata-session");
      const f = fixture([
        provider(claude, claudeDriver, {
          list: Effect.sync(() => [session]),
          history: () => Effect.succeed([]),
        }),
      ]);
      const context = yield* Layer.build(f.layer);
      yield* Effect.gen(function* () {
        const sync = yield* NativeSessionSync.NativeSessionSync;
        const store = yield* ProjectionStore.ProjectionStoreV2;
        const sink = yield* EventSink.EventSinkV2;
        const ids = yield* IdAllocator.IdAllocatorV2;
        const id = ids.derive.threadFromProviderThread({
          driver: claudeDriver,
          providerInstanceId: claude,
          nativeThreadId: session.id,
        });
        yield* sync.sync;
        session = { ...session, title: "Native automatic title" };
        yield* sync.sync;
        assert.equal((yield* store.getThread(id)).title, "Native automatic title");
        const localArchive = DateTime.makeUnsafe(42000);
        const initial = yield* store.getThread(id);
        yield* sink.write({
          events: [
            {
              id: EventId.make("metadata-local-customization"),
              type: "thread.archived",
              threadId: id,
              occurredAt: localArchive,
              payload: {
                ...initial,
                title: "My custom title",
                titleSource: "user",
                archivedAt: localArchive,
              },
            },
          ],
        });
        const beforeUnchanged = yield* sink.latestSequence();
        yield* sync.sync;
        assert.equal(yield* sink.latestSequence(), beforeUnchanged);
        assert.equal((yield* store.getThread(id)).title, "My custom title");
        assert.deepEqual((yield* store.getThread(id)).archivedAt, localArchive);
        // Rename without changing the summary timestamp to exercise source title identity.
        session = { ...session, title: "Renamed in native CLI" };
        yield* sync.sync;
        assert.equal((yield* store.getThread(id)).title, "My custom title");
        assert.deepEqual((yield* store.getThread(id)).archivedAt, localArchive);
        session = { ...session, archived: true };
        yield* sync.sync;
        assert.deepEqual((yield* store.getThread(id)).archivedAt, localArchive);
        session = { ...session, archived: false };
        yield* sync.sync;
        assert.isNull((yield* store.getThread(id)).archivedAt);
        const renamed = yield* store.getThread(id);
        yield* sink.write({
          events: [
            {
              id: EventId.make("metadata-local-custom-title"),
              type: "thread.metadata-updated",
              threadId: id,
              occurredAt: localArchive,
              payload: { ...renamed, title: "My newer title", titleSource: "user" },
            },
          ],
        });
        session = { ...session, updatedAt: session.updatedAt + 1 };
        yield* sync.sync;
        assert.equal((yield* store.getThread(id)).title, "My newer title");
        session = { ...session, archived: true };
        yield* sync.sync;
        assert.isNotNull((yield* store.getThread(id)).archivedAt);
        assert.equal((yield* store.getThread(id)).title, "My newer title");
        const archived = yield* store.getThread(id);
        yield* sink.write({
          events: [
            {
              id: EventId.make("metadata-local-unarchive"),
              type: "thread.unarchived",
              threadId: id,
              occurredAt: localArchive,
              payload: { ...archived, archivedAt: null },
            },
          ],
        });
        yield* sync.sync;
        assert.isNull((yield* store.getThread(id)).archivedAt);
        session = { ...session, title: "Next native rename" };
        yield* sync.sync;
        assert.equal((yield* store.getThread(id)).title, "My newer title");
        assert.isNull((yield* store.getThread(id)).archivedAt);
        session = { ...session, model: "claude-sonnet-native" };
        yield* sync.sync;
        assert.equal((yield* store.getThread(id)).modelSelection.model, "claude-sonnet-native");
        assert.equal((yield* store.getThread(id)).title, "My newer title");
        assert.isNull((yield* store.getThread(id)).archivedAt);
        session = { ...session, model: "claude-opus-native" };
        yield* sync.sync;
        assert.equal((yield* store.getThread(id)).modelSelection.model, "claude-opus-native");
        const legacy = yield* store.getThread(id);
        yield* sink.write({
          events: [
            {
              id: EventId.make("metadata-non-native-origin"),
              type: "thread.metadata-updated",
              threadId: id,
              occurredAt: localArchive,
              payload: { ...legacy, historyOrigin: "v1_import", title: "Keep legacy metadata" },
            },
          ],
        });
        session = { ...session, title: "Ignored native rename", archived: false };
        yield* sync.sync;
        session = { ...session, archived: true };
        yield* sync.sync;
        assert.equal((yield* store.getThread(id)).title, "Keep legacy metadata");
        assert.isNull((yield* store.getThread(id)).archivedAt);
        const { historyOrigin: _origin, ...createdInT3 } = yield* store.getThread(id);
        yield* sink.write({
          events: [
            {
              id: EventId.make("metadata-t3-native-title"),
              type: "thread.metadata-updated",
              threadId: id,
              occurredAt: localArchive,
              payload: { ...createdInT3, title: session.title, titleSource: "native" },
            },
          ],
        });
        session = { ...session, title: "Native title for T3 session" };
        yield* sync.sync;
        assert.equal((yield* store.getThread(id)).title, "Native title for T3 session");
        session = { ...session, title: "Native rename while T3 offline" };
        const restarted = yield* NativeSessionSync.NativeSessionSync.pipe(
          Effect.provide(Layer.fresh(NativeSessionSync.layer)),
        );
        yield* restarted.sync;
        assert.equal((yield* store.getThread(id)).title, "Native rename while T3 offline");
        const followed = yield* store.getThread(id);
        yield* sink.write({
          events: [
            {
              id: EventId.make("manual-title-durable"),
              type: "thread.metadata-updated",
              threadId: id,
              occurredAt: localArchive,
              payload: { ...followed, title: "Durable user title", titleSource: "user" },
            },
          ],
        });
        session = { ...session, title: "Another offline native rename" };
        const restartedAgain = yield* NativeSessionSync.NativeSessionSync.pipe(
          Effect.provide(Layer.fresh(NativeSessionSync.layer)),
        );
        yield* restartedAgain.sync;
        assert.equal((yield* store.getThread(id)).title, "Durable user title");
      }).pipe(Effect.provide(context));
    }).pipe(Effect.scoped),
);

it.effect(
  "lists history within the configured root and paginates without importing or starting work",
  () =>
    Effect.gen(function* () {
      const candidates = Array.from({ length: 35 }, (_, index) => ({
        ...native(`saved-${index}`),
        updatedAt: index,
        sharingState: "history" as const,
      }));
      const context = fixture([
        provider(claude, claudeDriver, {
          list: Effect.succeed([]),
          history: () => Effect.succeed([]),
          sharing: {
            list: Effect.succeed([
              ...candidates,
              { ...native("outside", "/other"), sharingState: "history" },
            ]),
            open: () => Effect.die("Browsing must not start a worker"),
          },
        }),
      ]);
      yield* Effect.gen(function* () {
        const sync = yield* NativeSessionSync.NativeSessionSync;
        const first = yield* sync.list({ instanceId: claude });
        assert.lengthOf(first.sessions, 30);
        assert.equal(first.nextOffset, 30);
        assert.equal(first.sessions[0]?.sessionId, "saved-34");
        const next = yield* sync.list({ instanceId: claude, offset: 30 });
        assert.lengthOf(next.sessions, 5);
        assert.isNull(next.nextOffset);
        assert.lengthOf((yield* sync.list({ instanceId: claude, query: "saved-34" })).sessions, 1);
        assert.lengthOf((yield* sync.list({ instanceId: claude, query: "outside" })).sessions, 0);
        assert.lengthOf(context.bootstrapped, 0);
        assert.lengthOf(context.opened, 0);
      }).pipe(Effect.provide(context.layer));
    }).pipe(Effect.scoped),
);

it.effect(
  "opens history with the same native identity and rejects foreground, out-of-root and changed identities",
  () =>
    Effect.gen(function* () {
      const history = { ...native("saved"), sharingState: "history" as const };
      const foreground = { ...native("foreground"), sharingState: "foreground" as const };
      const outside = { ...native("outside", "/other"), sharingState: "history" as const };
      let shared: NativeSession[] = [];
      const requests: string[] = [];
      const context = fixture([
        provider(claude, claudeDriver, {
          list: Effect.sync(() => shared),
          history: () => Effect.succeed([]),
          sharing: {
            list: Effect.succeed([
              history,
              foreground,
              outside,
              { ...native("wrong"), sharingState: "history" },
            ]),
            open: (id) =>
              Effect.sync(() => {
                requests.push(id);
                const session = native(id === "wrong" ? "copy" : id);
                shared = [session];
                return session;
              }),
          },
        }),
      ]);
      yield* Effect.gen(function* () {
        const sync = yield* NativeSessionSync.NativeSessionSync;
        assert.equal(
          (yield* sync.open({ instanceId: claude, sessionId: "foreground" }).pipe(Effect.flip))
            .reason,
          "foreground",
        );
        assert.equal(
          (yield* sync.open({ instanceId: claude, sessionId: "outside" }).pipe(Effect.flip)).reason,
          "outside-root",
        );
        assert.lengthOf(requests, 0);
        const first = yield* sync.open({ instanceId: claude, sessionId: "saved" });
        const store = yield* ProjectionStore.ProjectionStoreV2;
        const sink = yield* EventSink.EventSinkV2;
        const thread = yield* store.getThread(first.threadId);
        const at = DateTime.makeUnsafe(30000);
        yield* sink.write({
          events: [
            {
              id: EventId.make("archive-shared-history"),
              type: "thread.archived",
              threadId: thread.id,
              occurredAt: at,
              payload: { ...thread, archivedAt: at, updatedAt: at },
            },
          ],
        });
        const again = yield* sync.open({ instanceId: claude, sessionId: "saved" });
        assert.equal(first.threadId, again.threadId);
        assert.isNull((yield* store.getThread(first.threadId)).archivedAt);
        const records = yield* store.getThreadProviderContext(first.threadId, claude);
        assert.equal(records.providerThreads[0]?.nativeThreadRef?.nativeId, "saved");
        assert.equal(
          (yield* sync.open({ instanceId: claude, sessionId: "wrong" }).pipe(Effect.flip)).reason,
          "open-failed",
        );
        assert.equal((yield* store.getShellSnapshot()).threads.length, 1);
      }).pipe(Effect.provide(context.layer));
    }).pipe(Effect.scoped),
);

it.effect(
  "changes model activity only through the idle owned session action and preserves its thread",
  () =>
    Effect.gen(function* () {
      const idle = {
        ...native("owned"),
        sharingState: "shared" as const,
        modelActivityEnabled: true,
      };
      const busy = {
        ...native("busy", "/host/repo", true),
        sharingState: "shared" as const,
        modelActivityEnabled: true,
      };
      const calls: Array<{ id: string; enabled: boolean }> = [];
      const context = fixture([
        provider(claude, claudeDriver, {
          list: Effect.succeed([idle, busy]),
          history: () => Effect.succeed([]),
          sharing: {
            list: Effect.succeed([idle, busy]),
            open: () => Effect.die("An activity action must use the native reconfiguration method"),
            setModelActivityEnabled: (id, enabled) =>
              Effect.sync(() => {
                calls.push({ id, enabled });
                return { ...idle, modelActivityEnabled: enabled };
              }),
          },
        }),
      ]);
      yield* Effect.gen(function* () {
        const sync = yield* NativeSessionSync.NativeSessionSync;
        const listed = yield* sync.list({ instanceId: claude });
        assert.isTrue(listed.sessions.find((session) => session.sessionId === "busy")?.active);
        assert.isTrue(
          listed.sessions.find((session) => session.sessionId === "owned")?.modelActivityEnabled,
        );
        const disabled = yield* sync.open({
          instanceId: claude,
          sessionId: "owned",
          modelActivityAction: "disable",
        });
        const enabled = yield* sync.open({
          instanceId: claude,
          sessionId: "owned",
          modelActivityAction: "enable",
        });
        assert.equal(disabled.threadId, enabled.threadId);
        assert.deepEqual(calls, [
          { id: "owned", enabled: false },
          { id: "owned", enabled: true },
        ]);
        assert.equal(
          (yield* sync
            .open({ instanceId: claude, sessionId: "busy", modelActivityAction: "disable" })
            .pipe(Effect.flip)).reason,
          "open-failed",
        );
        assert.equal(
          (yield* sync
            .open({
              instanceId: claude,
              sessionId: "owned",
              modelActivityAction: "enable",
              observeModelActivity: true,
            })
            .pipe(Effect.flip)).reason,
          "open-failed",
        );
        assert.lengthOf(calls, 2);
      }).pipe(Effect.provide(context.layer));
    }).pipe(Effect.scoped),
);

it.effect(
  "waits for a fork's intended durable owner before importing copied native identities",
  () =>
    Effect.gen(function* () {
      const targetId = ThreadId.make("fork-selected-target");
      const sessions = [
        native("fork-source"),
        { ...native("fork-child"), requiresExistingOwner: { threadId: targetId } },
      ];
      const reads: string[] = [];
      const f = fixture([
        provider(claude, claudeDriver, {
          list: Effect.succeed(sessions),
          history: (id) =>
            Effect.sync(() => {
              reads.push(id);
              return history("copied-uuid", "claude");
            }),
        }),
      ]);
      const context = yield* Layer.build(f.layer);
      yield* Effect.gen(function* () {
        const sync = yield* NativeSessionSync.NativeSessionSync;
        const store = yield* ProjectionStore.ProjectionStoreV2;
        const sink = yield* EventSink.EventSinkV2;
        const ids = yield* IdAllocator.IdAllocatorV2;
        const sourceId = ids.derive.threadFromProviderThread({
          driver: claudeDriver,
          providerInstanceId: claude,
          nativeThreadId: "fork-source",
        });
        yield* sync.sync;
        yield* sync.sync;
        assert.deepEqual(reads, ["fork-source"]);
        assert.isNull(yield* store.getThreadShell(targetId));
        const source = yield* store.getThreadProjection(sourceId);
        const providerThreadId = ProviderThreadId.make("fork-target-provider");
        const at = yield* DateTime.now;
        yield* sink.write({
          events: [
            {
              id: EventId.make("fork-target-created"),
              type: "thread.created",
              threadId: targetId,
              occurredAt: at,
              payload: { ...source.thread, id: targetId, activeProviderThreadId: providerThreadId },
            },
            {
              id: EventId.make("fork-provider-created"),
              type: "provider-thread.updated",
              threadId: targetId,
              occurredAt: at,
              payload: {
                ...source.providerThreads[0]!,
                id: providerThreadId,
                appThreadId: targetId,
                nativeThreadRef: {
                  driver: claudeDriver,
                  nativeId: "fork-child",
                  strength: "strong",
                },
                nativeMetadata: { claudeForkIdentityNamespace: "fork-child" },
                forkedFrom: { providerThreadId: source.providerThreads[0]!.id },
              },
            },
          ],
        });
        yield* sync.sync;
        const target = yield* store.getThreadProjection(targetId);
        const unchanged = yield* store.getThreadProjection(sourceId);
        assert.isTrue(reads.includes("fork-child"));
        assert.equal(target.messages.length, source.messages.length);
        assert.isFalse(
          target.messages.some((message) =>
            source.messages.some((original) => original.id === message.id),
          ),
        );
        assert.deepEqual(unchanged.messages, source.messages);
        assert.equal(
          target.providerThreads[0]?.nativeMetadata?.claudeForkIdentityNamespace,
          "fork-child",
        );
        // A marker for another target cannot be satisfied by an unrelated existing owner.
        sessions[1] = {
          ...sessions[1]!,
          updatedAt: 30000,
          requiresExistingOwner: { threadId: "wrong-target" },
        };
        const before = reads.length;
        yield* sync.sync;
        assert.equal(reads.length, before);
      }).pipe(Effect.provide(context));
    }).pipe(Effect.scoped),
);

it.effect.each(["history", "copy", "existing", "missing"] as const)(
  "UI Claude sharing %s prepares the actual T3 owner's MCP before the native launch without upgrading old workers",
  (mode) =>
    Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem;
      const path = yield* Path.Path;
      const mcpStore = yield* McpProviderSessions.McpProviderSessions;
      const root = yield* fs.realPath(yield* fs.makeTempDirectoryScoped());
      const bindings = new NativeClaudeMcpBindings(path.join(root, "bindings"));
      const nativeId = "aaaaaaaa-1234-1234-1234-123456789abc";
      const candidate = {
        ...native(nativeId),
        sharingState: mode === "existing" ? ("shared" as const) : ("history" as const),
      };
      let shared: NativeSession[] = mode === "existing" ? [candidate] : [];
      let owner: ThreadId | undefined;
      let nativeOptions:
        | { readonly observeModelActivity?: boolean; readonly mcpConfigPath?: string }
        | undefined;
      const context = fixture(
        [
          provider(claude, claudeDriver, {
            list: Effect.sync(() => shared),
            history: () => Effect.succeed([]),
            sharing: {
              list: Effect.succeed([candidate]),
              open: (sessionId, options) => {
                owner = options?.threadId;
                return openNativeClaudeMcpHistory({
                  sessionId,
                  instanceId: claude,
                  ...(options ? { options } : {}),
                  bindings,
                  readConfig: mcpStore.read,
                  launcher: (directory) => ({ command: "node", args: ["relay", directory] }),
                  sessions: {
                    available: async () => [candidate],
                    open: async (id, openOptions) => {
                      nativeOptions = openOptions;
                      if (mode !== "existing") {
                        assert.isDefined(owner);
                        assert.isDefined(openOptions?.mcpConfigPath);
                      }
                      const result = native(
                        mode === "copy" ? "bbbbbbbb-1234-1234-1234-123456789abc" : id,
                      );
                      if (mode !== "copy") shared = [result];
                      return result;
                    },
                  },
                }).pipe(
                  Effect.mapError(
                    (cause) =>
                      new ProviderDriverError({
                        driver: claudeDriver,
                        instanceId: claude,
                        detail: cause.message,
                        cause,
                      }),
                  ),
                );
              },
            },
          }),
        ],
        mode !== "missing",
        mcpStore,
      );
      yield* Effect.gen(function* () {
        const sync = yield* NativeSessionSync.NativeSessionSync;
        const result = yield* Effect.exit(
          sync.open({
            instanceId: claude,
            sessionId: nativeId,
            ...(mode === "existing" ? {} : { observeModelActivity: true }),
          }),
        );
        const store = yield* ProjectionStore.ProjectionStoreV2;
        if (mode === "copy" || mode === "missing") {
          assert.isTrue(Exit.isFailure(result));
          const shell = yield* store.getShellSnapshot();
          assert.lengthOf(shell.threads, 0);
          assert.lengthOf(shell.archivedThreads, 1);
          assert.deepEqual(context.revoked, mode === "copy" ? [`credential:${owner}:0`] : []);
          assert.isTrue(context.detached.every((entry) => entry.preserveNativeWork === true));
          if (mode === "missing") assert.isUndefined(nativeOptions);
          assert.isUndefined(yield* mcpStore.read(owner!));
          assert.isFalse(yield* Effect.promise(() => bindings.hasBinding(nativeId)));
        } else {
          assert.isTrue(Exit.isSuccess(result));
          if (Exit.isFailure(result)) return;
          const thread = yield* store.getThread(result.value.threadId);
          if (mode === "existing") {
            assert.isUndefined(owner);
            assert.isUndefined(nativeOptions?.mcpConfigPath);
            assert.lengthOf(context.opened, 0);
          } else {
            assert.equal(owner, thread.id);
            assert.equal(nativeOptions?.observeModelActivity, true);
            const prepared = path.dirname(nativeOptions!.mcpConfigPath!);
            const credential = yield* Effect.promise(() =>
              readClaudeMcpRelayCredential({
                launchDirectory: prepared,
                nativeSessionId: nativeId,
                discovery: false,
              }),
            );
            assert.equal(credential.threadId, thread.id);
            assert.equal(credential.providerInstanceId, claude);
            assert.deepEqual(context.nativeMcpBindings, [
              { threadId: thread.id, sessionId: nativeId },
            ]);
          }
        }
      }).pipe(Effect.provide(context.layer));
    }).pipe(
      Effect.scoped,
      Effect.provide(Layer.mergeAll(NodeServices.layer, McpProviderSessions.layer)),
    ),
);

it.effect(
  "failed history reopening revokes only its fresh credential and leaves an existing shell unchanged",
  () =>
    Effect.gen(function* () {
      const mcpStore = yield* McpProviderSessions.McpProviderSessions;
      const candidate = { ...native("existing-history"), sharingState: "history" as const };
      let fail = false;
      let shared: NativeSession[] = [];
      const context = fixture(
        [
          provider(claude, claudeDriver, {
            list: Effect.sync(() => shared),
            history: () => Effect.succeed([]),
            sharing: {
              list: Effect.succeed([candidate]),
              open: () =>
                fail
                  ? Effect.fail(
                      new ProviderDriverError({
                        driver: claudeDriver,
                        instanceId: claude,
                        detail: "native claim failed",
                      }),
                    )
                  : Effect.sync(() => {
                      shared = [candidate];
                      return candidate;
                    }),
            },
          }),
        ],
        true,
        mcpStore,
      );
      yield* Effect.gen(function* () {
        const sync = yield* NativeSessionSync.NativeSessionSync;
        const first = yield* sync.open({ instanceId: claude, sessionId: candidate.id });
        const store = yield* ProjectionStore.ProjectionStoreV2;
        const sink = yield* EventSink.EventSinkV2;
        const before = yield* store.getThread(first.threadId);
        const archivedAt = DateTime.makeUnsafe(30000);
        yield* sink.write({
          events: [
            {
              id: EventId.make("switch-existing-history-owner-provider"),
              type: "thread.provider-switched",
              threadId: before.id,
              occurredAt: archivedAt,
              payload: {
                ...before,
                providerInstanceId: opencode,
                modelSelection: { instanceId: opencode, model: "other-provider-model" },
                updatedAt: archivedAt,
              },
            },
            {
              id: EventId.make("archive-existing-history-failure"),
              type: "thread.archived",
              threadId: before.id,
              occurredAt: archivedAt,
              payload: {
                ...before,
                archivedAt,
                updatedAt: archivedAt,
                providerInstanceId: opencode,
                modelSelection: { instanceId: opencode, model: "other-provider-model" },
              },
            },
          ],
        });
        const originalCredential = (yield* mcpStore.read(first.threadId))!.providerSessionId;
        yield* mcpStore.clear(first.threadId);
        shared = [];
        fail = true;
        assert.isTrue(
          Exit.isFailure(
            yield* Effect.exit(sync.open({ instanceId: claude, sessionId: candidate.id })),
          ),
        );
        const after = yield* store.getThread(first.threadId);
        assert.equal(after.title, before.title);
        assert.equal(after.modelSelection.instanceId, opencode);
        assert.isTrue(context.opened.every((request) => request.instanceId === claude));
        assert.equal(DateTime.toEpochMillis(after.archivedAt!), 30000);
        assert.deepEqual(context.revoked, [`credential:${first.threadId}:1`]);
        assert.isFalse(context.revoked.includes(originalCredential));
        assert.deepEqual(context.detached, [
          { threadId: first.threadId, preserveNativeWork: true },
        ]);
        assert.isUndefined(yield* mcpStore.read(first.threadId));
      }).pipe(Effect.provide(context.layer));
    }).pipe(Effect.scoped, Effect.provide(McpProviderSessions.layer)),
);

const sharingActiveRun = (
  threadId: ThreadId,
  status: "starting" | "running",
  at: DateTime.Utc,
): OrchestrationV2Run => ({
  id: RunId.make(`shared-conversion-${status}`),
  threadId,
  ordinal: 1,
  providerInstanceId: claude,
  modelSelection: { instanceId: claude, model: "native-model" },
  providerThreadId: null,
  userMessageId: MessageId.make("concurrent-shared-input"),
  rootNodeId: null,
  activeAttemptId: null,
  status,
  requestedAt: at,
  startedAt: status === "running" ? at : null,
  completedAt: null,
  checkpointId: null,
  contextHandoffId: null,
});

it.effect.each(["starting", "running"] as const)(
  "failed history conversion preserves a concurrent %s caller's same-generation token and shell",
  (status) =>
    Effect.gen(function* () {
      const mcpStore = yield* McpProviderSessions.McpProviderSessions;
      const entered = yield* Deferred.make<ThreadId>();
      const release = yield* Deferred.make<void>();
      const candidate = { ...native(`deferred-${status}`), sharingState: "history" as const };
      const context = fixture(
        [
          provider(claude, claudeDriver, {
            list: Effect.succeed([]),
            history: () => Effect.succeed([]),
            sharing: {
              list: Effect.succeed([candidate]),
              open: (_id, options) =>
                Effect.gen(function* () {
                  yield* Deferred.succeed(entered, options!.threadId!);
                  yield* Deferred.await(release);
                  return yield* Effect.fail(
                    new ProviderDriverError({
                      driver: claudeDriver,
                      instanceId: claude,
                      detail: "claim failed after caller attached",
                    }),
                  );
                }),
            },
          }),
        ],
        true,
        mcpStore,
      );
      yield* Effect.gen(function* () {
        const sync = yield* NativeSessionSync.NativeSessionSync;
        const store = yield* ProjectionStore.ProjectionStoreV2;
        const sink = yield* EventSink.EventSinkV2;
        const opening = yield* sync
          .open({ instanceId: claude, sessionId: candidate.id })
          .pipe(Effect.exit, Effect.forkScoped);
        const owner = yield* Deferred.await(entered);
        const credential = (yield* mcpStore.read(owner))!;
        const now = yield* DateTime.now;
        const run = sharingActiveRun(owner, status, now);
        yield* sink.write({
          events: [
            {
              id: EventId.make(`concurrent-${status}`),
              type: "run.updated",
              threadId: owner,
              runId: run.id,
              occurredAt: now,
              payload: run,
            },
          ],
        });
        yield* Deferred.succeed(release, undefined);
        assert.isTrue(Exit.isFailure(yield* Fiber.join(opening)));
        assert.equal(
          (yield* mcpStore.read(owner))?.providerSessionId,
          credential.providerSessionId,
        );
        assert.deepEqual(context.revoked, []);
        assert.deepEqual(context.detached, []);
        assert.isNull((yield* store.getThread(owner)).archivedAt);
      }).pipe(Effect.provide(context.layer));
    }).pipe(Effect.scoped, Effect.provide(McpProviderSessions.layer)),
);

it.effect.each(["foreign-credential", "active-run"] as const)(
  "history preopen rejects %s before touching another provider's caller",
  (mode) =>
    Effect.gen(function* () {
      const mcpStore = yield* McpProviderSessions.McpProviderSessions;
      const candidate = { ...native(`protected-${mode}`), sharingState: "history" as const };
      let sourceCalls = 0;
      let shared: NativeSession[] = [];
      const context = fixture(
        [
          provider(claude, claudeDriver, {
            list: Effect.sync(() => shared),
            history: () => Effect.succeed([]),
            sharing: {
              list: Effect.succeed([candidate]),
              open: () =>
                Effect.sync(() => {
                  sourceCalls++;
                  shared = [candidate];
                  return candidate;
                }),
            },
          }),
        ],
        true,
        mcpStore,
      );
      yield* Effect.gen(function* () {
        const sync = yield* NativeSessionSync.NativeSessionSync;
        const store = yield* ProjectionStore.ProjectionStoreV2;
        const sink = yield* EventSink.EventSinkV2;
        const first = yield* sync.open({ instanceId: claude, sessionId: candidate.id });
        const thread = yield* store.getThread(first.threadId);
        const now = yield* DateTime.now;
        yield* sink.write({
          events: [
            {
              id: EventId.make(`foreign-selection-${mode}`),
              type: "thread.provider-switched",
              threadId: thread.id,
              occurredAt: now,
              payload: {
                ...thread,
                providerInstanceId: opencode,
                modelSelection: { instanceId: opencode, model: "other-model" },
                updatedAt: now,
              },
            },
          ],
        });
        const previous = (yield* mcpStore.read(thread.id))!;
        const credential = {
          ...previous,
          ...(mode === "foreign-credential"
            ? {
                providerInstanceId: opencode,
                providerSessionId: "foreign-live-credential",
                authorizationHeader: "Bearer foreign-live-credential",
              }
            : {}),
        };
        yield* mcpStore.set(credential);
        if (mode === "active-run") {
          const run = sharingActiveRun(thread.id, "running", now);
          yield* sink.write({
            events: [
              {
                id: EventId.make("existing-active-sharing-run"),
                type: "run.updated",
                threadId: thread.id,
                runId: run.id,
                occurredAt: now,
                payload: run,
              },
            ],
          });
        }
        shared = [];
        assert.isTrue(
          Exit.isFailure(
            yield* Effect.exit(sync.open({ instanceId: claude, sessionId: candidate.id })),
          ),
        );
        assert.equal(context.opened.length, 1);
        assert.equal(sourceCalls, 1);
        assert.deepEqual(context.revoked, []);
        assert.deepEqual(context.detached, []);
        assert.equal(
          (yield* mcpStore.read(thread.id))?.providerSessionId,
          credential.providerSessionId,
        );
        assert.equal((yield* store.getThread(thread.id)).modelSelection.instanceId, opencode);
      }).pipe(Effect.provide(context.layer));
    }).pipe(Effect.scoped, Effect.provide(McpProviderSessions.layer)),
);
