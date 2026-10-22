import {
  CodexSettings,
  ProviderDriverKind,
  DEFAULT_MODEL,
  DEFAULT_MODEL_BY_PROVIDER,
  DEFAULT_PROVIDER_INTERACTION_MODE,
  DEFAULT_RUNTIME_MODE,
  type OrchestrationV2AppThread,
  type OrchestrationV2ProviderThread,
  type ProviderInstanceId,
  type ThreadId,
} from "@t3tools/contracts";
import * as IdAllocator from "@t3tools/provider-core/server/IdAllocator";
import { mergeProviderInstanceEnvironment } from "@t3tools/provider-core/server/instanceEnvironment";
import { expandHomePath } from "@t3tools/provider-core/server/pathExpansion";
import * as HostProcess from "@t3tools/shared/HostProcess";
import * as Context from "effect/Context";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as FileSystem from "effect/FileSystem";
import * as ChildProcessSpawner from "effect/process/ChildProcessSpawner";
import * as Path from "effect/Path";
import * as Schedule from "effect/Schedule";
import * as Schema from "effect/Schema";
import * as Scope from "effect/Scope";
import type * as CodexSchema from "effect-codex-app-server/schema";
import * as EventSink from "../orchestration-v2/EventSink.ts";
import * as ExternalCodexRuns from "../orchestration-v2/ExternalCodexRuns.ts";
import * as ProjectionStore from "../orchestration-v2/ProjectionStore.ts";
import * as ProviderSessionManager from "../orchestration-v2/ProviderSessionManager.ts";
import * as ProjectService from "../project/ProjectService.ts";
import * as ServerSettings from "../serverSettings.ts";
import { forkParked } from "../serverActivation.ts";
import * as CodexDaemonConnection from "./CodexDaemonConnection.ts";
import * as NativeCodexHistory from "./NativeCodexHistory.ts";
import * as ProviderInstanceRegistry from "./ProviderInstanceRegistry.ts";

const CODEX = ProviderDriverKind.make("codex");
const decodeSettings = Schema.decodeUnknownEffect(CodexSettings);

export const isWithinSharedRoot = (path: Path.Path, cwd: string, root: string | undefined) => {
  if (!root?.trim()) return true;
  const relative = path.relative(path.resolve(root), path.resolve(cwd));
  return (
    relative === "" ||
    (!relative.startsWith(`..${path.sep}`) && relative !== ".." && !path.isAbsolute(relative))
  );
};

class NativeCodexPaginationError extends Schema.TaggedError<NativeCodexPaginationError>()(
  "NativeCodexPaginationError",
  {},
) {
  override get message(): string {
    return "Codex returned a repeated pagination cursor.";
  }
}

/** Follow every opaque cursor; history and discovery deliberately have no age or item cap. */
export const collectPages = Effect.fn("NativeCodexSessionSync.collectPages")(function* <A, E, R>(
  page: (
    cursor: string | undefined,
  ) => Effect.Effect<
    { readonly data: ReadonlyArray<A>; readonly nextCursor?: string | null },
    E,
    R
  >,
) {
  const data: A[] = [];
  let cursor: string | undefined;
  const seen = new Set<string>();
  do {
    const result = yield* page(cursor);
    data.push(...result.data);
    cursor = result.nextCursor ?? undefined;
    if (cursor !== undefined && seen.has(cursor)) return yield* new NativeCodexPaginationError({});
    if (cursor !== undefined) seen.add(cursor);
  } while (cursor !== undefined);
  return data;
});

export class NativeCodexSessionSync extends Context.Service<
  NativeCodexSessionSync,
  {
    readonly sync: Effect.Effect<void>;
    readonly start: Effect.Effect<void, never, Scope.Scope>;
  }
>()("t3/provider/NativeCodexSessionSync") {}

const make = Effect.gen(function* () {
  const registry = yield* ProviderInstanceRegistry.ProviderInstanceRegistry;
  const settingsService = yield* ServerSettings.ServerSettingsService;
  const projects = yield* ProjectService.ProjectService;
  const store = yield* ProjectionStore.ProjectionStoreV2;
  const sink = yield* EventSink.EventSinkV2;
  const manager = yield* ProviderSessionManager.ProviderSessionManagerV2;
  const externalRuns = yield* ExternalCodexRuns.ExternalCodexRuns;
  const ids = yield* IdAllocator.IdAllocatorV2;
  const path = yield* Path.Path;
  const fileSystem = yield* FileSystem.FileSystem;
  const spawner = yield* ChildProcessSpawner.ChildProcessSpawner;
  const environment = yield* HostProcess.Environment;
  const home = yield* HostProcess.HomeDirectory;
  const revisions = new Map<string, string>();
  let scan = 0;

  const ensureThread = Effect.fn("NativeCodexSessionSync.ensureThread")(function* (
    native: CodexSchema.V2ThreadListResponse__Thread,
    instanceId: ProviderInstanceId,
    owners: Map<string, ThreadId>,
    archived: boolean,
  ) {
    const existingOwner = owners.get(native.id);
    if (existingOwner !== undefined) {
      const context = yield* store.getThreadProviderContext(existingOwner, instanceId);
      const providerThread = context.providerThreads.find(
        (value) =>
          value.nativeThreadRef?.nativeId === native.id && value.providerInstanceId === instanceId,
      );
      if (providerThread !== undefined) return { appThread: context.thread, providerThread };
    }
    const threadId = ids.derive.threadFromProviderThread({
      driver: CODEX,
      nativeThreadId: native.id,
      providerInstanceId: instanceId,
    });
    const existing = yield* store.getThreadShell(threadId);
    if (existing !== null) {
      const context = yield* store.getThreadProviderContext(threadId, instanceId);
      const providerThread = context.providerThreads.find(
        (value) => value.nativeThreadRef?.nativeId === native.id,
      );
      if (providerThread !== undefined) return { appThread: context.thread, providerThread };
    }
    const project = (yield* projects.bootstrap({
      projectId: yield* ids.allocate.project({ fixtureName: "native-codex" }),
      commandId: yield* ids.allocate.command({
        fixtureName: "native-codex",
        commandName: "project.bootstrap",
      }),
      title: path.basename(native.cwd) || native.cwd,
      workspaceRoot: native.cwd,
      allowMissingWorkspaceRoot: true,
    })).project;
    const providerThreadId = ids.derive.providerThread({
      driver: CODEX,
      nativeThreadId: native.id,
      providerInstanceId: instanceId,
    });
    const createdAt = DateTime.makeUnsafe(native.createdAt * 1000);
    const updatedAt = DateTime.makeUnsafe(native.updatedAt * 1000);
    const appThread: OrchestrationV2AppThread = {
      createdBy: "system",
      creationSource: "provider",
      id: threadId,
      projectId: project.id,
      title: native.name?.trim() || native.preview.slice(0, 160) || "Codex session",
      providerInstanceId: instanceId,
      modelSelection: {
        instanceId,
        model: native.model || DEFAULT_MODEL_BY_PROVIDER[CODEX] || DEFAULT_MODEL,
      },
      runtimeMode: DEFAULT_RUNTIME_MODE,
      interactionMode: DEFAULT_PROVIDER_INTERACTION_MODE,
      branch: native.gitInfo?.branch ?? null,
      worktreePath: null,
      linkedPullRequest: null,
      branchPullRequest: null,
      activeProviderThreadId: providerThreadId,
      historyOrigin: "native",
      lineage: { parentThreadId: null, relationshipToParent: null, rootThreadId: threadId },
      forkedFrom: null,
      createdAt,
      updatedAt,
      archivedAt: archived ? updatedAt : null,
      settledOverride: null,
      settledAt: updatedAt,
      unsettledAt: null,
      snoozedUntil: null,
      snoozedAt: null,
      pinnedAt: null,
      pinOrderKey: null,
      activeOrderKey: null,
      lastVisitedAt: null,
      deletedAt: null,
    };
    const providerThread: OrchestrationV2ProviderThread = {
      id: providerThreadId,
      driver: CODEX,
      providerInstanceId: instanceId,
      providerSessionId: null,
      appThreadId: threadId,
      ownerNodeId: null,
      nativeThreadRef: { driver: CODEX, nativeId: native.id, strength: "strong" },
      nativeConversationHeadRef: null,
      status: "idle",
      firstRunOrdinal: null,
      lastRunOrdinal: null,
      handoffIds: [],
      forkedFrom: null,
      pendingBackgroundTasks: [],
      createdAt,
      updatedAt,
    };
    const events = [
      {
        id: yield* ids.allocate.event({ threadId }),
        type: "thread.created" as const,
        threadId,
        providerInstanceId: instanceId,
        occurredAt: createdAt,
        payload: appThread,
      },
      {
        id: yield* ids.allocate.event({ threadId }),
        type: "provider-thread.updated" as const,
        threadId,
        providerInstanceId: instanceId,
        occurredAt: updatedAt,
        payload: providerThread,
      },
    ];
    const committed = yield* sink.write({ unlessNativeThreadOwned: providerThread, events });
    if (committed.length === 0) return undefined;
    owners.set(native.id, threadId);
    return { appThread, providerThread };
  });

  const sync = Effect.gen(function* () {
    scan += 1;
    const settings = yield* settingsService.getSettings;
    const instances = yield* registry.listInstances;
    for (const instance of instances) {
      if (instance.driverKind !== "codex" || !instance.enabled) continue;
      yield* Effect.scoped(
        Effect.gen(function* () {
          const entry = settings.providerInstances[instance.instanceId];
          const config = yield* decodeSettings(entry?.config ?? {});
          const env = yield* mergeProviderInstanceEnvironment(entry?.environment, environment);
          const client = yield* CodexDaemonConnection.makeClient({
            binaryPath: expandHomePath(config.binaryPath, home),
            homePath: expandHomePath(config.homePath, home),
            ...(config.daemonEndpoint === undefined ? {} : { endpoint: config.daemonEndpoint }),
            environment: env,
          }).pipe(
            Effect.provideService(Path.Path, path),
            Effect.provideService(FileSystem.FileSystem, fileSystem),
            Effect.provideService(ChildProcessSpawner.ChildProcessSpawner, spawner),
          );
          yield* client.request("initialize", {
            clientInfo: { name: "t3-shared-sessions", version: "1.0.0" },
            capabilities: { experimentalApi: true },
          });
          yield* client.notify("initialized", undefined);
          const owners = new Map<string, ThreadId>();
          const shell = yield* store.getShellSnapshot();
          for (const thread of [...shell.threads, ...shell.archivedThreads]) {
            const context = yield* store.getThreadProviderContext(thread.id, instance.instanceId);
            for (const providerThread of context.providerThreads) {
              if (
                providerThread.providerInstanceId === instance.instanceId &&
                providerThread.nativeThreadRef?.nativeId != null
              )
                owners.set(providerThread.nativeThreadRef.nativeId, thread.id);
            }
          }
          const activeThreads = yield* collectPages((cursor) =>
            client.request("thread/list", {
              ...(cursor === undefined ? {} : { cursor }),
              limit: 100,
              sortKey: "updated_at",
              sortDirection: "desc",
              sourceKinds: ["cli", "vscode", "appServer", "exec", "unknown"],
            }),
          );
          const archivedThreads = yield* collectPages((cursor) =>
            client.request("thread/list", {
              ...(cursor === undefined ? {} : { cursor }),
              archived: true,
              limit: 100,
              sortKey: "updated_at",
              sortDirection: "desc",
              sourceKinds: ["cli", "vscode", "appServer", "exec", "unknown"],
            }),
          );
          const nativeThreads = [
            ...activeThreads.map((native) => ({ native, archived: false })),
            ...archivedThreads.map((native) => ({ native, archived: true })),
          ];
          const loaded = new Set(
            yield* collectPages((cursor) =>
              client.request("thread/loaded/list", {
                ...(cursor === undefined ? {} : { cursor }),
                limit: 100,
              }),
            ),
          );
          for (const { native, archived } of nativeThreads) {
            if (native.ephemeral || native.parentThreadId) continue;
            if (
              !isWithinSharedRoot(
                path,
                native.cwd,
                config.sharedSessionRoot
                  ? expandHomePath(config.sharedSessionRoot, home)
                  : undefined,
              )
            ) {
              const owner = owners.get(native.id);
              if (owner !== undefined) {
                const thread = yield* store.getThread(owner);
                if (thread.historyOrigin === "native" && thread.nativeSessionScopeHidden !== true) {
                  const now = yield* DateTime.now;
                  yield* sink.write({
                    events: [
                      {
                        id: yield* ids.allocate.event({ threadId: owner }),
                        type: "thread.metadata-updated",
                        threadId: owner,
                        occurredAt: now,
                        payload: { ...thread, nativeSessionScopeHidden: true },
                      },
                    ],
                  });
                  yield* manager.detach({
                    providerSessionId: ids.derive.providerSession({
                      providerInstanceId: instance.instanceId,
                    }),
                    threadId: owner,
                    preserveNativeWork: true,
                  });
                }
              }
              continue;
            }
            yield* Effect.gen(function* () {
              const target = yield* ensureThread(native, instance.instanceId, owners, archived);
              if (target === undefined || target.appThread.deletedAt !== null) return;
              if (target.appThread.nativeSessionScopeHidden === true) {
                const now = yield* DateTime.now;
                target.appThread = {
                  ...target.appThread,
                  nativeSessionScopeHidden: false,
                };
                yield* sink.write({
                  events: [
                    {
                      id: yield* ids.allocate.event({ threadId: target.appThread.id }),
                      type: "thread.metadata-updated",
                      threadId: target.appThread.id,
                      occurredAt: now,
                      payload: target.appThread,
                    },
                  ],
                });
              }
              const key = `${instance.instanceId}:${native.id}`;
              const revision = `${native.updatedAt}:${native.name ?? ""}:${native.preview}`;
              // Native timestamps have second precision, so periodically reconcile
              // unchanged summaries too (two quick CLI turns can share a timestamp).
              if (revisions.get(key) !== revision || scan % 20 === 0) {
                const turns = yield* collectPages((cursor) =>
                  client.request("thread/turns/list", {
                    threadId: native.id,
                    ...(cursor === undefined ? {} : { cursor }),
                    limit: 100,
                    sortDirection: "asc",
                    itemsView: "full",
                  }),
                );
                yield* externalRuns.reconcileSnapshot({
                  threadId: target.appThread.id,
                  nativeThreadId: native.id,
                  idle: native.status.type === "idle" || native.status.type === "notLoaded",
                  turns,
                });
                const existing = yield* store.getThreadRecords(target.appThread.id, [
                  "messages",
                  "turnItems",
                  "runs",
                  "attempts",
                  "providerTurns",
                  "nodes",
                  "checkpointScopes",
                ]);
                const events = NativeCodexHistory.mapHistory(
                  {
                    thread: native,
                    turns,
                    appThread: target.appThread,
                    providerThread: target.providerThread,
                    existing,
                  },
                  ids,
                );
                if (events.length > 0) yield* sink.write({ insertHistoryOnly: true, events });
                revisions.set(key, revision);
              }
              const running = yield* store.getThreadRecords(target.appThread.id, ["runs"]);
              if (
                loaded.has(native.id) ||
                running.runs.some(
                  (run) =>
                    run.status === "running" && run.providerInstanceId === instance.instanceId,
                )
              ) {
                const runtime = yield* manager.open({
                  threadId: target.appThread.id,
                  providerSessionId: ids.derive.providerSession({
                    providerInstanceId: instance.instanceId,
                  }),
                  modelSelection: target.appThread.modelSelection,
                  runtimePolicy: {
                    cwd: native.cwd,
                    runtimeMode: target.appThread.runtimeMode,
                    interactionMode: target.appThread.interactionMode,
                  },
                  initialNativeThreadId: native.id,
                  initialProviderItemIdentityVersion: 2,
                });
                yield* runtime.resumeThread({
                  providerThread: target.providerThread,
                  threadId: target.appThread.id,
                });
              }
            }).pipe(
              Effect.catch((cause) =>
                Effect.logWarning("Could not synchronize a native Codex session", { cause }),
              ),
            );
          }
        }),
      ).pipe(
        Effect.catch((cause) =>
          Effect.logWarning("Shared Codex session discovery unavailable", {
            instanceId: instance.instanceId,
            cause,
          }),
        ),
      );
    }
  });
  return NativeCodexSessionSync.of({
    sync: sync.pipe(
      Effect.catch((cause) =>
        Effect.logWarning("Native Codex session synchronization failed", { cause }),
      ),
    ),
    start: Effect.gen(function* () {
      yield* sync.pipe(
        Effect.catch((cause) =>
          Effect.logWarning("Native Codex session synchronization failed", { cause }),
        ),
        Effect.repeat(Schedule.spaced("3 seconds")),
        forkParked,
      );
    }),
  });
});
export const layer = Layer.effect(NativeCodexSessionSync, make);
