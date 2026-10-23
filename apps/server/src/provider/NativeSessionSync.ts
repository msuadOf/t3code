import {
  type ProviderDriverKind,
  DEFAULT_MODEL,
  DEFAULT_MODEL_BY_PROVIDER,
  DEFAULT_PROVIDER_INTERACTION_MODE,
  DEFAULT_RUNTIME_MODE,
  type OrchestrationV2AppThread,
  type OrchestrationV2ProviderThread,
  type ProviderInstanceId,
  type ThreadId,
} from "@t3tools/contracts";
import { expandHomePath } from "@t3tools/provider-core/server/pathExpansion";
import * as HostProcess from "@t3tools/shared/HostProcess";
import type { NativeSession } from "@t3tools/provider-core/server/nativeSessions";
import * as IdAllocator from "@t3tools/provider-core/server/IdAllocator";
import * as Context from "effect/Context";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Path from "effect/Path";
import * as Schedule from "effect/Schedule";
import * as Scope from "effect/Scope";
import * as ExternalNativeRuns from "../orchestration-v2/ExternalNativeRuns.ts";
import * as EventSink from "../orchestration-v2/EventSink.ts";
import * as ProjectionStore from "../orchestration-v2/ProjectionStore.ts";
import * as ProviderSessionManager from "../orchestration-v2/ProviderSessionManager.ts";
import * as ProjectService from "../project/ProjectService.ts";
import * as ServerSettings from "../serverSettings.ts";
import { forkParked } from "../serverActivation.ts";
import { isWithinSharedRoot } from "./NativeCodexSessionSync.ts";
import * as NativeSessionHistory from "./NativeSessionHistory.ts";
import * as ProviderInstanceRegistry from "./ProviderInstanceRegistry.ts";

export class NativeSessionSync extends Context.Service<
  NativeSessionSync,
  {
    readonly sync: Effect.Effect<void>;
    readonly start: Effect.Effect<void, never, Scope.Scope>;
  }
>()("t3/provider/NativeSessionSync") {}

const make = Effect.gen(function* () {
  const registry = yield* ProviderInstanceRegistry.ProviderInstanceRegistry;
  const settingsService = yield* ServerSettings.ServerSettingsService;
  const projects = yield* ProjectService.ProjectService;
  const store = yield* ProjectionStore.ProjectionStoreV2;
  const sink = yield* EventSink.EventSinkV2;
  const manager = yield* ProviderSessionManager.ProviderSessionManagerV2;
  const ids = yield* IdAllocator.IdAllocatorV2;
  const externalRuns = yield* ExternalNativeRuns.ExternalNativeRuns;
  const path = yield* Path.Path;
  const home = yield* HostProcess.HomeDirectory;
  const revisions = new Map<string, Pick<NativeSession, "updatedAt" | "title" | "archived">>();
  const sourceMetadata = new Map<string, Pick<NativeSession, "title" | "archived">>();
  let scan = 0;
  const ensureThread = Effect.fn("NativeSessionSync.ensureThread")(function* (
    native: NativeSession,
    driver: ProviderDriverKind,
    instanceId: ProviderInstanceId,
    owners: Map<string, ThreadId>,
    archived: boolean,
  ) {
    const existingOwner = owners.get(native.id);
    if (existingOwner !== undefined) {
      const context = yield* store.getThreadProviderContext(existingOwner, instanceId);
      const providerThread = context.providerThreads.find(
        (value) =>
          value.nativeThreadRef?.nativeId === native.id &&
          value.driver === driver &&
          value.providerInstanceId === instanceId,
      );
      if (providerThread !== undefined) return { appThread: context.thread, providerThread };
    }
    const threadId = ids.derive.threadFromProviderThread({
      driver,
      nativeThreadId: native.id,
      providerInstanceId: instanceId,
    });
    const existing = yield* store.getThreadShell(threadId);
    if (existing !== null) {
      const context = yield* store.getThreadProviderContext(threadId, instanceId);
      const providerThread = context.providerThreads.find(
        (value) =>
          value.nativeThreadRef?.nativeId === native.id &&
          value.driver === driver &&
          value.providerInstanceId === instanceId,
      );
      if (providerThread !== undefined) return { appThread: context.thread, providerThread };
    }
    const project = (yield* projects.bootstrap({
      projectId: yield* ids.allocate.project({ fixtureName: "native-session" }),
      commandId: yield* ids.allocate.command({
        fixtureName: "native-session",
        commandName: "project.bootstrap",
      }),
      title: path.basename(native.cwd) || native.cwd,
      workspaceRoot: native.cwd,
      allowMissingWorkspaceRoot: true,
    })).project;
    const providerThreadId = ids.derive.providerThread({
      driver,
      nativeThreadId: native.id,
      providerInstanceId: instanceId,
    });
    const createdAt = DateTime.makeUnsafe(native.createdAt);
    const updatedAt = DateTime.makeUnsafe(native.updatedAt);
    const appThread: OrchestrationV2AppThread = {
      createdBy: "system",
      creationSource: "provider",
      id: threadId,
      projectId: project.id,
      title: native.title.trim() || `${driver} session`,
      providerInstanceId: instanceId,
      modelSelection: {
        instanceId,
        model: native.model || DEFAULT_MODEL_BY_PROVIDER[driver] || DEFAULT_MODEL,
      },
      runtimeMode: DEFAULT_RUNTIME_MODE,
      interactionMode: DEFAULT_PROVIDER_INTERACTION_MODE,
      branch: null,
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
      driver,
      providerInstanceId: instanceId,
      providerSessionId: null,
      appThreadId: threadId,
      ownerNodeId: null,
      nativeThreadRef: { driver, nativeId: native.id, strength: "strong" },
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
    for (const instance of yield* registry.listInstances) {
      const source = instance.nativeSessions;
      if (!instance.enabled || source === undefined) continue;
      yield* Effect.gen(function* () {
        const owners = new Map<string, ThreadId>();
        const shell = yield* store.getShellSnapshot();
        for (const thread of [...shell.threads, ...shell.archivedThreads]) {
          const context = yield* store.getThreadProviderContext(thread.id, instance.instanceId);
          for (const pt of context.providerThreads)
            if (
              pt.providerInstanceId === instance.instanceId &&
              pt.driver === instance.driverKind &&
              pt.nativeThreadRef?.nativeId != null
            )
              owners.set(pt.nativeThreadRef.nativeId, thread.id);
        }
        const raw = settings.providerInstances[instance.instanceId]?.config;
        const root =
          typeof raw === "object" &&
          raw !== null &&
          "sharedSessionRoot" in raw &&
          typeof raw.sharedSessionRoot === "string"
            ? expandHomePath(raw.sharedSessionRoot.trim(), home)
            : undefined;
        for (const native of yield* source.list) {
          yield* Effect.gen(function* () {
            const owner = owners.get(native.id);
            const visible = isWithinSharedRoot(path, native.cwd, root);
            if (!visible && owner === undefined) return;
            const target = yield* ensureThread(
              native,
              instance.driverKind,
              instance.instanceId,
              owners,
              native.archived,
            );
            if (target === undefined || target.appThread.deletedAt !== null) return;
            const key = `${instance.instanceId}:${native.id}`;
            const imported = target.appThread.historyOrigin === "native";
            const previousMetadata = sourceMetadata.get(key);
            // A first observation establishes the source baseline. Only later
            // native changes replace metadata; unchanged source values must not
            // overwrite a title or archive action made in T3.
            if (imported) {
              const titleChanged =
                previousMetadata !== undefined && previousMetadata.title !== native.title;
              const archiveChanged =
                previousMetadata !== undefined && previousMetadata.archived !== native.archived;
              // Claude's existing worker owns model selection. Its persisted replies
              // must also correct stale/default selections after a server restart.
              const model =
                instance.driverKind === "claudeAgent" &&
                native.model !== undefined &&
                native.model.length > 0
                  ? native.model
                  : target.appThread.modelSelection.model;
              const modelChanged = target.appThread.modelSelection.model !== model;
              const title = titleChanged
                ? native.title.trim() || `${instance.driverKind} session`
                : target.appThread.title;
              const archiveStateChanged =
                archiveChanged && (target.appThread.archivedAt !== null) !== native.archived;
              if (title !== target.appThread.title || archiveStateChanged || modelChanged) {
                const now = yield* DateTime.now;
                target.appThread = {
                  ...target.appThread,
                  title,
                  modelSelection: modelChanged
                    ? { ...target.appThread.modelSelection, model }
                    : target.appThread.modelSelection,
                  archivedAt: archiveStateChanged
                    ? native.archived
                      ? now
                      : null
                    : target.appThread.archivedAt,
                  updatedAt: now,
                };
                yield* sink.write({
                  events: [
                    {
                      id: yield* ids.allocate.event({ threadId: target.appThread.id }),
                      type: archiveStateChanged
                        ? native.archived
                          ? "thread.archived"
                          : "thread.unarchived"
                        : "thread.metadata-updated",
                      threadId: target.appThread.id,
                      providerInstanceId: instance.instanceId,
                      occurredAt: now,
                      payload: target.appThread,
                    },
                  ],
                });
              }
            }
            sourceMetadata.set(key, { title: native.title, archived: native.archived });
            const hidden = target.appThread.nativeSessionScopeHidden === true;
            if (imported && hidden === visible) {
              target.appThread = { ...target.appThread, nativeSessionScopeHidden: !visible };
              yield* sink.write({
                events: [
                  {
                    id: yield* ids.allocate.event({ threadId: target.appThread.id }),
                    type: "thread.metadata-updated",
                    threadId: target.appThread.id,
                    occurredAt: yield* DateTime.now,
                    payload: target.appThread,
                  },
                ],
              });
            }
            if (!visible) {
              revisions.delete(key);
              if (imported && !hidden)
                yield* manager.detach({
                  providerSessionId: ids.derive.providerSession({
                    providerInstanceId: instance.instanceId,
                  }),
                  threadId: target.appThread.id,
                  preserveNativeWork: true,
                });
              return;
            }
            const previous = revisions.get(key);
            const changed =
              previous === undefined ||
              previous.updatedAt !== native.updatedAt ||
              previous.title !== native.title ||
              previous.archived !== native.archived;
            // Active sessions need live reconciliation. Periodic full refreshes
            // cover providers whose summary timestamps do not change per item.
            if (native.active || changed || scan % 20 === 0) {
              const turns = yield* source.history(native.id);
              yield* externalRuns.reconcileSnapshot({
                driver: instance.driverKind,
                providerInstanceId: instance.instanceId,
                threadId: target.appThread.id,
                nativeThreadId: native.id,
                idle: !native.active,
                turns: turns.map((turn) => ({ id: turn.id, status: turn.status })),
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
              const events = NativeSessionHistory.mapHistory(
                {
                  driver: instance.driverKind,
                  thread: native,
                  turns,
                  appThread: target.appThread,
                  providerThread: target.providerThread,
                  existing,
                },
                ids,
              );
              if (events.length > 0) yield* sink.write({ insertHistoryOnly: true, events });
              revisions.set(key, {
                updatedAt: native.updatedAt,
                title: native.title,
                archived: native.archived,
              });
            }
            // Only providers that can attach the existing worker may observe live work.
            if (
              native.active &&
              (instance.driverKind === "opencode" || native.canObserveActive === true)
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
            Effect.catch(() =>
              Effect.logWarning("Could not synchronize a native session", {
                instanceId: instance.instanceId,
                driver: instance.driverKind,
              }),
            ),
          );
        }
      }).pipe(
        Effect.catch(() =>
          Effect.logWarning("Native session discovery unavailable", {
            instanceId: instance.instanceId,
            driver: instance.driverKind,
          }),
        ),
      );
    }
  });
  const safeSync = sync.pipe(
    Effect.catch(() => Effect.logWarning("Native session synchronization unavailable")),
  );
  return NativeSessionSync.of({
    sync: safeSync,
    start: safeSync.pipe(Effect.repeat(Schedule.spaced("3 seconds")), forkParked, Effect.asVoid),
  });
});
export const layer = Layer.effect(NativeSessionSync, make);
