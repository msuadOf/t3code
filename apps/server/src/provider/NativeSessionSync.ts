import {
  NativeSessionSharingError,
  type NativeSessionSharingListInput,
  type NativeSessionSharingListResult,
  type NativeSessionSharingOpenInput,
  type NativeSessionSharingOpenResult,
  type ProviderDriverKind,
  DEFAULT_MODEL,
  DEFAULT_MODEL_BY_PROVIDER,
  DEFAULT_PROVIDER_INTERACTION_MODE,
  DEFAULT_RUNTIME_MODE,
  type OrchestrationV2AppThread,
  type OrchestrationV2ProviderThread,
  type ProviderInstanceId,
  type ProviderSessionId,
  type ThreadId,
} from "@t3tools/contracts";
import { expandHomePath } from "@t3tools/provider-core/server/pathExpansion";
import * as McpProviderSessions from "@t3tools/provider-core/server/McpProviderSessions";
import * as HostProcess from "@t3tools/shared/HostProcess";
import {
  nativeSessionProjectionId,
  type NativeSession,
} from "@t3tools/provider-core/server/nativeSessions";
import * as IdAllocator from "@t3tools/provider-core/server/IdAllocator";
import * as Context from "effect/Context";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Path from "effect/Path";
import * as Option from "effect/Option";
import * as Stream from "effect/Stream";
import { nativeSessionSyncTriggers } from "./NativeSessionWakeups.ts";
import * as Scope from "effect/Scope";
import * as Semaphore from "effect/Semaphore";
import * as Schema from "effect/Schema";
import * as ExternalNativeRuns from "../orchestration-v2/ExternalNativeRuns.ts";
import * as McpSessionRegistry from "../mcp/McpSessionRegistry.ts";
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
    readonly list: (
      input: NativeSessionSharingListInput,
    ) => Effect.Effect<NativeSessionSharingListResult, NativeSessionSharingError>;
    readonly open: (
      input: NativeSessionSharingOpenInput,
    ) => Effect.Effect<NativeSessionSharingOpenResult, NativeSessionSharingError>;
    readonly start: Effect.Effect<void, never, Scope.Scope>;
  }
>()("t3/provider/NativeSessionSync") {}

const isSharingError = Schema.is(NativeSessionSharingError);

const make = Effect.gen(function* () {
  const registry = yield* ProviderInstanceRegistry.ProviderInstanceRegistry;
  const settingsService = yield* ServerSettings.ServerSettingsService;
  const projects = yield* ProjectService.ProjectService;
  const store = yield* ProjectionStore.ProjectionStoreV2;
  const sink = yield* EventSink.EventSinkV2;
  const manager = yield* ProviderSessionManager.ProviderSessionManagerV2;
  const mcpSessions = yield* McpProviderSessions.McpProviderSessions;
  const mcpRegistry = yield* McpSessionRegistry.McpSessionRegistry;
  const ids = yield* IdAllocator.IdAllocatorV2;
  const externalRuns = yield* ExternalNativeRuns.ExternalNativeRuns;
  const path = yield* Path.Path;
  const home = yield* HostProcess.HomeDirectory;
  const revisions = new Map<string, Pick<NativeSession, "updatedAt" | "title" | "archived">>();
  const sourceMetadata = new Map<string, Pick<NativeSession, "title" | "archived">>();
  let scan = 0;
  const mutation = yield* Semaphore.make(1);
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
      if (providerThread !== undefined)
        return { appThread: context.thread, providerThread, created: false };
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
      if (providerThread !== undefined)
        return { appThread: context.thread, providerThread, created: false };
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
      titleSource: "native",
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
    return { appThread, providerThread, created: true };
  });

  const sync = Effect.gen(function* () {
    scan += 1;
    const settings = yield* settingsService.getSettings;
    const instances = yield* registry.listInstances;
    // Lost native observers leave active children even when the native summary is unchanged.
    const recoveryThreadIds = new Set(
      instances.some((instance) => instance.enabled && instance.nativeSessions !== undefined)
        ? yield* store.getRecoveryThreadIds("runtime")
        : [],
    );
    for (const instance of instances) {
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
        const sessions = [...(yield* source.list)];
        if (instance.driverKind === "claudeAgent") {
          // The native daemon can forget a stopped job while its JSONL and T3 run
          // remain. Reconcile only known owners; never import ordinary CLI history.
          const discovered = new Set(sessions.map((session) => session.id));
          for (const [nativeId, threadId] of owners) {
            if (discovered.has(nativeId)) continue;
            const records = yield* store.getThreadRecords(threadId, ["runs"]);
            if (!records.runs.some((run) => run.status === "running" || run.status === "starting"))
              continue;
            const context = yield* store.getThreadProviderContext(threadId, instance.instanceId);
            const project = Option.getOrUndefined(
              yield* projects.getById(context.thread.projectId),
            );
            const cwd =
              context.thread.worktreePath ??
              context.providerSessions[0]?.cwd ??
              project?.workspaceRoot;
            if (!cwd) continue;
            sessions.push({
              id: nativeId,
              cwd,
              title: context.thread.title,
              createdAt: DateTime.toEpochMillis(context.thread.createdAt),
              updatedAt: DateTime.toEpochMillis(yield* DateTime.now),
              archived: context.thread.archivedAt !== null,
              active: false,
              canObserveActive: true,
            });
          }
        }
        for (const native of sessions) {
          yield* Effect.gen(function* () {
            const owner = owners.get(native.id);
            if (
              native.requiresExistingOwner !== undefined &&
              owner !== native.requiresExistingOwner.threadId
            )
              return;
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
            if (
              imported ||
              (instance.driverKind === "claudeAgent" &&
                target.appThread.historyOrigin !== "v1_import")
            ) {
              const titleChanged =
                instance.driverKind === "claudeAgent"
                  ? target.appThread.titleSource === "native" &&
                    target.appThread.title !== native.title
                  : previousMetadata !== undefined && previousMetadata.title !== native.title;
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
                  ...(titleChanged && instance.driverKind === "claudeAgent"
                    ? { titleSource: "native" as const }
                    : {}),
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
            let recoverRunningRun = false;
            if (
              native.active ||
              native.hasPendingBackgroundWork ||
              recoveryThreadIds.has(target.appThread.id) ||
              changed ||
              scan % 20 === 0
            ) {
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
              recoverRunningRun =
                (instance.driverKind === "opencode" || instance.driverKind === "claudeAgent") &&
                (existing.runs.some(
                  (run) =>
                    run.providerThreadId === target.providerThread.id && run.status === "running",
                ) ||
                  existing.turnItems.some(
                    (item) =>
                      item.providerThreadId === target.providerThread.id &&
                      item.type === "command_execution" &&
                      item.parentItemId !== null &&
                      item.nativeItemRef?.driver === instance.driverKind &&
                      item.nativeItemRef.nativeId?.startsWith(
                        nativeSessionProjectionId(
                          target.providerThread,
                          `background-command:${native.id}:`,
                        ),
                      ) &&
                      (item.status === "running" || item.status === "waiting"),
                  ));

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
              (native.active || native.hasPendingBackgroundWork || recoverRunningRun) &&
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
                providerThread: recoverRunningRun
                  ? { ...target.providerThread, status: "active" }
                  : target.providerThread,
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
    mutation.withPermits(1),
    Effect.catch(() => Effect.logWarning("Native session synchronization unavailable")),
  );
  const sharingSource = Effect.fn("NativeSessionSync.sharingSource")(function* (
    instanceId: ProviderInstanceId,
  ) {
    const instance = yield* registry.getInstance(instanceId);
    const source = instance?.nativeSessions?.sharing;
    if (!instance?.enabled || !source)
      return yield* new NativeSessionSharingError({ reason: "unavailable" });
    const settings = yield* settingsService.getSettings;
    const raw = settings.providerInstances[instanceId]?.config;
    const root =
      raw &&
      typeof raw === "object" &&
      "sharedSessionRoot" in raw &&
      typeof raw.sharedSessionRoot === "string"
        ? expandHomePath(raw.sharedSessionRoot.trim(), home)
        : undefined;
    return { instance, source, root };
  });
  const list = Effect.fn("NativeSessionSync.list")(
    function* (input: NativeSessionSharingListInput) {
      const { source, root } = yield* sharingSource(input.instanceId);
      const candidates = yield* source.list;
      const query = input.query?.trim().toLocaleLowerCase() ?? "";
      const filtered = candidates
        .filter(
          (session) =>
            isWithinSharedRoot(path, session.cwd, root) &&
            (!query ||
              `${session.title} ${session.cwd} ${session.id}`.toLocaleLowerCase().includes(query)),
        )
        .sort((a, b) => b.updatedAt - a.updatedAt || a.id.localeCompare(b.id));
      const offset = input.offset ?? 0;
      return {
        sessions: filtered.slice(offset, offset + 30).map((session) => ({
          sessionId: session.id,
          title: session.title,
          cwd: session.cwd,
          updatedAt: session.updatedAt,
          historyAvailable: session.historyAvailable !== false,
          ...(session.modelActivityAvailable === undefined
            ? {}
            : { modelActivityAvailable: session.modelActivityAvailable }),
          ...(session.modelActivityEnabled === undefined
            ? {}
            : { modelActivityEnabled: session.modelActivityEnabled }),
          active: session.active,
          state: session.sharingState,
        })),
        nextOffset: offset + 30 < filtered.length ? offset + 30 : null,
      };
    },
    Effect.mapError((cause) =>
      isSharingError(cause) ? cause : new NativeSessionSharingError({ reason: "list-failed" }),
    ),
  );
  const open = Effect.fn("NativeSessionSync.open")(
    function* (input: NativeSessionSharingOpenInput) {
      return yield* Effect.gen(function* () {
        const { instance, source, root } = yield* sharingSource(input.instanceId);
        const candidate = (yield* source.list).find((session) => session.id === input.sessionId);
        if (!candidate) return yield* new NativeSessionSharingError({ reason: "unavailable" });
        if (!isWithinSharedRoot(path, candidate.cwd, root))
          return yield* new NativeSessionSharingError({ reason: "outside-root" });
        if (candidate.sharingState === "foreground")
          return yield* new NativeSessionSharingError({ reason: "foreground" });
        if (
          input.modelActivityAction &&
          (input.observeModelActivity !== undefined ||
            candidate.active ||
            !source.setModelActivityEnabled)
        )
          return yield* new NativeSessionSharingError({ reason: "open-failed" });
        const readOwners = Effect.gen(function* () {
          const owners = new Map<string, ThreadId>();
          const shell = yield* store.getShellSnapshot();
          for (const thread of [...shell.threads, ...shell.archivedThreads]) {
            const context = yield* store.getThreadProviderContext(thread.id, instance.instanceId);
            for (const pt of context.providerThreads)
              if (
                pt.providerInstanceId === instance.instanceId &&
                pt.driver === instance.driverKind &&
                pt.nativeThreadRef?.nativeId
              )
                owners.set(pt.nativeThreadRef.nativeId, thread.id);
          }
          return owners;
        });
        let preparedTarget: Effect.Success<ReturnType<typeof ensureThread>>;
        let openedProviderSessionId: ProviderSessionId | undefined;
        let issuedCredentialId: string | undefined;
        let preserveExistingCaller = false;
        return yield* Effect.gen(function* () {
          if (
            instance.driverKind === "claudeAgent" &&
            candidate.sharingState === "history" &&
            !input.modelActivityAction
          ) {
            preparedTarget = yield* ensureThread(
              candidate,
              instance.driverKind,
              instance.instanceId,
              yield* readOwners,
              false,
            );
            if (!preparedTarget || preparedTarget.appThread.deletedAt !== null)
              return yield* new NativeSessionSharingError({ reason: "open-failed" });
            const previousCredential = yield* mcpSessions.read(preparedTarget.appThread.id);
            const existingWork = yield* store.getThreadRecords(preparedTarget.appThread.id, [
              "runs",
            ]);
            if (
              existingWork.runs.some(
                (run) => run.status === "running" || run.status === "starting",
              ) ||
              (previousCredential !== undefined &&
                previousCredential.providerInstanceId !== instance.instanceId)
            ) {
              // Do not let manager.open rotate a different provider's live credential.
              preserveExistingCaller = true;
              return yield* new NativeSessionSharingError({ reason: "open-failed" });
            }
            const providerSessionId = ids.derive.providerSession({
              providerInstanceId: instance.instanceId,
            });
            const runtime = yield* manager.open({
              threadId: preparedTarget.appThread.id,
              providerSessionId,
              modelSelection: {
                instanceId: instance.instanceId,
                model:
                  candidate.model ||
                  DEFAULT_MODEL_BY_PROVIDER[instance.driverKind] ||
                  DEFAULT_MODEL,
              },
              runtimePolicy: {
                cwd: candidate.cwd,
                runtimeMode: preparedTarget.appThread.runtimeMode,
                interactionMode: preparedTarget.appThread.interactionMode,
              },
              initialNativeThreadId: candidate.id,
              initialProviderItemIdentityVersion: 2,
            });
            openedProviderSessionId = runtime.providerSessionId ?? providerSessionId;
            const currentCredential = yield* mcpSessions.read(preparedTarget.appThread.id);
            if (currentCredential?.providerSessionId !== previousCredential?.providerSessionId)
              issuedCredentialId = currentCredential?.providerSessionId;
          }
          const shared =
            input.modelActivityAction && source.setModelActivityEnabled
              ? yield* source.setModelActivityEnabled(
                  input.sessionId,
                  input.modelActivityAction === "enable",
                )
              : yield* source.open(
                  input.sessionId,
                  input.observeModelActivity === undefined && !preparedTarget
                    ? undefined
                    : {
                        ...(input.observeModelActivity === undefined
                          ? {}
                          : { observeModelActivity: input.observeModelActivity }),
                        ...(preparedTarget ? { threadId: preparedTarget.appThread.id } : {}),
                      },
                );
          if (shared.id !== input.sessionId || !isWithinSharedRoot(path, shared.cwd, root))
            return yield* new NativeSessionSharingError({ reason: "open-failed" });
          const target =
            preparedTarget ??
            (yield* ensureThread(
              shared,
              instance.driverKind,
              instance.instanceId,
              yield* readOwners,
              false,
            ));
          if (!target || target.appThread.deletedAt !== null)
            return yield* new NativeSessionSharingError({ reason: "open-failed" });
          if (target.appThread.archivedAt !== null) {
            const now = yield* DateTime.now;
            yield* sink.write({
              events: [
                {
                  id: yield* ids.allocate.event({ threadId: target.appThread.id }),
                  type: "thread.unarchived",
                  threadId: target.appThread.id,
                  providerInstanceId: instance.instanceId,
                  occurredAt: now,
                  payload: { ...target.appThread, archivedAt: null, updatedAt: now },
                },
              ],
            });
          }
          if (input.modelActivityAction) {
            const current = yield* store.getThread(target.appThread.id);
            const now = yield* DateTime.now;
            yield* sink.write({
              events: [
                {
                  id: yield* ids.allocate.event({ threadId: current.id }),
                  type: "thread.metadata-updated",
                  threadId: current.id,
                  providerInstanceId: instance.instanceId,
                  occurredAt: now,
                  payload: { ...current, updatedAt: now },
                },
              ],
            });
          }
          return { threadId: target.appThread.id };
        }).pipe(
          Effect.onError(() =>
            Effect.gen(function* () {
              if (!preparedTarget || preserveExistingCaller) return;
              const threadId = preparedTarget.appThread.id;
              const records = yield* store.getThreadRecords(threadId, ["runs"]);
              // A caller can reuse this credential while native open is pending. Protect it
              // before any revocation, detach or shell update, including the same generation.
              if (records.runs.some((run) => run.status === "running" || run.status === "starting"))
                return;
              if (issuedCredentialId !== undefined) {
                yield* mcpRegistry.revokeProviderSession(issuedCredentialId);
                yield* mcpSessions.clear(threadId, issuedCredentialId);
              }
              if (
                openedProviderSessionId !== undefined &&
                (preparedTarget.created || issuedCredentialId !== undefined)
              )
                yield* manager.detach({
                  providerSessionId: openedProviderSessionId,
                  threadId,
                  preserveNativeWork: true,
                });
              if (!preparedTarget.created) return;
              const current = yield* store.getThread(threadId);
              if (current.deletedAt !== null) return;
              if (current.archivedAt === null) {
                const now = yield* DateTime.now;
                yield* sink.write({
                  events: [
                    {
                      id: yield* ids.allocate.event({ threadId: current.id }),
                      type: "thread.archived",
                      threadId: current.id,
                      providerInstanceId: instance.instanceId,
                      occurredAt: now,
                      payload: { ...current, archivedAt: now, updatedAt: now },
                    },
                  ],
                });
              }
            }).pipe(Effect.ignore),
          ),
        );
      }).pipe(mutation.withPermits(1));
    },
    Effect.tap(() => safeSync),
    Effect.mapError((cause) =>
      isSharingError(cause) ? cause : new NativeSessionSharingError({ reason: "open-failed" }),
    ),
  );
  return NativeSessionSync.of({
    list,
    open,
    sync: safeSync,
    start: nativeSessionSyncTriggers(registry).pipe(
      Stream.runForEach(() => safeSync),
      forkParked,
      Effect.asVoid,
    ),
  });
});
export const layer = Layer.effect(NativeSessionSync, make);
