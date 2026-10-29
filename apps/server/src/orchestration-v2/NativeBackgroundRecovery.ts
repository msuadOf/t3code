import { isOrchestrationV2WorkActive } from "@t3tools/contracts";
import { nativeSessionProjectionId } from "@t3tools/provider-core/server/nativeSessions";
import type * as ProviderAdapter from "@t3tools/provider-core/server/ProviderAdapter";
import type * as IdAllocator from "@t3tools/provider-core/server/IdAllocator";
import * as Effect from "effect/Effect";
import * as Scope from "effect/Scope";
import * as Stream from "effect/Stream";
import * as KeyedLock from "@t3tools/shared/KeyedLock";
import type * as ProjectionStore from "./ProjectionStore.ts";
import type * as ProviderEventIngestor from "./ProviderEventIngestor.ts";

type RecoveryInput = ProviderAdapter.ProviderAdapterV2NativeBackgroundTurnInput & {
  readonly session: ProviderAdapter.ProviderAdapterV2SessionRuntime;
};
export const makeNativeBackgroundRecovery = Effect.fn("NativeBackgroundRecovery.make")(
  function* (input: {
    readonly ids: IdAllocator.IdAllocatorV2["Service"];
    readonly store: Pick<ProjectionStore.ProjectionStoreV2["Service"], "getThreadProjection">;
    readonly ingestor: Pick<
      ProviderEventIngestor.ProviderEventIngestorV2["Service"],
      "ingestNormalized"
    >;
    readonly isObserved: (
      session: ProviderAdapter.ProviderAdapterV2SessionRuntime,
      runId: string,
    ) => boolean;
  }) {
    const scope = yield* Scope.Scope;
    const locks = yield* KeyedLock.make<string>();
    type Route = {
      allowed: Map<string, { noticeId: string; parentId: string }>;
      pending: Set<string>;
    };
    const observers = new WeakMap<
      ProviderAdapter.ProviderAdapterV2SessionRuntime,
      Map<string, Route>
    >();
    return Effect.fn("NativeBackgroundRecovery.recover")(function* (request: RecoveryInput) {
      return yield* locks.withLock(
        String(request.threadId),
        Effect.gen(function* () {
          if (!request.session.subscribeEvents) return undefined;
          const projection = yield* input.store.getThreadProjection(request.threadId);
          const thread = projection.providerThreads.find(
            (t) =>
              t.appThreadId === request.threadId &&
              t.driver === request.session.driver &&
              t.providerInstanceId === request.session.instanceId &&
              t.nativeThreadRef?.nativeId === request.nativeThreadId,
          );
          if (!thread) return undefined;
          const derivedTurn = input.ids.derive.providerTurn({
            driver: request.session.driver,
            nativeTurnId: nativeSessionProjectionId(thread, request.nativeTurnId),
          });
          const providerTurn = projection.providerTurns.find(
            (t) =>
              t.providerThreadId === thread.id &&
              (t.id === derivedTurn || t.nativeTurnRef?.nativeId === request.nativeTurnId),
          );
          if (!providerTurn) return undefined;
          const attempt = projection.attempts.find(
            (a) => a.id === providerTurn.runAttemptId && a.providerThreadId === thread.id,
          );
          const run =
            attempt &&
            projection.runs.find(
              (r) =>
                r.id === attempt.runId &&
                r.threadId === request.threadId &&
                r.providerThreadId === thread.id &&
                r.providerInstanceId === request.session.instanceId &&
                r.rootNodeId === attempt.rootNodeId,
            );
          if (
            !run ||
            !attempt ||
            run.status === "rolled_back" ||
            run.rootNodeId === null ||
            providerTurn.nodeId !== run.rootNodeId
          )
            return undefined;
          const allowed = new Map<string, { noticeId: string; parentId: string }>();
          const tasks = request.tasks
            .filter((task) => {
              const nativeId = nativeSessionProjectionId(
                thread,
                `background-command:${request.nativeThreadId}:${task.nativeToolId}:${task.nativeTaskId}`,
              );
              const id = input.ids.derive.turnItemFromProviderItem({
                driver: request.session.driver,
                nativeItemId: nativeId,
              });
              const persisted = projection.turnItems.find(
                (i) =>
                  i.id === id &&
                  i.threadId === request.threadId &&
                  i.runId === run.id &&
                  i.providerThreadId === thread.id &&
                  i.providerTurnId === providerTurn.id &&
                  i.nodeId === run.rootNodeId &&
                  i.type === "command_execution",
              );
              if (persisted && !isOrchestrationV2WorkActive(persisted.status)) return false;
              if (
                task.status !== "running" &&
                (!persisted || !isOrchestrationV2WorkActive(persisted.status))
              )
                return false;
              allowed.set(id, {
                noticeId: input.ids.derive.turnItemFromProviderItem({
                  driver: request.session.driver,
                  nativeItemId: `${nativeId}:output-unavailable`,
                }),
                parentId: input.ids.derive.turnItemFromProviderItem({
                  driver: request.session.driver,
                  nativeItemId: nativeSessionProjectionId(thread, task.nativeToolId),
                }),
              });
              return true;
            })
            .map(({ nativeToolId, nativeTaskId }) => ({ nativeToolId, nativeTaskId }));
          if (tasks.length === 0) return undefined;
          const message = projection.messages.find((m) => m.id === run.userMessageId);
          const owner: ProviderAdapter.ProviderAdapterV2NativeBackgroundTurnOwner = {
            providerTurnId: providerTurn.id,
            turn: {
              appThread: projection.thread,
              threadId: request.threadId,
              providerThread: thread,
              runId: run.id,
              runOrdinal: run.ordinal,
              providerTurnOrdinal: providerTurn.ordinal,
              attemptId: attempt.id,
              rootNodeId: run.rootNodeId,
              message: {
                messageId: run.userMessageId,
                text: message?.text ?? "",
                attachments: message?.attachments ?? [],
                createdBy: "user",
                creationSource: "provider",
              },
              modelSelection: run.modelSelection,
              runtimePolicy: {
                cwd:
                  projection.checkpointScopes.find((scope) => scope.nodeId === run.rootNodeId)
                    ?.cwd ??
                  projection.thread.worktreePath ??
                  request.session.providerSession.cwd,
                runtimeMode: projection.thread.runtimeMode,
                interactionMode: projection.thread.interactionMode,
              },
            },
            tasks,
          };
          if (input.isObserved(request.session, run.id)) return owner;
          let sessionObservers = observers.get(request.session);
          if (!sessionObservers) {
            sessionObservers = new Map();
            observers.set(request.session, sessionObservers);
          }
          const existing = sessionObservers.get(run.id);
          if (existing) {
            for (const [id, route] of allowed) {
              if (!existing.allowed.has(id)) {
                existing.allowed.set(id, route);
                existing.pending.add(id);
              }
            }
            return owner;
          }
          const subscription = yield* request.session.subscribeEvents;
          const route: Route = { allowed, pending: new Set(allowed.keys()) };
          sessionObservers.set(run.id, route);
          const currentObservers = sessionObservers;
          yield* subscription.events.pipe(
            Stream.mapEffect((event) =>
              Effect.gen(function* () {
                if (event.type !== "turn_item.updated" || event.driver !== request.session.driver)
                  return;
                const item = event.turnItem;
                if (
                  item.threadId !== request.threadId ||
                  item.runId !== run.id ||
                  item.providerThreadId !== thread.id ||
                  item.providerTurnId !== providerTurn.id ||
                  item.nodeId !== run.rootNodeId
                )
                  return;
                const direct = route.allowed.get(item.id);
                const notice = [...route.allowed].find(([, r]) => r.noticeId === item.id);
                if (
                  direct
                    ? item.type !== "command_execution" || item.parentItemId !== direct.parentId
                    : !notice || item.type !== "system_notice" || item.parentItemId !== notice[0]
                )
                  return;
                yield* input.ingestor.ingestNormalized({
                  providerSessionId: request.session.providerSessionId,
                  providerInstanceId: request.session.instanceId,
                  threadId: request.threadId,
                  event,
                });
                if (direct && !isOrchestrationV2WorkActive(item.status))
                  route.pending.delete(item.id);
              }),
            ),
            Stream.takeUntil(() => route.pending.size === 0),
            Stream.runDrain,
            Effect.ensuring(subscription.close),
            Effect.ensuring(
              Effect.sync(() => {
                if (currentObservers.get(run.id) === route) currentObservers.delete(run.id);
              }),
            ),
            Effect.catchCause((cause) =>
              Effect.logWarning("Native background recovery observer ended", cause),
            ),
            Effect.forkIn(scope),
          );
          return owner;
        }),
      );
    });
  },
);
