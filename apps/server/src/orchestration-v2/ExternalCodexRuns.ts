import {
  CommandId,
  type ThreadId,
  ProviderDriverKind,
  MessageId,
  RunId,
  type OrchestrationV2DomainEvent,
  type OrchestrationV2CheckpointScope,
  type OrchestrationV2ExecutionNode,
  type OrchestrationV2Run,
  type OrchestrationV2RunAttempt,
} from "@t3tools/contracts";
import * as Context from "effect/Context";
import * as DateTime from "effect/DateTime";
import { makeProviderFailure } from "@t3tools/provider-core/server/failure";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as KeyedLock from "@t3tools/shared/KeyedLock";
import * as ProviderAdapter from "@t3tools/provider-core/server/ProviderAdapter";
import * as IdAllocator from "@t3tools/provider-core/server/IdAllocator";
import * as EventSink from "./EventSink.ts";
import * as ProjectionStore from "./ProjectionStore.ts";
import * as RunExecutionService from "./RunExecutionService.ts";

export class ExternalCodexRuns extends Context.Service<
  ExternalCodexRuns,
  {
    readonly reconcileSnapshot: (input: {
      readonly threadId: ThreadId;
      readonly nativeThreadId: string;
      readonly idle: boolean;
      readonly turns: ReadonlyArray<{ readonly id: string; readonly status: string }>;
    }) => Effect.Effect<void, ProviderAdapter.ProviderAdapterProtocolError>;
    readonly adopt: (
      input: ProviderAdapter.ProviderAdapterV2ExternalTurnInput & {
        readonly session: ProviderAdapter.ProviderAdapterV2SessionRuntime;
      },
    ) => Effect.Effect<
      ProviderAdapter.ProviderAdapterV2TurnInput | undefined,
      ProviderAdapter.ProviderAdapterProtocolError
    >;
  }
>()("t3/orchestration-v2/ExternalCodexRuns") {}

const make = Effect.gen(function* () {
  const store = yield* ProjectionStore.ProjectionStoreV2;
  const sink = yield* EventSink.EventSinkV2;
  const ids = yield* IdAllocator.IdAllocatorV2;
  const execution = yield* RunExecutionService.RunExecutionServiceV2;
  const locks = yield* KeyedLock.make<string>();
  const startupBoundary = yield* DateTime.now;
  const observing = new WeakMap<ProviderAdapter.ProviderAdapterV2SessionRuntime, Set<string>>();

  const adopt = Effect.fn("ExternalCodexRuns.adopt")(function* (
    input: ProviderAdapter.ProviderAdapterV2ExternalTurnInput & {
      readonly session: ProviderAdapter.ProviderAdapterV2SessionRuntime;
    },
  ) {
    return yield* locks.withLock(
      String(input.threadId),
      Effect.gen(function* () {
        const projection = yield* store.getThreadProjection(input.threadId);
        const providerThread = projection.providerThreads.find(
          (thread) =>
            thread.nativeThreadRef?.nativeId === input.nativeThreadId &&
            thread.providerInstanceId === input.session.instanceId,
        );
        if (providerThread === undefined) return undefined;
        const providerTurnId = ids.derive.providerTurn({
          driver: input.session.driver,
          nativeTurnId: input.nativeTurnId,
        });
        const existingTurn = projection.providerTurns.find((turn) => turn.id === providerTurnId);
        const nativeRunId = RunId.make(
          `run:codex-external:${encodeURIComponent(providerThread.id)}:${encodeURIComponent(input.nativeTurnId)}`,
        );
        const previousRun = projection.runs.find(
          (run) =>
            run.id === nativeRunId ||
            (existingTurn !== undefined &&
              projection.attempts.some(
                (attempt) => attempt.runId === run.id && attempt.id === existingTurn.runAttemptId,
              )),
        );
        // Completed history is imported separately; an old notification must not restart it.
        const nativeRunning = (input.nativeStatus ?? "inProgress") === "inProgress";
        if (
          !nativeRunning &&
          (previousRun === undefined ||
            !["preparing", "starting", "running", "waiting"].includes(previousRun.status))
        )
          return undefined;
        if (previousRun !== undefined && previousRun.status !== "running" && !input.snapshot)
          return undefined;
        const ordinal = Math.max(0, ...projection.runs.map((run) => run.ordinal)) + 1;
        const runId = previousRun?.id ?? nativeRunId;
        const rootNodeId = previousRun?.rootNodeId ?? ids.derive.rootNode({ runId });
        const attemptId =
          previousRun?.activeAttemptId ?? ids.derive.runAttempt({ runId, attemptOrdinal: 1 });
        const messageId =
          previousRun?.userMessageId ??
          MessageId.make(`message:codex-external:${encodeURIComponent(runId)}`);
        const run: OrchestrationV2Run = {
          ...(previousRun ?? {
            id: runId,
            threadId: input.threadId,
            ordinal,
            providerInstanceId: input.session.instanceId,
            modelSelection: {
              instanceId: input.session.instanceId,
              model:
                input.nativeModel ??
                input.session.providerSession.model ??
                projection.thread.modelSelection.model,
            },
            providerThreadId: providerThread.id,
            userMessageId: messageId,
            rootNodeId,
            activeAttemptId: attemptId,
            status: "running",
            requestedAt: input.startedAt,
            startedAt: input.startedAt,
            completedAt: null,
            checkpointId: null,
            contextHandoffId: null,
          }),
          status: "running",
          completedAt: null,
        };
        const attempt: OrchestrationV2RunAttempt = {
          ...(projection.attempts.find((value) => value.id === attemptId) ?? {
            id: attemptId,
            nativeThreadId: input.nativeThreadId,
            runId,
            attemptOrdinal: 1,
            rootNodeId,
            providerInstanceId: input.session.instanceId,
            providerThreadId: providerThread.id,
            providerTurnId,
            reason: "initial",
            status: "running",
            startedAt: input.startedAt,
            completedAt: null,
          }),
          status: "running",
          completedAt: null,
        };
        const checkpointScope: OrchestrationV2CheckpointScope = projection.checkpointScopes.find(
          (value) => value.nodeId === rootNodeId,
        ) ?? {
          id: yield* ids.allocate.checkpointScope({
            threadId: input.threadId,
            name: String(runId),
          }),
          threadId: input.threadId,
          runId,
          nodeId: rootNodeId,
          parentScopeId: null,
          providerThreadId: providerThread.id,
          kind: "root_run",
          ordinalWithinParent: 0,
          advancesAppRunCount: true,
          cwd: input.cwd ?? input.session.providerSession.cwd,
          createdAt: input.startedAt,
        };
        const rootNode: OrchestrationV2ExecutionNode = {
          ...(projection.nodes.find((value) => value.id === rootNodeId) ?? {
            id: rootNodeId,
            threadId: input.threadId,
            runId,
            parentNodeId: null,
            rootNodeId,
            kind: "root_turn",
            status: "running",
            countsForRun: true,
            providerThreadId: providerThread.id,
            providerTurnId,
            nativeItemRef: null,
            runtimeRequestId: null,
            checkpointScopeId: checkpointScope.id,
            startedAt: input.startedAt,
            completedAt: null,
          }),
          status: "running",
          completedAt: null,
        };
        if (previousRun === undefined) {
          const payloads = [
            { type: "run.created", payload: run },
            { type: "run-attempt.created", payload: attempt },
            { type: "node.updated", payload: rootNode },
            { type: "checkpoint-scope.created", payload: checkpointScope },
            {
              type: "turn-item.updated",
              payload: {
                id: ids.derive.userTurnItem({ messageId }),
                threadId: input.threadId,
                runId,
                nodeId: rootNodeId,
                providerThreadId: providerThread.id,
                providerTurnId,
                nativeItemRef: null,
                parentItemId: null,
                ordinal: run.ordinal * 100,
                status: "completed",
                title: null,
                startedAt: input.startedAt,
                completedAt: input.startedAt,
                updatedAt: input.startedAt,
                type: "user_message",
                messageId,
                inputIntent: "turn_start",
                text: input.text,
                attachments: [],
                createdBy: "user",
                creationSource: "provider",
              },
            },
            {
              type: "message.updated",
              payload: {
                id: messageId,
                threadId: input.threadId,
                runId,
                nodeId: rootNodeId,
                role: "user",
                text: input.text,
                attachments: [],
                streaming: false,
                createdBy: "user",
                creationSource: "provider",
                createdAt: input.startedAt,
                updatedAt: input.startedAt,
              },
            },
          ] as const;
          const events: Array<OrchestrationV2DomainEvent> = yield* Effect.forEach(
            payloads,
            (event) =>
              ids.allocate.event({ threadId: input.threadId }).pipe(
                Effect.map((id) => ({
                  ...event,
                  id,
                  threadId: input.threadId,
                  runId,
                  nodeId: rootNodeId,
                  driver: input.session.driver,
                  providerInstanceId: input.session.instanceId,
                  occurredAt: input.startedAt,
                })),
              ),
          );
          yield* sink.write({ events });
        } else if (previousRun.status !== "running") {
          const payloads = [
            { type: "run.updated", payload: run },
            { type: "run-attempt.updated", payload: attempt },
            { type: "node.updated", payload: rootNode },
          ] as const;
          const events: Array<OrchestrationV2DomainEvent> = yield* Effect.forEach(
            payloads,
            (event) =>
              ids.allocate.event({ threadId: input.threadId }).pipe(
                Effect.map((id) => ({
                  ...event,
                  id,
                  threadId: input.threadId,
                  runId,
                  nodeId: rootNodeId,
                  driver: input.session.driver,
                  providerInstanceId: input.session.instanceId,
                  occurredAt: input.startedAt,
                })),
              ),
          );
          yield* sink.write({ events });
        }
        const turnInput: ProviderAdapter.ProviderAdapterV2TurnInput = {
          appThread: projection.thread,
          threadId: input.threadId,
          runId,
          runOrdinal: run.ordinal,
          providerTurnOrdinal:
            existingTurn?.ordinal ??
            Math.max(
              0,
              ...projection.providerTurns
                .filter((turn) => turn.providerThreadId === providerThread.id)
                .map((turn) => turn.ordinal),
            ) + 1,
          attemptId,
          rootNodeId,
          providerThread,
          message: {
            messageId,
            text: input.text,
            attachments: [],
            createdBy: "user",
            creationSource: "provider",
          },
          modelSelection: run.modelSelection,
          runtimePolicy: {
            cwd: input.cwd ?? input.session.providerSession.cwd,
            runtimeMode: projection.thread.runtimeMode,
            interactionMode: projection.thread.interactionMode,
          },
        };
        let observedRuns = observing.get(input.session);
        if (observedRuns === undefined) {
          observedRuns = new Set();
          observing.set(input.session, observedRuns);
        }
        if (!observedRuns.has(runId)) {
          const currentObservedRuns = observedRuns;
          const release = Effect.sync(() => {
            currentObservedRuns.delete(runId);
          });
          observedRuns.add(runId);
          yield* execution
            .observeExternalRun({
              onObservationClosed: release,
              commandId: CommandId.make(
                `command:codex-external:${encodeURIComponent(input.nativeTurnId)}`,
              ),
              appThread: projection.thread,
              providerSessionId: input.session.providerSessionId,
              session: input.session,
              run,
              rootNode,
              checkpointScope,
              providerThread,
              attempt,
              attemptId,
              providerTurnOrdinal: turnInput.providerTurnOrdinal,
              message: turnInput.message,
              modelSelection: turnInput.modelSelection,
              runtimePolicy: turnInput.runtimePolicy,
            })
            .pipe(Effect.onError(() => release));
        }
        return turnInput;
      }),
    );
  });
  const reconcileSnapshot = Effect.fn("ExternalCodexRuns.reconcileSnapshot")(function* (
    input: Parameters<ExternalCodexRuns["Service"]["reconcileSnapshot"]>[0],
  ) {
    if (!input.idle || input.turns.some((turn) => turn.status === "inProgress")) return;
    return yield* locks.withLock(
      String(input.threadId),
      Effect.gen(function* () {
        const projection = yield* store.getThreadProjection(input.threadId);
        const providerThread = projection.providerThreads.find(
          (thread) =>
            thread.driver === "codex" && thread.nativeThreadRef?.nativeId === input.nativeThreadId,
        );
        if (providerThread === undefined) return;
        const now = yield* DateTime.now;
        for (const run of projection.runs) {
          // A run created by this process may still be preparing its first send.
          // Only an unacknowledged input left by an older process is recoverable here.
          if (
            run.status !== "starting" ||
            run.providerThreadId !== providerThread.id ||
            !DateTime.isLessThan(run.requestedAt, startupBoundary) ||
            run.activeAttemptId === null
          )
            continue;
          const attempt = projection.attempts.find((value) => value.id === run.activeAttemptId);
          if (
            attempt === undefined ||
            attempt.providerTurnId !== null ||
            projection.providerTurns.some((turn) => turn.runAttemptId === attempt.id)
          )
            continue;
          const rootNode = projection.nodes.find((node) => node.id === run.rootNodeId);
          const failure = makeProviderFailure({
            code: "manual_retry_required",
            retryable: false,
            message:
              "T3 restarted before Codex acknowledged this input. The shared daemon is idle. Review the conversation and retry manually if needed; T3 has not replayed the input.",
          });
          const payloads = [
            { type: "run.updated", payload: { ...run, status: "interrupted", completedAt: now } },
            {
              type: "run-attempt.updated",
              payload: { ...attempt, status: "interrupted", completedAt: now },
            },
            ...(rootNode === undefined
              ? []
              : [
                  {
                    type: "node.updated" as const,
                    payload: { ...rootNode, status: "interrupted" as const, completedAt: now },
                  },
                ]),
            {
              type: "turn-item.updated",
              payload: {
                id: ids.derive.runSignalTurnItem({
                  runId: run.id,
                  signal: "native-unacknowledged-input",
                }),
                threadId: input.threadId,
                runId: run.id,
                nodeId: run.rootNodeId,
                providerThreadId: providerThread.id,
                providerTurnId: null,
                nativeItemRef: null,
                parentItemId: null,
                ordinal: run.ordinal * 100 + 1,
                status: "failed",
                title: "Retry required",
                startedAt: now,
                completedAt: now,
                updatedAt: now,
                type: "error",
                failure,
              },
            },
          ] as const;
          const events: Array<OrchestrationV2DomainEvent> = yield* Effect.forEach(
            payloads,
            (event) =>
              ids.allocate.event({ threadId: input.threadId }).pipe(
                Effect.map((id) => ({
                  ...event,
                  id,
                  threadId: input.threadId,
                  runId: run.id,
                  providerInstanceId: run.providerInstanceId,
                  occurredAt: now,
                })),
              ),
          );
          yield* sink.writeIfRunCurrent({
            threadId: input.threadId,
            runId: run.id,
            activeAttemptId: attempt.id,
            expectedStatus: "starting",
            events,
          });
        }
      }),
    );
  });
  return ExternalCodexRuns.of({
    reconcileSnapshot: (input) =>
      reconcileSnapshot(input).pipe(
        Effect.mapError(
          (cause) =>
            new ProviderAdapter.ProviderAdapterProtocolError({
              driver: ProviderDriverKind.make("codex"),
              detail: "Could not reconcile an unacknowledged Codex input",
              cause,
            }),
        ),
      ),
    adopt: (input) =>
      adopt(input).pipe(
        Effect.mapError(
          (cause) =>
            new ProviderAdapter.ProviderAdapterProtocolError({
              driver: input.session.driver,
              detail: "Could not attach an external native turn",
              cause,
            }),
        ),
      ),
  });
});

export const layer = Layer.effect(ExternalCodexRuns, make);
