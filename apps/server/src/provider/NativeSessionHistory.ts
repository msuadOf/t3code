import {
  EventId,
  CheckpointScopeId,
  MessageId,
  RunId,
  type OrchestrationV2AppThread,
  type OrchestrationV2ProviderThread,
  type OrchestrationV2ThreadProjection,
  type OrchestrationV2Run,
  type OrchestrationV2RunAttempt,
  type OrchestrationV2ProviderTurn,
  type OrchestrationV2ExecutionNode,
  type OrchestrationV2CheckpointScope,
  type ProviderDriverKind,
  type OrchestrationV2ConversationMessage,
  type OrchestrationV2DomainEvent,
  type OrchestrationV2TurnItem,
} from "@t3tools/contracts";
import type * as IdAllocator from "@t3tools/provider-core/server/IdAllocator";
import type {
  NativeHistoryTurn,
  NativeHistoryItem,
} from "@t3tools/provider-core/server/nativeSessions";
import * as DateTime from "effect/DateTime";

export interface NativeSessionHistoryInput {
  readonly driver: ProviderDriverKind;
  readonly thread: {
    readonly id: string;
    readonly cwd: string;
    readonly createdAt: number;
    readonly updatedAt: number;
  };
  /** All pages in chronological order. Active turns are owned by the adapter. */
  readonly turns: ReadonlyArray<NativeHistoryTurn>;
  readonly appThread: OrchestrationV2AppThread;
  readonly providerThread: OrchestrationV2ProviderThread;
  readonly existing?: Pick<
    OrchestrationV2ThreadProjection,
    "runs" | "attempts" | "providerTurns" | "nodes" | "checkpointScopes" | "messages" | "turnItems"
  >;
}

/** Materialize historical native items without replacing any live-owned record. */
export const mapHistory = (
  input: NativeSessionHistoryInput,
  idAllocator: IdAllocator.IdAllocatorV2["Service"],
): ReadonlyArray<OrchestrationV2DomainEvent> => {
  const events: Array<OrchestrationV2DomainEvent> = [];
  const driver = input.driver;
  let ordinal = 0;
  let nextRunOrdinal =
    input.existing?.runs.reduce((max, run) => Math.max(max, run.ordinal), 0) ?? 0;
  let nextProviderOrdinal =
    input.existing?.providerTurns.reduce(
      (max, turn) =>
        turn.providerThreadId === input.providerThread.id ? Math.max(max, turn.ordinal) : max,
      0,
    ) ?? 0;
  const existingMessages = new Set(input.existing?.messages.map((message) => message.id));
  const existingItems = new Map(input.existing?.turnItems.map((item) => [item.id, item]));
  for (const turn of input.turns) {
    const active = turn.status === "running";
    const lifecycleStatus = turn.status;
    const startedAt = DateTime.makeUnsafe(turn.startedAt);
    const completedAt = DateTime.makeUnsafe(turn.completedAt ?? input.thread.updatedAt);
    const nativeProviderTurnId = idAllocator.derive.providerTurn({
      driver,
      nativeTurnId: turn.id,
    });
    // T3-started turns can retain an admission ID after learning their native identity.
    const existingTurn = input.existing?.providerTurns.find(
      (value) =>
        value.providerThreadId === input.providerThread.id &&
        (value.id === nativeProviderTurnId ||
          (value.nativeTurnRef?.driver === driver && value.nativeTurnRef.nativeId === turn.id)),
    );
    const providerTurnId = existingTurn?.id ?? nativeProviderTurnId;
    const existingAttempt = input.existing?.attempts.find(
      (value) => value.id === existingTurn?.runAttemptId,
    );
    const nativeRunId = RunId.make(
      `run:${driver}-external:${encodeURIComponent(input.providerThread.id)}:${encodeURIComponent(turn.id)}`,
    );
    const existingRun = input.existing?.runs.find(
      (value) => value.id === (existingAttempt?.runId ?? nativeRunId),
    );
    const runId = existingRun?.id ?? nativeRunId;
    const rootNodeId = existingRun?.rootNodeId ?? idAllocator.derive.rootNode({ runId });
    const attemptId =
      existingAttempt?.id ?? idAllocator.derive.runAttempt({ runId, attemptOrdinal: 1 });
    // Only the adapter may adopt an active native turn. Canonical history can
    // then fill completed reasoning parts that predate our live subscription.
    if (
      active &&
      (existingRun === undefined ||
        existingAttempt === undefined ||
        existingTurn === undefined ||
        !input.existing?.nodes.some((node) => node.id === rootNodeId) ||
        !input.existing.checkpointScopes.some((scope) => scope.nodeId === rootNodeId))
    ) {
      continue;
    }
    const firstUser = turn.items.find((item) => item.type === "user");
    const rootMessageId =
      existingRun?.userMessageId ??
      MessageId.make(`message:${driver}-external:${encodeURIComponent(runId)}`);
    const run: OrchestrationV2Run = existingRun ?? {
      id: runId,
      threadId: input.appThread.id,
      ordinal: ++nextRunOrdinal,
      providerInstanceId: input.providerThread.providerInstanceId,
      modelSelection: input.appThread.modelSelection,
      providerThreadId: input.providerThread.id,
      userMessageId: rootMessageId,
      rootNodeId,
      activeAttemptId: attemptId,
      status: lifecycleStatus,
      requestedAt: startedAt,
      startedAt,
      completedAt,
      checkpointId: null,
      contextHandoffId: null,
    };
    const attempt: OrchestrationV2RunAttempt = existingAttempt ?? {
      id: attemptId,
      runId,
      nativeThreadId: input.thread.id,
      attemptOrdinal: 1,
      rootNodeId,
      providerInstanceId: input.providerThread.providerInstanceId,
      providerThreadId: input.providerThread.id,
      providerTurnId,
      reason: "initial",
      status: lifecycleStatus,
      startedAt,
      completedAt,
    };
    const providerTurn: OrchestrationV2ProviderTurn = existingTurn ?? {
      id: providerTurnId,
      providerThreadId: input.providerThread.id,
      nodeId: rootNodeId,
      runAttemptId: attemptId,
      nativeTurnRef: { driver, nativeId: turn.id, strength: "strong" },
      ordinal: ++nextProviderOrdinal,
      status: lifecycleStatus,
      startedAt,
      completedAt,
    };
    const existingScope = input.existing?.checkpointScopes.find(
      (scope) => scope.nodeId === rootNodeId,
    );
    const scope: OrchestrationV2CheckpointScope = existingScope ?? {
      id: CheckpointScopeId.make(
        `checkpoint-scope:${driver}-external:${encodeURIComponent(runId)}`,
      ),
      threadId: input.appThread.id,
      runId,
      nodeId: rootNodeId,
      parentScopeId: null,
      providerThreadId: input.providerThread.id,
      kind: "root_run",
      ordinalWithinParent: 0,
      advancesAppRunCount: true,
      cwd: input.thread.cwd,
      createdAt: startedAt,
    };
    const existingNode = input.existing?.nodes.find((node) => node.id === rootNodeId);
    const rootNode: OrchestrationV2ExecutionNode = existingNode ?? {
      id: rootNodeId,
      threadId: input.appThread.id,
      runId,
      parentNodeId: null,
      rootNodeId,
      kind: "root_turn",
      status: lifecycleStatus,
      countsForRun: true,
      providerThreadId: input.providerThread.id,
      providerTurnId,
      nativeItemRef: null,
      runtimeRequestId: null,
      checkpointScopeId: scope.id,
      startedAt,
      completedAt,
    };
    const recordEvents = [
      ...(existingRun === undefined ? ([{ type: "run.created", payload: run }] as const) : []),
      ...(existingAttempt === undefined
        ? ([{ type: "run-attempt.created", payload: attempt }] as const)
        : []),
      ...(existingTurn === undefined
        ? ([{ type: "provider-turn.updated", payload: providerTurn }] as const)
        : []),
      ...(existingNode === undefined
        ? ([{ type: "node.updated", payload: rootNode }] as const)
        : []),
      ...(existingScope === undefined
        ? ([{ type: "checkpoint-scope.created", payload: scope }] as const)
        : []),
    ];
    for (const event of recordEvents)
      events.push({
        ...event,
        id: EventId.make(
          `native-${driver}-history:${input.appThread.id}:${event.type}:${event.payload.id}`,
        ),
        threadId: input.appThread.id,
        runId,
        nodeId: rootNodeId,
        driver,
        providerInstanceId: input.providerThread.providerInstanceId,
        occurredAt: completedAt,
      });
    const base = (item: NativeHistoryItem, identity = item.id) => ({
      id: idAllocator.derive.turnItemFromProviderItem({ driver, nativeItemId: identity }),
      threadId: input.appThread.id,
      runId,
      nodeId: rootNodeId,
      providerThreadId: input.providerThread.id,
      providerTurnId,
      nativeItemRef: { driver, nativeId: item.id, strength: "strong" as const },
      parentItemId: null,
      ordinal: ++ordinal,
      status: item.status,
      title: item.title ?? null,
      startedAt,
      completedAt,
      updatedAt: completedAt,
    });
    const appendItem = (item: OrchestrationV2TurnItem) => {
      if (existingItems.has(item.id)) return;
      events.push({
        id: EventId.make(`native-${driver}-history:${input.appThread.id}:item:${item.id}`),
        type: "turn-item.updated",
        threadId: input.appThread.id,
        runId,
        providerInstanceId: input.providerThread.providerInstanceId,
        occurredAt: completedAt,
        payload: item,
      });
    };
    for (const item of turn.items) {
      // Native full history exposes completed reasoning even while a later
      // assistant item is still streaming. Do not invent a partial assistant
      // snapshot or duplicate the adapter's synthetic user input.
      if (active && (item.type !== "reasoning" || item.status !== "completed")) continue;
      if (item.type === "user" || item.type === "assistant") {
        const messageId =
          item === firstUser
            ? rootMessageId
            : idAllocator.derive.messageFromProviderItem({ driver, nativeItemId: item.id });
        const common = base(item);
        if (item === firstUser && existingRun !== undefined) {
          const existingUserItem = input.existing?.turnItems.find(
            (value) => value.type === "user_message" && value.messageId === rootMessageId,
          );
          common.id =
            existingUserItem?.id ?? idAllocator.derive.userTurnItem({ messageId: rootMessageId });
        }
        const text = item.text;
        if (!existingMessages.has(messageId)) {
          const message: OrchestrationV2ConversationMessage = {
            id: messageId,
            createdBy: item.type === "user" ? "user" : "agent",
            creationSource: "provider",
            threadId: input.appThread.id,
            runId,
            nodeId: rootNodeId,
            role: item.type === "user" ? "user" : "assistant",
            text,
            attachments: [],
            streaming: false,
            createdAt: startedAt,
            updatedAt: completedAt,
          };
          events.push({
            id: EventId.make(`native-${driver}-history:${input.appThread.id}:message:${messageId}`),
            type: "message.updated",
            threadId: input.appThread.id,
            runId,
            providerInstanceId: input.providerThread.providerInstanceId,
            occurredAt: completedAt,
            payload: message,
          });
        }
        appendItem(
          item.type === "user"
            ? {
                ...common,
                type: "user_message",
                messageId,
                text,
                attachments: [],
                inputIntent: "turn_start",
                createdBy: "user",
                creationSource: "provider",
              }
            : { ...common, type: "assistant_message", messageId, text, streaming: false },
        );
      } else if (item.type === "reasoning") {
        appendItem({ ...base(item), type: "reasoning", text: item.text, streaming: false });
      } else {
        appendItem({
          ...base(item),
          type: "dynamic_tool",
          toolName: item.title || "Tool",
          input: {},
          output: item.text,
        });
      }
    }
  }
  return events;
};
