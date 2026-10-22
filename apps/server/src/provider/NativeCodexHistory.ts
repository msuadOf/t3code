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
  ProviderDriverKind,
  type OrchestrationV2ConversationMessage,
  type OrchestrationV2DomainEvent,
  type OrchestrationV2TurnItem,
} from "@t3tools/contracts";
import type * as IdAllocator from "@t3tools/provider-core/server/IdAllocator";
import type * as CodexSchema from "effect-codex-app-server/schema";
import * as DateTime from "effect/DateTime";

const CODEX = ProviderDriverKind.make("codex");

type NativeTurn = CodexSchema.V2ThreadTurnsListResponse__Turn;
type NativeItem = NativeTurn["items"][number];

export interface NativeCodexHistoryInput {
  readonly thread: {
    readonly id: string;
    readonly cwd: string;
    readonly createdAt: number;
    readonly updatedAt: number;
  };
  /** All pages in chronological order. Active turns are owned by the adapter. */
  readonly turns: ReadonlyArray<NativeTurn>;
  readonly appThread: OrchestrationV2AppThread;
  readonly providerThread: OrchestrationV2ProviderThread;
  readonly existing?: Pick<
    OrchestrationV2ThreadProjection,
    "runs" | "attempts" | "providerTurns" | "nodes" | "checkpointScopes" | "messages" | "turnItems"
  >;
}

const itemStatus = (item: NativeItem): OrchestrationV2TurnItem["status"] => {
  if (!("status" in item)) return "completed";
  switch (item.status) {
    case "inProgress":
      return "running";
    case "failed":
      return "failed";
    case "declined":
      return "cancelled";
    default:
      return "completed";
  }
};

const userText = (item: Extract<NativeItem, { type: "userMessage" }>) =>
  item.content
    .map((part) => {
      switch (part.type) {
        case "text":
          return part.text;
        case "mention":
        case "skill":
          return `${part.name} (${part.path})`;
        case "localImage":
          return `[Image: ${part.path}]`;
        case "localAudio":
          return `[Audio: ${part.path}]`;
        case "image":
          return "url" in part ? `[Image: ${part.url}]` : `[Image: ${part.fileId}]`;
        case "audio":
          return `[Audio: ${part.url}]`;
      }
    })
    .join("\n");

/** Materialize historical native items without replacing any live-owned record. */
export const mapHistory = (
  input: NativeCodexHistoryInput,
  idAllocator: IdAllocator.IdAllocatorV2["Service"],
): ReadonlyArray<OrchestrationV2DomainEvent> => {
  const events: Array<OrchestrationV2DomainEvent> = [];
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
    if (turn.status === "inProgress") continue;
    const startedAt = DateTime.makeUnsafe((turn.startedAt ?? input.thread.createdAt) * 1000);
    const completedAt = DateTime.makeUnsafe((turn.completedAt ?? input.thread.updatedAt) * 1000);
    const providerTurnId = idAllocator.derive.providerTurn({
      driver: CODEX,
      nativeTurnId: turn.id,
    });
    const existingTurn = input.existing?.providerTurns.find((value) => value.id === providerTurnId);
    const existingAttempt = input.existing?.attempts.find(
      (value) => value.id === existingTurn?.runAttemptId,
    );
    const nativeRunId = RunId.make(
      `run:codex-external:${encodeURIComponent(input.providerThread.id)}:${encodeURIComponent(turn.id)}`,
    );
    const existingRun = input.existing?.runs.find(
      (value) => value.id === (existingAttempt?.runId ?? nativeRunId),
    );
    const runId = existingRun?.id ?? nativeRunId;
    const rootNodeId = existingRun?.rootNodeId ?? idAllocator.derive.rootNode({ runId });
    const attemptId =
      existingAttempt?.id ?? idAllocator.derive.runAttempt({ runId, attemptOrdinal: 1 });
    const firstUser = turn.items.find((item) => item.type === "userMessage");
    const rootMessageId =
      existingRun?.userMessageId ??
      (firstUser === undefined
        ? MessageId.make(`message:codex-external:${encodeURIComponent(runId)}`)
        : idAllocator.derive.messageFromProviderItem({
            driver: CODEX,
            nativeItemId: firstUser.id,
          }));
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
      status: turn.status,
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
      status: turn.status,
      startedAt,
      completedAt,
    };
    const providerTurn: OrchestrationV2ProviderTurn = existingTurn ?? {
      id: providerTurnId,
      providerThreadId: input.providerThread.id,
      nodeId: rootNodeId,
      runAttemptId: attemptId,
      nativeTurnRef: { driver: CODEX, nativeId: turn.id, strength: "strong" },
      ordinal: ++nextProviderOrdinal,
      status: turn.status,
      startedAt,
      completedAt,
    };
    const existingScope = input.existing?.checkpointScopes.find(
      (scope) => scope.nodeId === rootNodeId,
    );
    const scope: OrchestrationV2CheckpointScope = existingScope ?? {
      id: CheckpointScopeId.make(`checkpoint-scope:codex-external:${encodeURIComponent(runId)}`),
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
      status: turn.status,
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
          `native-codex-history:${input.appThread.id}:${event.type}:${event.payload.id}`,
        ),
        threadId: input.appThread.id,
        runId,
        nodeId: rootNodeId,
        driver: CODEX,
        providerInstanceId: input.providerThread.providerInstanceId,
        occurredAt: completedAt,
      });
    const base = (item: NativeItem, identity = item.id) => ({
      id: idAllocator.derive.turnItemFromProviderItem({ driver: CODEX, nativeItemId: identity }),
      threadId: input.appThread.id,
      runId,
      nodeId: rootNodeId,
      providerThreadId: input.providerThread.id,
      providerTurnId,
      nativeItemRef: { driver: CODEX, nativeId: item.id, strength: "strong" as const },
      parentItemId: null,
      ordinal: ++ordinal,
      status: itemStatus(item),
      title: null,
      startedAt,
      completedAt,
      updatedAt: completedAt,
    });
    const appendItem = (item: OrchestrationV2TurnItem) => {
      if (existingItems.has(item.id)) return;
      events.push({
        id: EventId.make(`native-codex-history:${input.appThread.id}:item:${item.id}`),
        type: "turn-item.updated",
        threadId: input.appThread.id,
        runId,
        providerInstanceId: input.providerThread.providerInstanceId,
        occurredAt: completedAt,
        payload: item,
      });
    };
    for (const item of turn.items) {
      if (item.type === "userMessage" || item.type === "agentMessage") {
        const messageId =
          item === firstUser
            ? rootMessageId
            : idAllocator.derive.messageFromProviderItem({ driver: CODEX, nativeItemId: item.id });
        const common = base(item);
        if (item === firstUser && existingRun !== undefined) {
          const existingUserItem = input.existing?.turnItems.find(
            (value) => value.type === "user_message" && value.messageId === rootMessageId,
          );
          common.id =
            existingUserItem?.id ?? idAllocator.derive.userTurnItem({ messageId: rootMessageId });
        }
        const text = item.type === "userMessage" ? userText(item) : item.text;
        if (!existingMessages.has(messageId)) {
          const message: OrchestrationV2ConversationMessage = {
            id: messageId,
            createdBy: item.type === "userMessage" ? "user" : "agent",
            creationSource: "provider",
            threadId: input.appThread.id,
            runId,
            nodeId: rootNodeId,
            role: item.type === "userMessage" ? "user" : "assistant",
            text,
            attachments: [],
            streaming: false,
            createdAt: startedAt,
            updatedAt: completedAt,
          };
          events.push({
            id: EventId.make(`native-codex-history:${input.appThread.id}:message:${messageId}`),
            type: "message.updated",
            threadId: input.appThread.id,
            runId,
            providerInstanceId: input.providerThread.providerInstanceId,
            occurredAt: completedAt,
            payload: message,
          });
        }
        appendItem(
          item.type === "userMessage"
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
        for (const stream of ["summary", "content"] as const) {
          for (const [index, text] of (item[stream] ?? []).entries()) {
            // The adapter splits reasoning into these same native part IDs.
            const identity = JSON.stringify([turn.id, item.id, stream, index]);
            appendItem({ ...base(item, identity), type: "reasoning", text, streaming: false });
          }
        }
      } else if (item.type === "commandExecution") {
        appendItem({
          ...base(item),
          type: "command_execution",
          input: item.command,
          ...(item.aggregatedOutput == null ? {} : { output: item.aggregatedOutput }),
          ...(item.exitCode == null ? {} : { exitCode: item.exitCode }),
        });
      } else if (item.type === "fileChange" && item.changes[0] !== undefined) {
        const first = item.changes[0];
        appendItem({
          ...base(item),
          type: "file_change",
          fileName: first.path,
          diffStr: first.diff,
          changes: item.changes.map((change) => {
            const movePath = change.kind.type === "update" ? change.kind.move_path : undefined;
            return movePath
              ? { operation: change.kind.type, path: movePath, oldPath: change.path }
              : { operation: change.kind.type, path: change.path };
          }),
        });
      } else if (item.type === "contextCompaction") {
        appendItem({ ...base(item), type: "compaction", driver: CODEX });
      } else if (item.type === "webSearch") {
        appendItem({ ...base(item), type: "web_search", patterns: [item.query] });
      } else {
        // Keep provider-specific tool input/output available through existing
        // tool cards even when it has no specialized T3 rendering.
        const toolName = "tool" in item ? item.tool : "name" in item ? item.name : item.type;
        const output =
          item.type === "functionCallOutput"
            ? item.output
            : item.type === "mcpToolCall"
              ? (item.result ?? item.error)
              : item.type === "dynamicToolCall"
                ? item.contentItems
                : item.type === "plan"
                  ? item.text
                  : item;
        appendItem({
          ...base(item),
          type: "dynamic_tool",
          toolName: toolName || item.type,
          input: "arguments" in item ? item.arguments : item,
          output,
        });
      }
    }
  }
  return events;
};
