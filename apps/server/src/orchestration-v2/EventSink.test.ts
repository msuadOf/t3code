import { assert, it } from "@effect/vitest";
import {
  EventId,
  ProviderDriverKind,
  ProviderInstanceId,
  ProviderThreadId,
  ProviderTurnId,
  RunAttemptId,
  CheckpointScopeId,
  MessageId,
  NodeId,
  RunId,
  ThreadId,
  TurnItemId,
  type OrchestrationV2DomainEvent,
} from "@t3tools/contracts";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as SqlClient from "effect/sql/SqlClient";

import * as Sqlite from "../persistence/Sqlite.ts";
import * as EventSink from "./EventSink.ts";
import * as EventStore from "./EventStore.ts";
import * as ProjectionStore from "./ProjectionStore.ts";

const stores = Layer.mergeAll(EventStore.layer, ProjectionStore.layer).pipe(
  Layer.provideMerge(Sqlite.layerMemory),
);
const layer = EventSink.layer.pipe(Layer.provideMerge(stores));

it.layer(layer)("event sink native history guard", (it) => {
  it.effect("does not overwrite live ownership that arrived after a history snapshot", () =>
    Effect.gen(function* () {
      const sink = yield* EventSink.EventSinkV2;
      const sql = yield* SqlClient.SqlClient;
      const now = yield* DateTime.now;
      const threadId = ThreadId.make("shared-thread");
      const messageId = MessageId.make("native-message");
      const itemId = TurnItemId.make("native-item");
      const historyMessage: Extract<OrchestrationV2DomainEvent, { type: "message.updated" }> = {
        id: EventId.make("history-message"),
        type: "message.updated",
        threadId,
        occurredAt: now,
        payload: {
          id: messageId,
          threadId,
          createdBy: "agent",
          creationSource: "provider",
          runId: null,
          nodeId: null,
          role: "assistant",
          text: "stale snapshot",
          attachments: [],
          streaming: false,
          createdAt: now,
          updatedAt: now,
        },
      };
      const historyItem = {
        id: EventId.make("history-item"),
        type: "turn-item.updated",
        threadId,
        occurredAt: now,
        payload: {
          id: itemId,
          threadId,
          runId: null,
          nodeId: null,
          providerThreadId: null,
          providerTurnId: null,
          nativeItemRef: null,
          parentItemId: null,
          ordinal: 1,
          status: "completed",
          title: null,
          startedAt: now,
          completedAt: now,
          updatedAt: now,
          type: "assistant_message",
          messageId,
          text: "stale snapshot",
          streaming: false,
        },
      } satisfies Extract<OrchestrationV2DomainEvent, { type: "turn-item.updated" }>;
      const runId = RunId.make("live-run");
      const nodeId = NodeId.make("live-node");
      yield* sink.write({
        events: [
          {
            ...historyMessage,
            id: EventId.make("live-message"),
            payload: { ...historyMessage.payload, runId, nodeId, text: "live content" },
          },
          {
            ...historyItem,
            id: EventId.make("live-item"),
            payload: {
              ...historyItem.payload,
              type: "assistant_message",
              messageId,
              runId,
              nodeId,
              text: "live content",
              streaming: false,
            },
          },
        ],
      });
      const committed = yield* sink.writeWithEffects({
        insertHistoryOnly: true,
        events: [historyMessage, historyItem],
        effects: [],
      });
      assert.equal(committed.length, 0);
      const messages = yield* sql<{
        readonly runId: string;
        readonly nodeId: string;
        readonly payload: string;
      }>`SELECT run_id AS runId, node_id AS nodeId, payload_json AS payload FROM orchestration_v2_projection_messages WHERE message_id = ${messageId}`;
      assert.equal(messages[0]?.runId, runId);
      assert.equal(messages[0]?.nodeId, nodeId);
      assert.equal(JSON.parse(messages[0]?.payload ?? "{}").text, "live content");
      const items = yield* sql<{
        readonly runId: string;
        readonly nodeId: string;
      }>`SELECT run_id AS runId, node_id AS nodeId FROM orchestration_v2_projection_turn_items WHERE turn_item_id = ${itemId}`;
      assert.equal(items[0]?.runId, runId);
      assert.equal(items[0]?.nodeId, nodeId);
      const newMessage = {
        ...historyMessage,
        id: EventId.make("new-history-event"),
        payload: { ...historyMessage.payload, id: MessageId.make("new-native-message") },
      };
      assert.equal(
        (yield* sink.write({ insertHistoryOnly: true, events: [newMessage] })).length,
        1,
      );
    }),
  );
  it.effect(
    "preserves live lifecycle ownership when a different run adopted the native turn during history loading",
    () =>
      Effect.gen(function* () {
        const sink = yield* EventSink.EventSinkV2;
        const sql = yield* SqlClient.SqlClient;
        const now = yield* DateTime.now;
        const threadId = ThreadId.make("race-thread");
        const providerThreadId = ProviderThreadId.make("race-provider-thread");
        const providerTurnId = ProviderTurnId.make("same-native-turn");
        const instanceId = ProviderInstanceId.make("codex");
        const driver = ProviderDriverKind.make("codex");
        const metadata = (
          suffix: string,
          status: "running" | "completed",
        ): OrchestrationV2DomainEvent[] => {
          const runId = RunId.make(`run-${suffix}`);
          const nodeId = NodeId.make(`node-${suffix}`);
          const attemptId = RunAttemptId.make(`attempt-${suffix}`);
          const scopeId = CheckpointScopeId.make(`scope-${suffix}`);
          const completedAt = status === "completed" ? now : null;
          const payloads = [
            {
              type: "run.created",
              payload: {
                id: runId,
                threadId,
                ordinal: 1,
                providerInstanceId: instanceId,
                modelSelection: { instanceId, model: "test-model" },
                providerThreadId,
                userMessageId: MessageId.make(`user-${suffix}`),
                rootNodeId: nodeId,
                activeAttemptId: attemptId,
                status,
                requestedAt: now,
                startedAt: now,
                completedAt,
                checkpointId: null,
                contextHandoffId: null,
              },
            },
            {
              type: "run-attempt.created",
              payload: {
                id: attemptId,
                runId,
                attemptOrdinal: 1,
                rootNodeId: nodeId,
                providerInstanceId: instanceId,
                providerThreadId,
                providerTurnId,
                reason: "initial",
                status,
                startedAt: now,
                completedAt,
              },
            },
            {
              type: "provider-turn.updated",
              payload: {
                id: providerTurnId,
                providerThreadId,
                nodeId,
                runAttemptId: attemptId,
                nativeTurnRef: { driver, nativeId: "native-turn", strength: "strong" },
                ordinal: 1,
                status,
                startedAt: now,
                completedAt,
              },
            },
            {
              type: "node.updated",
              payload: {
                id: nodeId,
                threadId,
                runId,
                parentNodeId: null,
                rootNodeId: nodeId,
                kind: "root_turn",
                status,
                countsForRun: true,
                providerThreadId,
                providerTurnId,
                nativeItemRef: null,
                runtimeRequestId: null,
                checkpointScopeId: scopeId,
                startedAt: now,
                completedAt,
              },
            },
            {
              type: "checkpoint-scope.created",
              payload: {
                id: scopeId,
                threadId,
                runId,
                nodeId,
                parentScopeId: null,
                providerThreadId,
                kind: "root_run",
                ordinalWithinParent: 0,
                advancesAppRunCount: true,
                cwd: "/repo",
                createdAt: now,
              },
            },
          ] as const;
          return payloads.map((event) => ({
            ...event,
            id: EventId.make(`${suffix}:${event.type}`),
            threadId,
            runId,
            nodeId,
            providerInstanceId: instanceId,
            occurredAt: now,
          }));
        };
        const stale = metadata("history", "completed");
        yield* sink.write({ events: metadata("live", "running") });
        const before = yield* sink.latestSequence();
        assert.equal((yield* sink.write({ insertHistoryOnly: true, events: stale })).length, 0);
        assert.equal(yield* sink.latestSequence(), before);
        const rows = yield* sql<{
          readonly id: string;
          readonly status: string;
        }>`SELECT run_id AS id, status FROM orchestration_v2_projection_runs WHERE thread_id = ${threadId}`;
        assert.deepEqual(rows, [{ id: "run-live", status: "running" }]);
        const scopes = yield* sql<{
          readonly id: string;
        }>`SELECT scope_id AS id FROM orchestration_v2_projection_checkpoint_scopes WHERE thread_id = ${threadId}`;
        assert.deepEqual(scopes, [{ id: "scope-live" }]);
        const sameOwner = metadata("live", "completed").map((event) => ({
          ...event,
          id: EventId.make(`stale-same-owner:${event.id}`),
        }));
        assert.equal((yield* sink.write({ insertHistoryOnly: true, events: sameOwner })).length, 0);
      }),
  );
});
