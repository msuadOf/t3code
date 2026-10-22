import {
  ProviderDriverKind,
  MessageId,
  type OrchestrationV2DomainEvent,
  ProjectId,
  ProviderInstanceId,
  ProviderThreadId,
  ThreadId,
} from "@t3tools/contracts";
import * as IdAllocator from "@t3tools/provider-core/server/IdAllocator";
import { assert, it } from "@effect/vitest";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";

import { mapHistory, type NativeCodexHistoryInput } from "./NativeCodexHistory.ts";

const recordsOf = (events: ReadonlyArray<OrchestrationV2DomainEvent>) => ({
  messages: events.flatMap((event) => (event.type === "message.updated" ? [event.payload] : [])),
  turnItems: events.flatMap((event) => (event.type === "turn-item.updated" ? [event.payload] : [])),
  runs: events.flatMap((event) => (event.type === "run.created" ? [event.payload] : [])),
  attempts: events.flatMap((event) =>
    event.type === "run-attempt.created" ? [event.payload] : [],
  ),
  providerTurns: events.flatMap((event) =>
    event.type === "provider-turn.updated" ? [event.payload] : [],
  ),
  nodes: events.flatMap((event) => (event.type === "node.updated" ? [event.payload] : [])),
  checkpointScopes: events.flatMap((event) =>
    event.type === "checkpoint-scope.created" ? [event.payload] : [],
  ),
});

const fixture = (): NativeCodexHistoryInput => ({
  thread: { id: "native-thread", cwd: "/repo", createdAt: 100, updatedAt: 200 },
  appThread: {
    id: ThreadId.make("app-thread"),
    projectId: ProjectId.make("project"),
    title: "Native session",
    createdBy: "system",
    creationSource: "provider",
    providerInstanceId: ProviderInstanceId.make("codex-instance"),
    modelSelection: { instanceId: ProviderInstanceId.make("codex-instance"), model: "test-model" },
    runtimeMode: "full-access",
    interactionMode: "default",
    branch: null,
    worktreePath: null,
    activeProviderThreadId: ProviderThreadId.make("provider-thread"),
    lineage: {
      parentThreadId: null,
      relationshipToParent: null,
      rootThreadId: ThreadId.make("app-thread"),
    },
    forkedFrom: null,
    createdAt: DateTime.makeUnsafe(100000),
    updatedAt: DateTime.makeUnsafe(200000),
    archivedAt: null,
    settledOverride: null,
    settledAt: null,
    lastVisitedAt: null,
    deletedAt: null,
  },
  providerThread: {
    id: ProviderThreadId.make("provider-thread"),
    driver: ProviderDriverKind.make("codex"),
    providerInstanceId: ProviderInstanceId.make("codex-instance"),
    providerSessionId: null,
    appThreadId: ThreadId.make("app-thread"),
    ownerNodeId: null,
    nativeThreadRef: {
      driver: ProviderDriverKind.make("codex"),
      nativeId: "native-thread",
      strength: "strong",
    },
    nativeConversationHeadRef: null,
    status: "idle",
    firstRunOrdinal: null,
    lastRunOrdinal: null,
    handoffIds: [],
    forkedFrom: null,
    pendingBackgroundTasks: [],
    createdAt: DateTime.makeUnsafe(100000),
    updatedAt: DateTime.makeUnsafe(200000),
  },
  turns: [
    {
      id: "turn-1",
      status: "completed",
      startedAt: 110,
      completedAt: 120,
      items: [
        {
          id: "user-1",
          type: "userMessage",
          content: [{ type: "text", text: "Inspect the repository" }],
        },
        {
          id: "reason-1",
          type: "reasoning",
          summary: ["Find the entry point", "Check dependencies"],
          content: ["Read the config"],
        },
        {
          id: "cmd-1",
          type: "commandExecution",
          command: "git status",
          commandActions: [],
          cwd: "/repo",
          status: "completed",
          exitCode: 0,
          aggregatedOutput: "clean",
        },
        {
          id: "mcp-1",
          type: "mcpToolCall",
          server: "docs",
          tool: "read",
          arguments: { path: "README" },
          status: "completed",
          result: { content: [{ type: "text", text: "Documentation" }] },
        },
        { id: "assistant-1", type: "agentMessage", text: "The repository is clean." },
      ],
    },
  ],
});

it.layer(IdAllocator.layer)("native Codex history", (it) => {
  it.effect(
    "renders messages, reasoning parts and tool output with the same native IDs as live events",
    () =>
      Effect.gen(function* () {
        const allocator = yield* IdAllocator.IdAllocatorV2;
        const events = mapHistory(fixture(), allocator);
        const messages = events.flatMap((event) =>
          event.type === "message.updated" ? [event.payload] : [],
        );
        const items = events.flatMap((event) =>
          event.type === "turn-item.updated" ? [event.payload] : [],
        );
        assert.deepEqual(
          messages.map((message) => [message.role, message.text]),
          [
            ["user", "Inspect the repository"],
            ["assistant", "The repository is clean."],
          ],
        );
        assert.equal(
          messages[1]?.id,
          allocator.derive.messageFromProviderItem({
            driver: ProviderDriverKind.make("codex"),
            nativeItemId: "assistant-1",
          }),
        );
        const records = recordsOf(events);
        assert.equal(records.runs.length, 1);
        assert.equal(records.runs[0]?.status, "completed");
        assert.equal(records.attempts[0]?.providerTurnId, records.providerTurns[0]?.id);
        assert.equal(records.providerTurns[0]?.nativeTurnRef?.nativeId, "turn-1");
        assert.equal(records.nodes[0]?.checkpointScopeId, records.checkpointScopes[0]?.id);
        assert.equal(
          events.some((event) => event.type === "checkpoint.captured"),
          false,
        );
        const reasoning = items.filter((item) => item.type === "reasoning");
        assert.equal(reasoning.length, 3);
        assert.equal(
          reasoning[0]?.id,
          allocator.derive.turnItemFromProviderItem({
            driver: ProviderDriverKind.make("codex"),
            nativeItemId: JSON.stringify(["turn-1", "reason-1", "summary", 0]),
          }),
        );
        const command = items.find((item) => item.type === "command_execution");
        assert.equal(command?.output, "clean");
        assert.equal(command?.exitCode, 0);
        const tool = items.find((item) => item.type === "dynamic_tool");
        assert.deepEqual(tool?.output, { content: [{ type: "text", text: "Documentation" }] });
        assert.equal(
          items.every(
            (item) => item.runId !== null && item.providerThreadId === fixture().providerThread.id,
          ),
          true,
        );
      }),
  );

  it.effect("keeps live-owned records untouched and leaves in-progress turns to the adapter", () =>
    Effect.gen(function* () {
      const allocator = yield* IdAllocator.IdAllocatorV2;
      const original = fixture();
      const first = mapHistory(original, allocator);
      const events = mapHistory(
        {
          ...original,
          existing: recordsOf(first),
          turns: [
            ...original.turns,
            {
              id: "active-turn",
              status: "inProgress",
              items: [{ id: "partial-live-answer", type: "agentMessage", text: "still running" }],
            },
          ],
        },
        allocator,
      );
      assert.deepEqual(events, []);
    }),
  );

  it.effect(
    "restores missing native output for a known live run without recreating its synthetic user or changing lifecycle",
    () =>
      Effect.gen(function* () {
        const allocator = yield* IdAllocator.IdAllocatorV2;
        const original = fixture();
        const records = recordsOf(mapHistory(original, allocator));
        const syntheticUser = MessageId.make("synthetic-live-user");
        const existing = {
          ...records,
          runs: records.runs.map((run) => ({
            ...run,
            userMessageId: syntheticUser,
            status: "running" as const,
            completedAt: null,
          })),
          messages: records.messages
            .filter((message) => message.role === "user")
            .map((message) => ({ ...message, id: syntheticUser })),
          turnItems: records.turnItems
            .filter((item) => item.type !== "assistant_message")
            .map((item) =>
              item.type === "user_message"
                ? {
                    ...item,
                    id: allocator.derive.userTurnItem({ messageId: syntheticUser }),
                    messageId: syntheticUser,
                  }
                : item,
            ),
        };
        const events = mapHistory({ ...original, existing }, allocator);
        assert.deepEqual(
          events.map((event) => event.type),
          ["message.updated", "turn-item.updated"],
        );
        const message = events.find((event) => event.type === "message.updated")?.payload;
        assert.equal(message?.role, "assistant");
        assert.equal(message?.runId, existing.runs[0]?.id);
        assert.equal(message?.nodeId, existing.runs[0]?.rootNodeId);
        assert.equal(existing.runs[0]?.status, "running");
      }),
  );

  it.effect("hydrates canonical completed reasoning when joining an already running CLI turn", () =>
    Effect.gen(function* () {
      const allocator = yield* IdAllocator.IdAllocatorV2;
      const original = fixture();
      const records = recordsOf(mapHistory(original, allocator));
      const existing = {
        ...records,
        runs: records.runs.map((run) => ({
          ...run,
          status: "running" as const,
          completedAt: null,
        })),
        attempts: records.attempts.map((attempt) => ({
          ...attempt,
          status: "running" as const,
          completedAt: null,
        })),
        providerTurns: records.providerTurns.map((turn) => ({
          ...turn,
          status: "running" as const,
          completedAt: null,
        })),
        nodes: records.nodes.map((node) => ({
          ...node,
          status: "running" as const,
          completedAt: null,
        })),
        messages: records.messages.filter((message) => message.role === "user"),
        turnItems: records.turnItems.filter((item) => item.type === "user_message"),
      };
      // Actual thread/turns/list shape while the assistant stream is held:
      // canonical user/reasoning IDs, completed reasoning, no assistant prefix.
      const input: NativeCodexHistoryInput = {
        ...original,
        existing,
        turns: [
          {
            id: "turn-1",
            status: "inProgress",
            startedAt: 110,
            completedAt: null,
            items: [
              {
                id: "user-1",
                type: "userMessage",
                content: [{ type: "text", text: "Inspect the repository" }],
              },
              {
                id: "reason_100001",
                type: "reasoning",
                summary: [
                  "I am verifying that the phone and terminal control the same native Codex session.",
                ],
                content: [],
              },
            ],
          },
        ],
      };
      const events = mapHistory(input, allocator);
      assert.equal(events.length, 1);
      const item = recordsOf(events).turnItems[0];
      assert.equal(item?.type, "reasoning");
      assert.equal(item?.nodeId, existing.runs[0]?.rootNodeId);
      assert.equal(item?.runId, existing.runs[0]?.id);
      assert.equal(item?.nativeItemRef?.nativeId, "reason_100001");
      assert.equal(
        item?.id,
        allocator.derive.turnItemFromProviderItem({
          driver: ProviderDriverKind.make("codex"),
          nativeItemId: JSON.stringify(["turn-1", "reason_100001", "summary", 0]),
        }),
      );
      assert.deepEqual(
        mapHistory(
          {
            ...input,
            existing: {
              ...existing,
              turnItems: [...existing.turnItems, ...recordsOf(events).turnItems],
            },
          },
          allocator,
        ),
        [],
      );
      assert.equal(existing.runs[0]?.status, "running");
      assert.equal(existing.runs[0]?.completedAt, null);
    }),
  );

  it.effect("does not truncate a completed native conversation at the old importer limit", () =>
    Effect.gen(function* () {
      const allocator = yield* IdAllocator.IdAllocatorV2;
      const original = fixture();
      const events = mapHistory(
        {
          ...original,
          turns: [
            {
              id: "long-turn",
              status: "completed",
              items: Array.from({ length: 250 }, (_, index) => ({
                id: `answer-${index}`,
                type: "agentMessage" as const,
                text: `message ${index}`,
              })),
            },
          ],
        },
        allocator,
      );
      const messages = events.flatMap((event) =>
        event.type === "message.updated" ? [event.payload] : [],
      );
      assert.equal(messages.length, 250);
      assert.equal(messages.at(-1)?.text, "message 249");
    }),
  );
});
