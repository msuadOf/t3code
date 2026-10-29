import {
  ProviderDriverKind,
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

import { mapHistory, type NativeSessionHistoryInput } from "./NativeSessionHistory.ts";

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

const fixture = (driver: ProviderDriverKind): NativeSessionHistoryInput => ({
  driver,
  thread: { id: "native-thread", cwd: "/repo", createdAt: 100000, updatedAt: 200000 },
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
    driver,
    providerInstanceId: ProviderInstanceId.make("codex-instance"),
    providerSessionId: null,
    appThreadId: ThreadId.make("app-thread"),
    ownerNodeId: null,
    nativeThreadRef: {
      driver,
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
      startedAt: 110000,
      completedAt: 120000,
      items: [
        { id: "user-1", type: "user", text: "Inspect the repository", status: "completed" },
        { id: "reason-1", type: "reasoning", text: "Find the entry point", status: "completed" },
        {
          id: "tool-1",
          type: "tool",
          title: "Bash",
          input: { command: "git status" },
          text: "git status: clean",
          status: "completed",
        },
        {
          id: "assistant-1",
          type: "assistant",
          text: "The repository is clean.",
          status: "completed",
        },
      ],
    },
  ],
});

for (const provider of ["claudeAgent", "opencode"] as const) {
  it.layer(IdAllocator.layer)(`${provider} native history`, (it) => {
    it.effect(
      "materializes native messages, thinking and tools once using external-run identities",
      () =>
        Effect.gen(function* () {
          const allocator = yield* IdAllocator.IdAllocatorV2;
          const input = fixture(ProviderDriverKind.make(provider));
          const events = mapHistory(input, allocator);
          const records = recordsOf(events);
          assert.equal(records.runs.length, 1);
          assert.equal(records.runs[0]?.id, `run:${provider}-external:provider-thread:turn-1`);
          assert.equal(
            records.messages[0]?.id,
            `message:${provider}-external:${encodeURIComponent(String(records.runs[0]?.id))}`,
          );
          assert.deepEqual(
            records.messages.map((message) => [message.role, message.text]),
            [
              ["user", "Inspect the repository"],
              ["assistant", "The repository is clean."],
            ],
          );
          assert.deepEqual(
            records.turnItems.map((item) => item.type),
            ["user_message", "reasoning", "dynamic_tool", "assistant_message"],
          );
          assert.equal(
            records.turnItems[1]?.id,
            allocator.derive.turnItemFromProviderItem({
              driver: input.driver,
              nativeItemId: "reason-1",
            }),
          );
          const tool = records.turnItems.find((item) => item.type === "dynamic_tool");
          assert.equal(tool?.type, "dynamic_tool");
          if (tool?.type === "dynamic_tool") {
            assert.deepEqual(tool.input, { command: "git status" });
            assert.equal(tool.output, "git status: clean");
          }
          assert.equal(records.providerTurns[0]?.nativeTurnRef?.driver, input.driver);
          assert.equal(DateTime.toEpochMillis(records.runs[0]!.startedAt!), 110000);
          assert.deepEqual(mapHistory({ ...input, existing: records }, allocator), []);
        }),
    );
    it.effect("reuses a T3 admission turn after its native identity is learned", () =>
      Effect.gen(function* () {
        const allocator = yield* IdAllocator.IdAllocatorV2;
        const input = fixture(ProviderDriverKind.make(provider));
        const existing = recordsOf(mapHistory(input, allocator));
        const admissionId = allocator.derive.providerTurn({
          driver: input.driver,
          nativeTurnId: "session:attempt:t3-attempt",
        });
        existing.providerTurns = existing.providerTurns.map((turn) => ({
          ...turn,
          id: admissionId,
        }));
        existing.turnItems = existing.turnItems.map((item) => ({
          ...item,
          providerTurnId: admissionId,
        }));
        assert.deepEqual(mapHistory({ ...input, existing }, allocator), []);
      }),
    );
    it.effect(
      "does not create active runs and only fills missing completed thinking after live adoption",
      () =>
        Effect.gen(function* () {
          const allocator = yield* IdAllocator.IdAllocatorV2;
          const input = fixture(ProviderDriverKind.make(provider));
          const active = {
            ...input,
            turns: input.turns.map((turn) => ({
              ...turn,
              status: "running" as const,
              completedAt: null,
            })),
          };
          assert.deepEqual(mapHistory(active, allocator), []);
          const existing = recordsOf(mapHistory(input, allocator));
          existing.turnItems = existing.turnItems.filter((item) => item.type !== "reasoning");
          existing.runs = existing.runs.map((run) => ({
            ...run,
            status: "running",
            completedAt: null,
          }));
          existing.attempts = existing.attempts.map((attempt) => ({
            ...attempt,
            status: "running",
            completedAt: null,
          }));
          existing.providerTurns = existing.providerTurns.map((turn) => ({
            ...turn,
            status: "running",
            completedAt: null,
          }));
          const events = mapHistory({ ...active, existing }, allocator);
          assert.equal(events.length, 1);
          assert.equal(events[0]?.type, "turn-item.updated");
          const reasoning = recordsOf(events).turnItems[0];
          assert.equal(reasoning?.type, "reasoning");
          assert.equal(reasoning?.status, "completed");
          const incomplete = {
            ...active,
            turns: active.turns.map((turn) => ({
              ...turn,
              items: turn.items.map((item) => ({ ...item, status: "running" as const })),
            })),
          };
          assert.deepEqual(mapHistory({ ...incomplete, existing }, allocator), []);
        }),
    );
    it.effect("leaves live-owned message text, item content and run lifecycle unchanged", () =>
      Effect.gen(function* () {
        const allocator = yield* IdAllocator.IdAllocatorV2;
        const input = fixture(ProviderDriverKind.make(provider));
        const existing = recordsOf(mapHistory(input, allocator));
        existing.messages = existing.messages.map((message) => ({
          ...message,
          text: "Live text",
          streaming: true,
        }));
        existing.turnItems = existing.turnItems.map((item) =>
          item.type === "reasoning" ? { ...item, text: "Live reasoning", streaming: true } : item,
        );
        existing.runs = existing.runs.map((run) => ({
          ...run,
          status: "running",
          completedAt: null,
        }));
        assert.deepEqual(mapHistory({ ...input, existing }, allocator), []);
        assert.equal(existing.messages[0]?.text, "Live text");
        assert.equal(existing.runs[0]?.status, "running");
      }),
    );
    it.effect(
      "repairs only marked text from completed native blocks without changing identity",
      () =>
        Effect.gen(function* () {
          const allocator = yield* IdAllocator.IdAllocatorV2;
          const input = fixture(ProviderDriverKind.make(provider));
          const existing = recordsOf(mapHistory(input, allocator));
          existing.messages = existing.messages.map((message) =>
            message.role === "assistant"
              ? {
                  ...message,
                  text: "latest suffix",
                  textPrefixMissing: true,
                  streaming: false,
                  createdAt: DateTime.makeUnsafe(115000),
                }
              : message,
          );
          existing.turnItems = existing.turnItems.map((item) =>
            item.type === "assistant_message" || item.type === "reasoning"
              ? {
                  ...item,
                  text: "latest suffix",
                  textPrefixMissing: true,
                  streaming: false,
                  ordinal: item.ordinal + 10,
                  title: "Keep live title",
                }
              : item,
          );
          const events = mapHistory({ ...input, existing }, allocator);
          const repaired = recordsOf(events);
          assert.equal(events.length, 3);
          assert.equal(repaired.messages.length, 1);
          assert.equal(repaired.messages[0]?.id, existing.messages[1]?.id);
          assert.equal(repaired.messages[0]?.text, "The repository is clean.");
          assert.equal(repaired.messages[0]?.createdAt, existing.messages[1]?.createdAt);
          assert.isFalse(Object.hasOwn(repaired.messages[0]!, "textPrefixMissing"));
          assert.deepEqual(
            repaired.turnItems.map((item) => [item.id, "text" in item ? item.text : undefined]),
            existing.turnItems
              .filter((item) => item.type === "reasoning" || item.type === "assistant_message")
              .map((item) => [
                item.id,
                item.type === "reasoning" ? "Find the entry point" : "The repository is clean.",
              ]),
          );
          for (const item of repaired.turnItems) {
            const previous = existing.turnItems.find((candidate) => candidate.id === item.id)!;
            assert.equal(item.ordinal, previous.ordinal);
            assert.equal(item.nodeId, previous.nodeId);
            assert.equal(item.providerTurnId, previous.providerTurnId);
            assert.equal(item.title, previous.title);
            assert.isFalse(Object.hasOwn(item, "textPrefixMissing"));
          }
          const recovered = {
            ...existing,
            messages: existing.messages.map(
              (message) => repaired.messages.find((entry) => entry.id === message.id) ?? message,
            ),
            turnItems: existing.turnItems.map(
              (item) => repaired.turnItems.find((entry) => entry.id === item.id) ?? item,
            ),
          };
          assert.deepEqual(mapHistory({ ...input, existing: recovered }, allocator), []);
        }),
    );
    it.effect(
      "repairs completed thinking in an active turn without touching its assistant suffix",
      () =>
        Effect.gen(function* () {
          const allocator = yield* IdAllocator.IdAllocatorV2;
          const input = fixture(ProviderDriverKind.make(provider));
          const existing = recordsOf(mapHistory(input, allocator));
          existing.messages = existing.messages.map((message) =>
            message.role === "assistant"
              ? { ...message, text: "latest suffix", textPrefixMissing: true, streaming: true }
              : message,
          );
          existing.turnItems = existing.turnItems.map((item) =>
            item.type === "assistant_message" || item.type === "reasoning"
              ? { ...item, text: "latest suffix", textPrefixMissing: true, streaming: true }
              : item,
          );
          const turns = input.turns.map((turn) => ({
            ...turn,
            status: "running" as const,
            completedAt: null,
          }));
          const events = mapHistory({ ...input, turns, existing }, allocator);
          assert.equal(events.length, 1);
          const reasoning = recordsOf(events).turnItems[0];
          assert.equal(reasoning?.type, "reasoning");
          assert.equal(
            reasoning && "text" in reasoning ? reasoning.text : null,
            "Find the entry point",
          );
          assert.isFalse(Object.hasOwn(reasoning!, "textPrefixMissing"));
        }),
    );
    it.effect("keeps missing-text markers through unfinished and empty native history", () =>
      Effect.gen(function* () {
        const allocator = yield* IdAllocator.IdAllocatorV2;
        const input = fixture(ProviderDriverKind.make(provider));
        const existing = recordsOf(mapHistory(input, allocator));
        existing.messages = existing.messages.map((message) =>
          message.role === "assistant"
            ? { ...message, text: "latest suffix", textPrefixMissing: true }
            : message,
        );
        existing.turnItems = existing.turnItems.map((item) =>
          item.type === "assistant_message" || item.type === "reasoning"
            ? { ...item, text: "latest suffix", textPrefixMissing: true }
            : item,
        );
        for (const incomplete of ["unfinished", "empty"] as const) {
          const turns = input.turns.map((turn) => ({
            ...turn,
            items: turn.items.map((item) =>
              item.type === "assistant" || item.type === "reasoning"
                ? incomplete === "empty"
                  ? { ...item, text: "" }
                  : { ...item, status: "running" as const }
                : item,
            ),
          }));
          assert.deepEqual(mapHistory({ ...input, turns, existing }, allocator), []);
        }
      }),
    );
  });
}

it.layer(IdAllocator.layer)("Claude native fork identity isolation", (it) => {
  it.effect(
    "keeps copied native UUIDs distinct from the source and stable after metadata reload",
    () =>
      Effect.gen(function* () {
        const ids = yield* IdAllocator.IdAllocatorV2;
        const source = fixture(ProviderDriverKind.make("claudeAgent"));
        const target = {
          ...source,
          appThread: { ...source.appThread, id: ThreadId.make("fork-app") },
          providerThread: {
            ...source.providerThread,
            id: ProviderThreadId.make("fork-provider"),
            appThreadId: ThreadId.make("fork-app"),
            nativeMetadata: { claudeForkIdentityNamespace: "fork-session-uuid" },
            forkedFrom: { providerThreadId: source.providerThread.id },
          },
        };
        const original = recordsOf(mapHistory(source, ids));
        const fork = recordsOf(mapHistory(target, ids));
        const reloaded = recordsOf(
          mapHistory(
            {
              ...target,
              providerThread: {
                ...target.providerThread,
                nativeMetadata: JSON.parse(JSON.stringify(target.providerThread.nativeMetadata)),
              },
            },
            ids,
          ),
        );
        for (const key of ["messages", "turnItems", "providerTurns", "nodes", "runs"] as const) {
          assert.isFalse(
            fork[key].some((value) => original[key].some((entry) => entry.id === value.id)),
          );
          assert.deepEqual(
            fork[key].map((value) => value.id),
            reloaded[key].map((value) => value.id),
          );
        }
        assert.deepEqual(
          original.providerTurns[0]?.nativeTurnRef,
          fork.providerTurns[0]?.nativeTurnRef,
        );
        assert.deepEqual(
          original.turnItems.map((item) => item.nativeItemRef),
          fork.turnItems.map((item) => item.nativeItemRef),
        );
        assert.deepEqual(recordsOf(mapHistory(source, ids)), original);
        assert.equal(
          original.providerTurns[0]?.id,
          ids.derive.providerTurn({ driver: source.driver, nativeTurnId: source.turns[0]!.id }),
        );
      }),
  );
  it.effect(
    "projects native absorbed input as an additional user message in its original turn",
    () =>
      Effect.gen(function* () {
        const ids = yield* IdAllocator.IdAllocatorV2;
        const input = fixture(ProviderDriverKind.make("claudeAgent"));
        const turn = input.turns[0]!;
        const output = recordsOf(
          mapHistory(
            {
              ...input,
              turns: [
                {
                  ...turn,
                  items: [
                    ...turn.items,
                    {
                      id: "queued-input:delivery",
                      type: "user",
                      inputIntent: "steer",
                      title: "Additional CLI input",
                      text: "Also run tests",
                      status: "completed",
                    },
                  ],
                },
              ],
            },
            ids,
          ),
        );
        assert.equal(output.runs.length, 1);
        assert.equal(output.messages.filter((message) => message.role === "user").length, 2);
        assert.deepInclude(output.turnItems.at(-1), {
          type: "user_message",
          inputIntent: "steer",
          title: "Additional CLI input",
          text: "Also run tests",
        });
      }),
  );
});
