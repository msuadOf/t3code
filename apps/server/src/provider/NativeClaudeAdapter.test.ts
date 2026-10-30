import { NativeClaudeSessionOutput } from "./NativeClaudeSessionOutput.ts";
// @effect-diagnostics nodeBuiltinImport:off -- Isolated native MCP launch metadata fixture.
import * as NodeFSP from "node:fs/promises";
import { vi } from "vite-plus/test";
import { NativeClaudeSessions } from "./NativeClaudeSessions.ts";
import * as Layer from "effect/Layer";
import {
  makeProviderEventRoutingState,
  routeProviderEvent,
} from "../orchestration-v2/RunExecutionService.ts";
import * as Stream from "effect/Stream";
import * as Deferred from "effect/Deferred";
import * as TestClock from "effect/testing/TestClock";
import * as NodeServices from "@effect/platform-node/NodeServices";
import * as FileSystem from "effect/FileSystem";
import * as Path from "effect/Path";
import * as ProviderAdapter from "@t3tools/provider-core/server/ProviderAdapter";
import { describe, expect, it } from "@effect/vitest";
import {
  ProviderInstanceId,
  EnvironmentId,
  ProviderSessionId,
  ProviderThreadId,
  ProviderTurnId,
  ProviderDriverKind,
  ThreadId,
  ProjectId,
  RunId,
  RunAttemptId,
  NodeId,
  MessageId,
  type OrchestrationV2ProviderThread,
  type OrchestrationV2RuntimeRequest,
} from "@t3tools/contracts";
import { nativeSessionProjectionId } from "@t3tools/provider-core/server/nativeSessions";
import * as IdAllocator from "@t3tools/provider-core/server/IdAllocator";
import * as Effect from "effect/Effect";
import * as DateTime from "effect/DateTime";
import * as Exit from "effect/Exit";
import * as Cause from "effect/Cause";
import { NativeClaudeClient, type NativeClaudeSession } from "./NativeClaudeClient.ts";
import {
  makeNativeClaudeAdapter,
  nativeClaudeCapabilities,
  nativeClaudeMatchesPendingTurn,
  nativeClaudeObservedTurnIsTerminal,
} from "./NativeClaudeAdapter.ts";

import {
  NativeClaudeMcpBindings,
  readClaudeMcpRelayCredential,
} from "./NativeClaudeMcpBindings.ts";
import type { McpProviderSessionConfig } from "@t3tools/provider-core/server/mcpSession";

const instanceId = ProviderInstanceId.make("claude-native-test");
const threadId = ThreadId.make("thread-test");
const modelSelection = { instanceId, model: "claude-test" };
const runtimePolicy = {
  cwd: "/work",
  runtimeMode: "approval-required" as const,
  interactionMode: "default" as const,
};
const nativeId = "1234abcd-1234-1234-1234-123456789abc";
class FixtureClient extends NativeClaudeClient {
  readonly inventory: readonly NativeClaudeSession[];
  readonly resumes: string[] = [];
  subscriptions = 0;
  cancelled = false;
  private onSubscribe: (() => void) | undefined;
  readonly subscribed = new Promise<void>((resolve) => {
    this.onSubscribe = resolve;
  });
  override async *subscribe(
    _session: NativeClaudeSession,
    signal?: AbortSignal,
  ): ReturnType<NativeClaudeClient["subscribe"]> {
    this.subscriptions += 1;
    this.onSubscribe?.();
    yield { type: "snapshot", record: {} };
    await new Promise<void>((resolve) => {
      if (signal?.aborted) resolve();
      else
        signal?.addEventListener(
          "abort",
          () => {
            this.cancelled = true;
            resolve();
          },
          { once: true },
        );
    });
  }

  override async startBackground(input: Parameters<NativeClaudeClient["startBackground"]>[0]) {
    if (input.sessionId) this.resumes.push(input.sessionId);
    return super.startBackground(input);
  }
  constructor(kind: "interactive" | "background") {
    super();
    this.inventory = [
      {
        kind,
        id: "1234abcd",
        sessionId: nativeId,
        cwd: "/work",
        startedAt: 1,
        pid: 42,
        status: "idle",
      },
    ];
  }
  override async listSessions() {
    return this.inventory;
  }
}

const fixtureAppThread = (
  now: DateTime.Utc,
  providerThreadId: ProviderThreadId,
): ProviderAdapter.ProviderAdapterV2TurnInput["appThread"] => ({
  id: threadId,
  projectId: ProjectId.make("project-test"),
  title: "Shared",
  createdBy: "user",
  creationSource: "web",
  providerInstanceId: instanceId,
  modelSelection,
  runtimeMode: "approval-required",
  interactionMode: "default",
  branch: null,
  worktreePath: null,
  activeProviderThreadId: providerThreadId,
  lineage: { rootThreadId: threadId, parentThreadId: null, relationshipToParent: null },
  forkedFrom: null,
  createdAt: now,
  updatedAt: now,
  archivedAt: null,
  settledOverride: null,
  settledAt: null,
  lastVisitedAt: null,
  deletedAt: null,
});

it.layer(IdAllocator.layer)("Claude native adapter", (it) => {
  it.effect("attaches an existing native background worker without query/resume subprocesses", () =>
    Effect.scoped(
      Effect.gen(function* () {
        const client = new FixtureClient("background");
        const adapter = yield* makeNativeClaudeAdapter({
          instanceId,
          binaryPath: "never-executed",
          configDirectory: "/nonexistent-claude-fixture",
          environment: {},
          client,
        });
        const runtime = yield* adapter.openSession({
          threadId,
          providerSessionId: ProviderSessionId.make("session-test"),
          modelSelection,
          runtimePolicy,
        });
        const now = DateTime.makeUnsafe(1);
        const existingProviderThread = {
          id: ProviderThreadId.make("provider-thread-test"),
          driver: ProviderDriverKind.make("claudeAgent"),
          providerInstanceId: instanceId,
          providerSessionId: runtime.providerSessionId,
          appThreadId: threadId,
          nativeThreadRef: {
            driver: ProviderDriverKind.make("claudeAgent"),
            nativeId,
            strength: "strong" as const,
          },
          nativeConversationHeadRef: null,
          status: "idle" as const,
          ownerNodeId: null,
          firstRunOrdinal: null,
          lastRunOrdinal: null,
          handoffIds: [],
          forkedFrom: null,
          createdAt: now,
          updatedAt: now,
        };
        const result = yield* runtime.ensureThread({
          threadId,
          modelSelection,
          runtimePolicy,
          existingProviderThread,
        });
        expect(result.nativeThreadRef?.nativeId).toBe(nativeId);
        expect(result.id).toBe(existingProviderThread.id);
        const send = yield* Effect.exit(
          runtime.startTurn({
            threadId,
            providerThread: result,
            modelSelection,
            runtimePolicy,
            runId: RunId.make("run-test"),
            runOrdinal: 1,
            providerTurnOrdinal: 1,
            attemptId: RunAttemptId.make("attempt-test"),
            rootNodeId: NodeId.make("node-test"),
            message: {
              createdBy: "user",
              creationSource: "web",
              messageId: MessageId.make("message-test"),
              text: "Continue",
              attachments: [],
            },
            appThread: fixtureAppThread(now, result.id),
          }),
        );
        // The absent transcript stops sending, but an observed thread must first reclaim its worker.
        expect(Exit.isFailure(send)).toBe(true);
        expect(client.resumes).toEqual([nativeId, nativeId]);
        const approval = yield* Effect.exit(
          runtime.respondToRuntimeRequest({ requestId: "unused" as never }),
        );
        expect(Exit.isFailure(approval)).toBe(true);
        if (Exit.isFailure(approval))
          expect(String(Cause.squash(approval.cause))).toContain("no approval was sent");
      }),
    ),
  );

  it.effect(
    "observes a live foreground session but explains backgrounding instead of spawning a second writer",
    () =>
      Effect.scoped(
        Effect.gen(function* () {
          const adapter = yield* makeNativeClaudeAdapter({
            instanceId,
            binaryPath: "never-executed",
            configDirectory: "/nonexistent-claude-fixture",
            environment: {},
            client: new FixtureClient("interactive"),
          });
          const runtime = yield* adapter.openSession({
            threadId,
            providerSessionId: ProviderSessionId.make("session-test"),
            modelSelection,
            runtimePolicy,
          });
          const now = DateTime.makeUnsafe(1);
          const existingProviderThread = {
            id: ProviderThreadId.make("provider-thread-test"),
            driver: ProviderDriverKind.make("claudeAgent"),
            providerInstanceId: instanceId,
            providerSessionId: runtime.providerSessionId,
            appThreadId: threadId,
            nativeThreadRef: {
              driver: ProviderDriverKind.make("claudeAgent"),
              nativeId,
              strength: "strong" as const,
            },
            nativeConversationHeadRef: null,
            status: "idle" as const,
            ownerNodeId: null,
            firstRunOrdinal: null,
            lastRunOrdinal: null,
            handoffIds: [],
            forkedFrom: null,
            createdAt: now,
            updatedAt: now,
          };
          const observed = yield* runtime.resumeThread({
            providerThread: existingProviderThread,
            threadId,
          });
          expect(observed.nativeThreadRef?.nativeId).toBe(nativeId);
          const outcome = yield* Effect.exit(
            runtime.ensureThread({
              threadId,
              modelSelection,
              runtimePolicy,
              existingProviderThread,
            }),
          );
          expect(Exit.isFailure(outcome)).toBe(true);
          if (Exit.isFailure(outcome))
            expect(String(Cause.squash(outcome.cause))).toContain("/background");
        }),
      ),
  );
});

describe("Claude native capability boundary", () => {
  it("does not advertise token deltas or a structured permission control that native daemon does not provide", () => {
    expect(nativeClaudeCapabilities.streaming.streamsReasoning).toBe(false);
    expect(nativeClaudeCapabilities.approvals.supportsCommandApproval).toBe(false);
    expect(nativeClaudeCapabilities.turns.supportsInterrupt).toBe(true);
  });
});

describe("native reply correlation", () => {
  it("does not assign an external CLI turn inserted before reply acknowledgement to a T3 request", () => {
    const pending = { baseline: new Set(["existing"]), expectedText: "T3 prompt" };
    const external = { id: "CLI-before-ack", items: [{ type: "user", text: "CLI prompt" }] };
    const own = { id: "T3-after-ack", items: [{ type: "user", text: "T3 prompt" }] };
    expect(nativeClaudeMatchesPendingTurn(external, pending)).toBe(false);
    expect(nativeClaudeMatchesPendingTurn(own, pending)).toBe(true);
    expect(nativeClaudeMatchesPendingTurn({ ...own, id: "existing" }, pending)).toBe(false);
  });
});

it("settles foreground observations from canonical turns without background daemon status", () => {
  const foreground: NativeClaudeSession = {
    kind: "interactive",
    cwd: "/work",
    startedAt: 1,
    sessionId: nativeId,
    pid: 42,
  };
  expect(
    nativeClaudeObservedTurnIsTerminal({ status: "running", completedAt: null }, foreground),
  ).toBe(false);
  expect(
    nativeClaudeObservedTurnIsTerminal({ status: "completed", completedAt: 2 }, foreground),
  ).toBe(true);
  expect(
    nativeClaudeObservedTurnIsTerminal({ status: "running", completedAt: null }, undefined),
  ).toBe(false);
  expect(
    nativeClaudeObservedTurnIsTerminal({ status: "interrupted", completedAt: null }, undefined),
  ).toBe(true);
  expect(
    nativeClaudeObservedTurnIsTerminal(
      { status: "running", completedAt: null },
      { ...foreground, kind: "background", id: "1234abcd", status: "waiting" },
    ),
  ).toBe(false);
});

it("settles an acknowledged interrupt only after the native worker returns to idle", () => {
  const turn = { status: "running", completedAt: null };
  const job: NativeClaudeSession = {
    kind: "background",
    cwd: "/work",
    startedAt: 1,
    sessionId: nativeId,
    pid: 42,
    status: "busy",
  };
  expect(nativeClaudeObservedTurnIsTerminal(turn, job, true)).toBe(false);
  expect(nativeClaudeObservedTurnIsTerminal(turn, { ...job, status: "idle" }, false)).toBe(false);
  expect(nativeClaudeObservedTurnIsTerminal(turn, { ...job, status: "idle" }, true)).toBe(true);
});

// Reopening an observer must recover the same native turn even if it ended while T3 was down.
it.effect.each(["completed", "failed", "interrupted"] as const)(
  "recovers an offline %s native turn without sending input or replaying older history",
  (status) =>
    Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem;
      const path = yield* Path.Path;
      const root = yield* fs.makeTempDirectoryScoped();
      const directory = path.join(root, "projects", "workspace");
      yield* fs.makeDirectory(directory, { recursive: true });
      const entries = [
        { type: "user", uuid: "old-user", parentUuid: null, message: { content: "old" } },
        {
          type: "assistant",
          uuid: "old-answer",
          parentUuid: "old-user",
          message: { content: "old answer", stop_reason: "end_turn" },
        },
        {
          type: "user",
          uuid: "current-user",
          parentUuid: "old-answer",
          message: { content: "current" },
        },
        {
          type: "assistant",
          uuid: "current-answer",
          parentUuid: "current-user",
          message: { content: "current answer", stop_reason: "end_turn" },
        },
      ];
      yield* fs.writeFileString(
        path.join(directory, `${nativeId}.jsonl`),
        entries
          .map((entry) =>
            JSON.stringify({
              ...entry,
              sessionId: nativeId,
              cwd: "/work",
              timestamp: "2026-10-23T00:00:01Z",
            }),
          )
          .join("\n") + "\n",
      );
      const client = new FixtureClient("background");
      const job = client.inventory[0]!;
      // The same worker is idle or has stopped; its final JSONL record is already durable.
      Object.assign(job, {
        state: status === "failed" ? "failed" : status === "interrupted" ? "stopped" : "done",
      });
      const adopted: ProviderAdapter.ProviderAdapterV2ExternalTurnInput[] = [];
      const adapter = yield* makeNativeClaudeAdapter({
        instanceId,
        configDirectory: root,
        binaryPath: "never-executed",
        environment: {},
        client,
      });
      const runtime = yield* adapter.openSession({
        threadId,
        providerSessionId: ProviderSessionId.make("recovery-session"),
        modelSelection,
        runtimePolicy,
        onExternalTurn: (turn) =>
          Effect.sync(() => {
            adopted.push(turn);
            return undefined;
          }),
      });
      const now = DateTime.makeUnsafe(1);
      const providerThread = {
        id: ProviderThreadId.make("recovery-thread"),
        driver: ProviderDriverKind.make("claudeAgent"),
        providerInstanceId: instanceId,
        providerSessionId: runtime.providerSessionId,
        appThreadId: threadId,
        nativeThreadRef: {
          driver: ProviderDriverKind.make("claudeAgent"),
          nativeId,
          strength: "strong" as const,
        },
        nativeConversationHeadRef: null,
        status: "active" as const,
        ownerNodeId: null,
        firstRunOrdinal: null,
        lastRunOrdinal: null,
        handoffIds: [],
        forkedFrom: null,
        createdAt: now,
        updatedAt: now,
      };
      yield* runtime.resumeThread({ threadId, providerThread });
      expect(adopted).toHaveLength(1);
      expect(adopted[0]).toMatchObject({
        nativeTurnId: "current-user",
        nativeStatus: status,
        snapshot: true,
      });
      yield* runtime.resumeThread({ threadId, providerThread });
      expect(adopted).toHaveLength(1);
      expect(client.resumes).toEqual([]);
      yield* Effect.promise(() => client.subscribed);
      expect(client.subscriptions).toBe(1);
      // Closing the observer's scope owns no worker teardown or input operation.
      expect((yield* adapter.getCapabilities()).sessions.runtimeSurvivesHostRestart).toBe(true);
    }).pipe(Effect.scoped, Effect.provide(Layer.mergeAll(NodeServices.layer, IdAllocator.layer))),
);

it.effect.each(["resumed", "ended"] as const)(
  "updates the same waiting notice when the native worker %s without duplicate hydration notices",
  (transition) =>
    Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem;
      const path = yield* Path.Path;
      const root = yield* fs.makeTempDirectoryScoped();
      const directory = path.join(root, "projects", "workspace");
      yield* fs.makeDirectory(directory, { recursive: true });
      const file = path.join(directory, `${nativeId}.jsonl`);
      const user = {
        type: "user",
        uuid: "waiting-user",
        parentUuid: null,
        sessionId: nativeId,
        cwd: "/work",
        timestamp: "2026-10-23T00:00:01Z",
        message: { content: "work" },
      };
      yield* fs.writeFileString(file, JSON.stringify(user) + "\n");
      const client = new FixtureClient("background");
      Object.assign(client.inventory[0]!, {
        status: "waiting",
        state: "blocked",
        waitingFor: "unknown-native-reason",
      });
      const adapter = yield* makeNativeClaudeAdapter({
        instanceId,
        configDirectory: root,
        binaryPath: "never-executed",
        environment: {},
        client,
      });
      const now = DateTime.makeUnsafe(1);
      const providerThread: OrchestrationV2ProviderThread = {
        id: ProviderThreadId.make("waiting-thread"),
        driver: ProviderDriverKind.make("claudeAgent"),
        providerInstanceId: instanceId,
        providerSessionId: ProviderSessionId.make("waiting-session"),
        appThreadId: threadId,
        nativeThreadRef: {
          driver: ProviderDriverKind.make("claudeAgent"),
          nativeId,
          strength: "strong",
        },
        nativeConversationHeadRef: null,
        status: "active",
        ownerNodeId: null,
        firstRunOrdinal: null,
        lastRunOrdinal: null,
        handoffIds: [],
        forkedFrom: null,
        createdAt: now,
        updatedAt: now,
      };
      const runtime = yield* adapter.openSession({
        threadId,
        providerSessionId: ProviderSessionId.make("waiting-session"),
        modelSelection,
        runtimePolicy,
        onExternalTurn: () =>
          Effect.succeed({
            threadId,
            providerThread,
            modelSelection,
            runtimePolicy,
            runId: RunId.make("waiting-run"),
            runOrdinal: 1,
            providerTurnOrdinal: 1,
            attemptId: RunAttemptId.make("waiting-attempt"),
            rootNodeId: NodeId.make("waiting-root"),
            message: {
              createdBy: "user",
              creationSource: "web",
              messageId: MessageId.make("waiting-message"),
              text: "work",
              attachments: [],
            },
            appThread: fixtureAppThread(now, providerThread.id),
          }),
      });
      // Pull directly from the runtime queue after each awaited hydration. Every
      // registration publishes an event, so missing notices fail assertions, not timeouts.
      const pull = yield* Stream.toPull(runtime.events);
      const notices = (events: ReadonlyArray<ProviderAdapter.ProviderAdapterV2Event>) =>
        events.flatMap((event) =>
          event.type === "turn_item.updated" && event.turnItem.type === "system_notice"
            ? [event.turnItem]
            : [],
        );
      yield* runtime.resumeThread({ threadId, providerThread });
      const waiting = notices(yield* pull);
      expect(waiting).toHaveLength(1);
      expect(waiting[0]).toMatchObject({
        status: "running",
        nativeItemRef: null,
        completedAt: null,
      });
      expect(waiting[0]!.message).toContain("claude attach 1234abcd");
      yield* runtime.resumeThread({ threadId, providerThread });
      expect(notices(yield* pull)).toEqual([]);
      if (transition === "ended") {
        Object.assign(client.inventory[0]!, { status: "idle", state: "done" });
        yield* fs.writeFileString(
          file,
          JSON.stringify(user) +
            "\n" +
            JSON.stringify({
              type: "attachment",
              uuid: "waiting-extra-input",
              parentUuid: "waiting-user",
              attachment: {
                type: "queued_command",
                commandMode: "prompt",
                humanTurn: true,
                origin: { kind: "human" },
                delivery_id: "waiting-delivery",
                prompt: "Also run tests",
              },
            }) +
            "\n" +
            JSON.stringify({
              type: "assistant",
              uuid: "waiting-answer",
              parentUuid: "waiting-extra-input",
              sessionId: nativeId,
              timestamp: "2026-10-23T00:00:02Z",
              message: { content: "done", stop_reason: "end_turn" },
            }) +
            "\n",
        );
      } else Object.assign(client.inventory[0]!, { status: "working", state: "running" });
      yield* runtime.resumeThread({ threadId, providerThread });
      const resumedEvents = yield* pull;
      const completed = notices(resumedEvents);
      if (transition === "ended") {
        const additionalInput = resumedEvents.flatMap((event) =>
          event.type === "message.updated" && event.message.role === "user" ? [event.message] : [],
        );
        expect(additionalInput).toHaveLength(1);
        expect(additionalInput[0]).toMatchObject({
          runId: RunId.make("waiting-run"),
          text: "Also run tests",
          createdBy: "user",
          creationSource: "provider",
        });
        expect(resumedEvents).toContainEqual(
          expect.objectContaining({
            type: "turn_item.updated",
            turnItem: expect.objectContaining({
              type: "user_message",
              inputIntent: "steer",
              messageId: additionalInput[0]!.id,
            }),
          }),
        );
      }
      expect(completed).toHaveLength(1);
      expect(completed[0]).toMatchObject({
        id: waiting[0]!.id,
        status: "completed",
        startedAt: waiting[0]!.startedAt,
      });
      expect(completed[0]!.completedAt).not.toBeNull();
      expect(completed[0]!.message).not.toContain("claude attach");
      expect(completed[0]!.message).toContain(
        transition === "ended" ? "turn has ended" : "continued",
      );
      yield* runtime.resumeThread({ threadId, providerThread });
      expect(notices(yield* pull)).toEqual([]);
    }).pipe(Effect.scoped, Effect.provide(Layer.mergeAll(NodeServices.layer, IdAllocator.layer))),
);

it.effect.each([
  "answer",
  "multi-answer",
  "stale",
  "cli",
  "no-actual",
  "read-failure",
  "restart",
  "restart-uncertain",
  "restart-unsubmitted",
  "restart-completed",
  "restart-resolved",
  "multiple",
  "became-multiple",
] as const)("handles a native question through %s without resolving on routing ACK", (mode) =>
  Effect.gen(function* () {
    const fs = yield* FileSystem.FileSystem;
    const path = yield* Path.Path;
    const root = yield* fs.makeTempDirectoryScoped();
    const directory = path.join(root, "projects", "workspace");
    yield* fs.makeDirectory(directory, { recursive: true });
    const file = path.join(directory, `${nativeId}.jsonl`);
    const multi = mode === "multi-answer";
    const actualAnswer = multi ? ['Green "forest"', "Blue, Navy"] : "Green";
    const nativeActualAnswer = multi ? '"Green \\"forest\\"", "Blue, Navy"' : "Green";
    const question = {
      questions: [
        {
          question: "Choose color",
          header: "Color",
          options: [
            { label: multi ? "Blue, Navy" : "Blue", description: "Use blue" },
            { label: multi ? 'Green "forest"' : "Green", description: "Use green" },
          ],
          multiSelect: multi,
        },
      ],
    };
    const entries = [
      {
        type: "user",
        uuid: "q-user",
        parentUuid: null,
        sessionId: nativeId,
        cwd: "/work",
        message: { content: "ask" },
      },
      {
        type: "assistant",
        uuid: "q-tool",
        parentUuid: "q-user",
        sessionId: nativeId,
        cwd: "/work",
        message: {
          content: [
            { type: "tool_use", id: "question-tool", name: "AskUserQuestion", input: question },
          ],
          stop_reason: "tool_use",
        },
      },
    ];
    const multipleEntries = entries.map((entry) =>
      entry.type === "assistant" && Array.isArray(entry.message.content)
        ? {
            ...entry,
            message: {
              ...entry.message,
              content: [
                ...entry.message.content,
                {
                  type: "tool_use",
                  id: "second-question-tool",
                  name: "AskUserQuestion",
                  input: question,
                },
              ],
            },
          }
        : entry,
    );
    yield* fs.writeFileString(
      file,
      (mode === "multiple" ? multipleEntries : entries)
        .map((entry) => JSON.stringify(entry))
        .join("\n") + "\n",
    );
    class Client extends FixtureClient {
      replies: string[] = [];
      override async reply(_job: NativeClaudeSession, text: string) {
        this.replies.push(text);
      }
    }
    const client = new Client("background");
    Object.assign(client.inventory[0]!, { status: "waiting", state: "blocked" });
    const now = DateTime.makeUnsafe(1);
    const adapter = yield* makeNativeClaudeAdapter({
      instanceId,
      configDirectory: root,
      binaryPath: "never-executed",
      environment: {},
      client,
    });
    const sessionInput: ProviderAdapter.ProviderAdapterV2OpenSessionInput = {
      threadId,
      providerSessionId: ProviderSessionId.make("question-session"),
      modelSelection,
      runtimePolicy,
      onExternalTurn: (): Effect.Effect<ProviderAdapter.ProviderAdapterV2TurnInput> =>
        Effect.succeed({
          threadId,
          providerThread: providerThread,
          modelSelection,
          runtimePolicy,
          runId: RunId.make("question-run"),
          runOrdinal: 1,
          providerTurnOrdinal: 1,
          attemptId: RunAttemptId.make("question-attempt"),
          rootNodeId: NodeId.make("question-root"),
          message: {
            createdBy: "user",
            creationSource: "web",
            messageId: MessageId.make("question-message"),
            text: "ask",
            attachments: [],
          },
          appThread: fixtureAppThread(now, ProviderThreadId.make("question-thread")),
        }),
    };
    const runtime = yield* adapter.openSession(sessionInput);
    const providerThread: OrchestrationV2ProviderThread = {
      id: ProviderThreadId.make("question-thread"),
      driver: ProviderDriverKind.make("claudeAgent"),
      providerInstanceId: instanceId,
      providerSessionId: runtime.providerSessionId,
      appThreadId: threadId,
      nativeThreadRef: {
        driver: ProviderDriverKind.make("claudeAgent"),
        nativeId,
        strength: "strong" as const,
      },
      nativeConversationHeadRef: null,
      status: "active" as const,
      ownerNodeId: null,
      firstRunOrdinal: null,
      lastRunOrdinal: null,
      handoffIds: [],
      forkedFrom: null,
      createdAt: now,
      updatedAt: now,
    };
    const pendingPublication = yield* Deferred.make<void>();
    const resolved = yield* Deferred.make<OrchestrationV2RuntimeRequest>();
    const terminal = yield* Deferred.make<void>();
    const deliveredEvents: ProviderAdapter.ProviderAdapterV2Event[] = [];
    yield* Stream.runForEach(runtime.events, (event) =>
      Effect.gen(function* () {
        deliveredEvents.push(event);
        if (event.type === "runtime_request.updated" && event.runtimeRequest.status === "pending")
          yield* Deferred.succeed(pendingPublication, undefined);
        if (event.type === "runtime_request.updated" && event.runtimeRequest.status === "resolved")
          yield* Deferred.succeed(resolved, event.runtimeRequest);
        if (event.type === "provider_turn.updated" && event.providerTurn.status === "completed")
          yield* Deferred.succeed(terminal, undefined);
      }),
    ).pipe(Effect.forkScoped);
    yield* runtime.resumeThread({ threadId, providerThread });
    const snapshot = yield* runtime.readThreadSnapshot({ providerThread });
    if (mode === "multiple") {
      expect(snapshot.runtimeRequests).toEqual([]);
      expect(client.replies).toEqual([]);
      return;
    }
    const request = snapshot.runtimeRequests[0]!;
    yield* Deferred.await(pendingPublication);
    const pendingIndex = deliveredEvents.findIndex(
      (event) =>
        event.type === "runtime_request.updated" && event.runtimeRequest.status === "pending",
    );
    const runningIndex = deliveredEvents.findIndex(
      (event) => event.type === "provider_turn.updated" && event.providerTurn.status === "running",
    );
    expect(runningIndex).toBeGreaterThanOrEqual(0);
    expect(pendingIndex).toBeGreaterThan(runningIndex);
    const identity = {
      threadId,
      runId: RunId.make("question-run"),
      attemptId: RunAttemptId.make("question-attempt"),
      providerThreadId: providerThread.id,
    };
    let routing = makeProviderEventRoutingState({ identity, providerTurnId: null });
    let acceptedPending = false;
    for (const event of deliveredEvents.slice(0, pendingIndex + 1)) {
      const [accepted, next] = routeProviderEvent(event, identity, routing);
      routing = next;
      if (event.type === "runtime_request.updated" && event.runtimeRequest.id === request.id)
        acceptedPending = accepted;
    }
    expect(acceptedPending).toBe(true);

    expect(request).toMatchObject({
      status: "pending",
      kind: "user_input",
      nativeRequestRef: { nativeId: "question-tool" },
    });
    if (
      mode === "restart" ||
      mode === "restart-uncertain" ||
      mode === "restart-unsubmitted" ||
      mode === "restart-completed" ||
      mode === "restart-resolved"
    ) {
      const persisted: OrchestrationV2RuntimeRequest =
        mode === "restart-unsubmitted"
          ? request
          : {
              ...request,
              submittedAt: now,
              answers: { "Choose color": "Blue" },
              ...(mode === "restart-uncertain"
                ? {
                    status: "cancelled",
                    resolvedAt: now,
                    responseCapability: { type: "not_resumable", reason: "Delivery was uncertain" },
                  }
                : {}),
            };
      if (mode === "restart-completed") {
        yield* fs.writeFileString(
          file,
          entries.map((entry) => JSON.stringify(entry)).join("\n") +
            "\n" +
            JSON.stringify({
              type: "user",
              uuid: "offline-result",
              parentUuid: "q-tool",
              sessionId: nativeId,
              toolUseResult: { answers: { "Choose color": "Green" } },
              message: {
                content: [
                  { type: "tool_result", tool_use_id: "question-tool", content: "Native answer" },
                ],
              },
            }) +
            "\n" +
            JSON.stringify({
              type: "assistant",
              uuid: "offline-final",
              parentUuid: "offline-result",
              sessionId: nativeId,
              message: { content: [{ type: "text", text: "done" }], stop_reason: "end_turn" },
            }) +
            "\n",
        );
        Object.assign(client.inventory[0]!, { status: "idle", state: "done" });
      }
      const reopened = yield* adapter.openSession({
        ...sessionInput,
        findNativeRuntimeRequest: () =>
          Effect.succeed(
            mode === "restart-resolved"
              ? { ...persisted, status: "resolved", resolvedAt: now }
              : persisted,
          ),
      });
      const nativeResolution = yield* Deferred.make<OrchestrationV2RuntimeRequest>();
      yield* Stream.runForEach(reopened.events, (event) =>
        event.type === "runtime_request.updated" && event.runtimeRequest.status === "resolved"
          ? Deferred.succeed(nativeResolution, event.runtimeRequest).pipe(Effect.asVoid)
          : Effect.void,
      ).pipe(Effect.forkScoped);
      yield* reopened.resumeThread({ threadId, providerThread });
      if (mode === "restart-completed" || mode === "restart-resolved") {
        expect((yield* reopened.readThreadSnapshot({ providerThread })).runtimeRequests).toEqual(
          [],
        );
        if (mode === "restart-completed") {
          const completedRequest = yield* Deferred.await(nativeResolution);
          expect(completedRequest.id).toBe(request.id);
          expect(completedRequest.answers).toEqual({ "Choose color": "Green" });
        }
        expect(client.replies).toEqual([]);
        return;
      }

      const recovered = (yield* reopened.readThreadSnapshot({ providerThread }))
        .runtimeRequests[0]!;
      expect(recovered.id).toBe(request.id);
      expect(recovered.nodeId).toBe(request.nodeId);
      expect(recovered.status).toBe(persisted.status);
      expect(recovered.submittedAt).toBe(persisted.submittedAt);
      const response = yield* Effect.exit(
        reopened.respondToRuntimeRequest({
          requestId: request.id,
          answers: { "Choose color": "Green" },
        }),
      );
      expect(Exit.isFailure(response)).toBe(mode !== "restart-unsubmitted");
      expect(client.replies).toEqual(mode === "restart-unsubmitted" ? ["Green"] : []);
      return;
    }
    if (mode === "read-failure") {
      yield* fs.writeFileString(file, "{broken}\n");
      yield* runtime.resumeThread({ threadId, providerThread });
      yield* fs.writeFileString(
        file,
        entries.map((entry) => JSON.stringify(entry)).join("\n") + "\n",
      );
    }
    if (mode === "stale" || mode === "became-multiple") {
      if (mode === "stale") Object.assign(client.inventory[0]!, { status: "idle" });
      else
        yield* fs.writeFileString(
          file,
          multipleEntries.map((entry) => JSON.stringify(entry)).join("\n") + "\n",
        );
      const failed = yield* Effect.exit(
        runtime.respondToRuntimeRequest({
          requestId: request.id,
          answers: { "Choose color": "Blue" },
        }),
      );
      expect(Exit.isFailure(failed)).toBe(true);
      expect(client.replies).toEqual([]);
      expect(
        (yield* runtime.readThreadSnapshot({ providerThread })).runtimeRequests[0]?.status,
      ).toBe("cancelled");
      return;
    }
    if (
      mode === "answer" ||
      mode === "multi-answer" ||
      mode === "no-actual" ||
      mode === "read-failure"
    ) {
      yield* runtime.respondToRuntimeRequest({
        requestId: request.id,
        answers: { "Choose color": multi ? ["Blue, Navy", 'Green "forest"'] : ["blue"] },
      });
      expect(client.replies).toEqual([multi ? '"Blue, Navy", "Green \\"forest\\""' : "Blue"]);
      expect(
        (yield* runtime.readThreadSnapshot({ providerThread })).runtimeRequests[0]?.status,
      ).toBe("pending");
      expect(
        Exit.isFailure(
          yield* Effect.exit(
            runtime.respondToRuntimeRequest({
              requestId: request.id,
              answers: { "Choose color": "Green" },
            }),
          ),
        ),
      ).toBe(true);
      expect(client.replies).toHaveLength(1);
    }
    yield* fs.writeFileString(
      file,
      entries.map((entry) => JSON.stringify(entry)).join("\n") +
        "\n" +
        JSON.stringify({
          type: "user",
          uuid: "q-answer",
          ...(mode === "no-actual"
            ? {}
            : { toolUseResult: { answers: { "Choose color": nativeActualAnswer } } }),
          parentUuid: "q-tool",
          sessionId: nativeId,
          cwd: "/work",
          message: {
            content: [
              {
                type: "tool_result",
                tool_use_id: "question-tool",
                content: '"Choose color"="Blue"',
              },
            ],
          },
        }) +
        "\n" +
        JSON.stringify({
          type: "assistant",
          uuid: "q-final",
          parentUuid: "q-answer",
          sessionId: nativeId,
          cwd: "/work",
          timestamp: "2026-10-28T00:00:01Z",
          message: { content: [{ type: "text", text: "done" }], stop_reason: "end_turn" },
        }) +
        "\n",
    );
    Object.assign(client.inventory[0]!, { status: "idle", state: "done" });
    yield* runtime.resumeThread({ threadId, providerThread });
    yield* Deferred.await(terminal);
    const resolvedIndex = deliveredEvents.findIndex(
      (event) =>
        event.type === "runtime_request.updated" && event.runtimeRequest.status === "resolved",
    );
    const terminalIndex = deliveredEvents.findIndex(
      (event) =>
        event.type === "provider_turn.updated" && event.providerTurn.status === "completed",
    );
    expect(resolvedIndex).toBeGreaterThanOrEqual(0);
    expect(resolvedIndex).toBeLessThan(terminalIndex);
    const completedItem = deliveredEvents.find(
      (event) =>
        event.type === "turn_item.updated" &&
        event.turnItem.type === "user_input_request" &&
        event.turnItem.status === "completed",
    );
    expect(
      completedItem?.type === "turn_item.updated" &&
        completedItem.turnItem.type === "user_input_request"
        ? completedItem.turnItem.questionAnswer?.answers
        : undefined,
    ).toEqual(mode === "no-actual" ? undefined : { "Choose color": actualAnswer });
    expect((yield* runtime.readThreadSnapshot({ providerThread })).runtimeRequests).toEqual([]);
    expect((yield* Deferred.await(resolved)).answers).toEqual(
      mode === "no-actual" ? undefined : { "Choose color": actualAnswer },
    );
  }).pipe(Effect.scoped, Effect.provide(Layer.mergeAll(NodeServices.layer, IdAllocator.layer))),
);

it.effect("keeps a replacement watcher alive when its predecessor's scope closes", () =>
  Effect.gen(function* () {
    let release: (() => void) | undefined;
    const advance = new Promise<void>((resolve) => {
      release = resolve;
    });
    let secondReady: (() => void) | undefined;
    const second = new Promise<void>((resolve) => {
      secondReady = resolve;
    });
    let firstClosed: (() => void) | undefined;
    const closed = new Promise<void>((resolve) => {
      firstClosed = resolve;
    });
    const controllers: AbortSignal[] = [];
    const job = {
      kind: "background" as const,
      id: "1234abcd",
      sessionId: nativeId,
      cwd: "/work",
      startedAt: 1,
      pid: 42,
      status: "idle",
    };
    class Client extends NativeClaudeClient {
      override async listSessions() {
        return [job];
      }
      override async *subscribe(_job: NativeClaudeSession, signal?: AbortSignal) {
        const index = controllers.length;
        controllers.push(signal!);
        if (index === 1) secondReady?.();
        yield { type: "snapshot", record: {} };
        try {
          if (index === 0) {
            await advance;
            yield { type: "state", patch: { tempo: "active" } };
          }
          await new Promise<void>((resolve) => {
            if (signal?.aborted) resolve();
            else signal?.addEventListener("abort", () => resolve(), { once: true });
          });
        } finally {
          if (index === 0) firstClosed?.();
        }
      }
    }
    const adapter = yield* makeNativeClaudeAdapter({
      instanceId,
      configDirectory: "/nonexistent",
      binaryPath: "never-executed",
      environment: {},
      client: new Client(),
    });
    const runtime = yield* adapter.openSession({
      threadId,
      providerSessionId: ProviderSessionId.make("watch-scope"),
      modelSelection,
      runtimePolicy,
    });
    const now = DateTime.makeUnsafe(1);
    const providerThread: OrchestrationV2ProviderThread = {
      id: ProviderThreadId.make("watch-scope-thread"),
      driver: ProviderDriverKind.make("claudeAgent"),
      providerInstanceId: instanceId,
      providerSessionId: runtime.providerSessionId,
      appThreadId: threadId,
      nativeThreadRef: {
        driver: ProviderDriverKind.make("claudeAgent"),
        nativeId,
        strength: "strong",
      },
      nativeConversationHeadRef: null,
      status: "idle",
      ownerNodeId: null,
      firstRunOrdinal: null,
      lastRunOrdinal: null,
      handoffIds: [],
      forkedFrom: null,
      createdAt: now,
      updatedAt: now,
    };
    yield* runtime.resumeThread({ threadId, providerThread });
    job.pid = 43;
    release?.();
    yield* Effect.promise(() => second);
    yield* Effect.promise(() => closed);
    expect(controllers[0]?.aborted).toBe(true);
    expect(controllers[1]?.aborted).toBe(false);
  }).pipe(Effect.scoped, Effect.provide(Layer.mergeAll(NodeServices.layer, IdAllocator.layer))),
);

it.effect(
  "hydrates native todo and proposed-plan artifacts without replacing the original tool records",
  () =>
    Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem;
      const path = yield* Path.Path;
      const root = yield* fs.makeTempDirectoryScoped();
      const directory = path.join(root, "projects", "workspace");
      yield* fs.makeDirectory(directory, { recursive: true });
      const entries: unknown[] = [
        { type: "user", uuid: "planning-user", parentUuid: null, message: { content: "plan" } },
      ];
      let parent = "planning-user";
      for (const [id, name, input, result] of [
        [
          "todo-one",
          "TodoWrite",
          { todos: [] },
          { newTodos: [{ content: "Do work", status: "in_progress" }] },
        ],
        [
          "todo-two",
          "TodoWrite",
          { todos: [] },
          { newTodos: [{ content: "Do work", status: "completed" }] },
        ],
        [
          "plan",
          "ExitPlanMode",
          { plan: "# Original" },
          { plan: "# Native edited plan", isAgent: false },
        ],
      ] as const) {
        entries.push({
          type: "assistant",
          uuid: `${id}-call`,
          parentUuid: parent,
          message: { content: [{ type: "tool_use", id, name, input }], stop_reason: "tool_use" },
        });
        parent = `${id}-result`;
        entries.push({
          type: "user",
          uuid: parent,
          parentUuid: `${id}-call`,
          toolUseResult: result,
          message: {
            content: [{ type: "tool_result", tool_use_id: id, content: "Native result" }],
          },
        });
      }
      entries.push({
        type: "assistant",
        uuid: "done",
        parentUuid: parent,
        message: { content: "done", stop_reason: "end_turn" },
      });
      yield* fs.writeFileString(
        path.join(directory, `${nativeId}.jsonl`),
        entries
          .map((entry) =>
            JSON.stringify({
              ...(entry as object),
              sessionId: nativeId,
              cwd: "/work",
              timestamp: "2026-10-29T00:00:00Z",
            }),
          )
          .join("\n") + "\n",
      );
      const client = new FixtureClient("background");
      Object.assign(client.inventory[0]!, { state: "done" });
      const now = DateTime.makeUnsafe(1);
      const providerThreadId = ProviderThreadId.make("planning-thread");
      const providerThread: OrchestrationV2ProviderThread = {
        id: providerThreadId,
        driver: ProviderDriverKind.make("claudeAgent"),
        providerInstanceId: instanceId,
        providerSessionId: ProviderSessionId.make("planning-session"),
        appThreadId: threadId,
        nativeThreadRef: {
          driver: ProviderDriverKind.make("claudeAgent"),
          nativeId,
          strength: "strong",
        },
        nativeConversationHeadRef: null,
        status: "active",
        ownerNodeId: null,
        firstRunOrdinal: null,
        lastRunOrdinal: null,
        handoffIds: [],
        forkedFrom: null,
        createdAt: now,
        updatedAt: now,
      };
      const adapter = yield* makeNativeClaudeAdapter({
        instanceId,
        configDirectory: root,
        binaryPath: "never-executed",
        environment: {},
        client,
      });
      const runtime = yield* adapter.openSession({
        threadId,
        providerSessionId: providerThread.providerSessionId!,
        modelSelection,
        runtimePolicy,
        onExternalTurn: () =>
          Effect.succeed({
            threadId,
            providerThread,
            modelSelection,
            runtimePolicy,
            runId: RunId.make("planning-run"),
            runOrdinal: 1,
            providerTurnOrdinal: 1,
            attemptId: RunAttemptId.make("planning-attempt"),
            rootNodeId: NodeId.make("planning-root"),
            appThread: fixtureAppThread(now, providerThreadId),
            message: {
              createdBy: "user",
              creationSource: "provider",
              messageId: MessageId.make("planning-message"),
              text: "plan",
              attachments: [],
            },
          }),
      });
      yield* runtime.resumeThread({ threadId, providerThread });
      const events = yield* runtime.events.pipe(
        Stream.takeUntil((event) => event.type === "turn.terminal"),
        Stream.runCollect,
      );
      const plans = events
        .filter((event) => event.type === "plan.updated")
        .map((event) => event.plan);
      expect(plans.map((plan) => [plan.kind, plan.status])).toEqual([
        ["todo_list", "active"],
        ["todo_list", "superseded"],
        ["todo_list", "completed"],
        ["proposed_plan", "active"],
      ]);
      expect(plans.at(-1)?.id).toBe(
        `plan:claude-native:${encodeURIComponent(threadId)}:proposed_plan%3Aplan`,
      );
      expect(plans.at(-1)).toMatchObject({
        markdown: "# Native edited plan",
        runId: "planning-run",
      });
      const items = events
        .filter((event) => event.type === "turn_item.updated")
        .map((event) => event.turnItem);
      expect(items.filter((item) => item.type === "dynamic_tool")).toHaveLength(3);
      expect(items.filter((item) => item.type === "todo_list")).toHaveLength(2);
      expect(items.filter((item) => item.type === "proposed_plan")).toHaveLength(1);
      expect(client.resumes).toEqual([]);
    }).pipe(Effect.scoped, Effect.provide(Layer.mergeAll(NodeServices.layer, IdAllocator.layer))),
);

it.effect(
  "observes a native background child after its parent turn ends without creating a child conversation or another input",
  () =>
    Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem;
      const path = yield* Path.Path;
      const root = yield* fs.makeTempDirectoryScoped();
      const directory = path.join(root, "projects", "workspace");
      const childDirectory = path.join(directory, nativeId, "subagents");
      yield* fs.makeDirectory(childDirectory, { recursive: true });
      const agentId = "a5321b469ff7aa652";
      const records = [
        {
          type: "user",
          uuid: "agent-user",
          parentUuid: null,
          message: { content: "Spawn native child" },
        },
        {
          type: "assistant",
          uuid: "agent-call",
          parentUuid: "agent-user",
          message: {
            content: [
              {
                type: "tool_use",
                id: "agent-tool",
                name: "Agent",
                input: { prompt: "Child work", description: "Native child" },
              },
            ],
            stop_reason: "tool_use",
          },
        },
        {
          type: "user",
          uuid: "agent-result",
          parentUuid: "agent-call",
          toolUseResult: {
            status: "async_launched",
            agentId,
            prompt: "Child work",
            description: "Native child",
          },
          message: {
            content: [{ type: "tool_result", tool_use_id: "agent-tool", content: "Launched" }],
          },
        },
        {
          type: "assistant",
          uuid: "parent-done",
          parentUuid: "agent-result",
          message: { content: "Parent done", stop_reason: "end_turn" },
        },
      ];
      const file = path.join(directory, `${nativeId}.jsonl`);
      yield* fs.writeFileString(
        file,
        records
          .map((entry) =>
            JSON.stringify({
              ...entry,
              sessionId: nativeId,
              cwd: "/work",
              timestamp: "2026-10-29T00:00:00Z",
            }),
          )
          .join("\n") + "\n",
      );
      const meta = path.join(childDirectory, `agent-${agentId}.meta.json`);
      yield* fs.writeFileString(meta, JSON.stringify({ toolUseId: "agent-tool" }));
      yield* fs.writeFileString(
        path.join(childDirectory, `agent-${agentId}.jsonl`),
        JSON.stringify({
          type: "assistant",
          sessionId: nativeId,
          agentId,
          isSidechain: true,
          timestamp: "2026-10-29T00:00:01Z",
          message: {
            content: [{ type: "text", text: "Child model stopped; children may still run" }],
            stop_reason: "end_turn",
          },
        }) + "\n",
      );
      const client = new FixtureClient("background");
      Object.assign(client.inventory[0]!, { state: "done" });
      const now = DateTime.makeUnsafe(1);
      const providerThreadId = ProviderThreadId.make("subagent-thread");
      const providerThread: OrchestrationV2ProviderThread = {
        id: providerThreadId,
        driver: ProviderDriverKind.make("claudeAgent"),
        providerInstanceId: instanceId,
        providerSessionId: ProviderSessionId.make("subagent-session"),
        appThreadId: threadId,
        nativeThreadRef: {
          driver: ProviderDriverKind.make("claudeAgent"),
          nativeId,
          strength: "strong",
        },
        nativeConversationHeadRef: null,
        status: "active",
        ownerNodeId: null,
        firstRunOrdinal: null,
        lastRunOrdinal: null,
        handoffIds: [],
        forkedFrom: null,
        createdAt: now,
        updatedAt: now,
      };
      let adoptions = 0;
      const adapter = yield* makeNativeClaudeAdapter({
        instanceId,
        configDirectory: root,
        binaryPath: "never-executed",
        environment: {},
        client,
      });
      const runtime = yield* adapter.openSession({
        threadId,
        providerSessionId: providerThread.providerSessionId!,
        modelSelection,
        runtimePolicy,
        onExternalTurn: () => {
          adoptions++;
          return Effect.succeed({
            threadId,
            providerThread,
            modelSelection,
            runtimePolicy,
            runId: RunId.make("subagent-run"),
            runOrdinal: 1,
            providerTurnOrdinal: 1,
            attemptId: RunAttemptId.make("subagent-attempt"),
            rootNodeId: NodeId.make("subagent-root"),
            appThread: fixtureAppThread(now, providerThreadId),
            message: {
              createdBy: "user",
              creationSource: "provider",
              messageId: MessageId.make("subagent-message"),
              text: "Spawn native child",
              attachments: [],
            },
          });
        },
      });
      const events: ProviderAdapter.ProviderAdapterV2Event[] = [];
      const running = yield* Deferred.make<void>();
      const terminal = yield* Deferred.make<void>();
      const childFailed = yield* Deferred.make<void>();
      yield* runtime.events.pipe(
        Stream.runForEach((event) =>
          Effect.gen(function* () {
            events.push(event);
            if (event.type === "subagent.updated" && event.subagent.status === "running")
              yield* Deferred.succeed(running, undefined);
            if (event.type === "turn.terminal") yield* Deferred.succeed(terminal, undefined);
            if (event.type === "subagent.updated" && event.subagent.status === "failed")
              yield* Deferred.succeed(childFailed, undefined);
          }),
        ),
        Effect.forkScoped,
      );
      yield* runtime.resumeThread({ threadId, providerThread });
      yield* Deferred.await(running);
      yield* Deferred.await(terminal);
      yield* fs.writeFileString(meta, JSON.stringify({ toolUseId: "agent-tool", finished: true }));
      yield* fs.writeFileString(
        file,
        (yield* fs.readFileString(file)) +
          JSON.stringify({
            type: "user",
            uuid: "child-report",
            parentUuid: "parent-done",
            sessionId: nativeId,
            timestamp: "2026-10-29T00:00:02Z",
            promptSource: "system",
            turnOrigin: "task_notification",
            origin: { kind: "task-notification", producer: "session-task" },
            message: {
              content: `<task-notification>\n<task-id>${agentId}</task-id>\n<tool-use-id>agent-tool</tool-use-id>\n<status>failed</status>\n</task-notification>`,
            },
          }) +
          "\n",
      );
      yield* runtime.resumeThread({ threadId, providerThread });
      yield* Deferred.await(childFailed);
      const lifecycle = events
        .filter((event) => event.type === "subagent.updated")
        .map((event) => event.subagent);
      expect(lifecycle.map((entry) => entry.status)).toEqual(["running", "failed"]);
      expect(lifecycle[0]?.id).toBe(lifecycle[1]?.id);
      expect(lifecycle[1]).toMatchObject({
        runId: "subagent-run",
        childThreadId: null,
        nativeTaskRef: { nativeId: agentId, strength: "strong" },
      });
      expect(events.some((event) => event.type === "app_thread.created")).toBe(false);
      expect(adoptions).toBe(1);
      expect(client.resumes).toEqual([]);
    }).pipe(Effect.scoped, Effect.provide(Layer.mergeAll(NodeServices.layer, IdAllocator.layer))),
);

it.effect(
  "sends owned attachments through the same native reply and correlates its exact reference text",
  () =>
    Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem;
      const path = yield* Path.Path;
      const root = yield* fs.makeTempDirectoryScoped();
      const directory = path.join(root, "projects", "workspace");
      yield* fs.makeDirectory(directory, { recursive: true });
      yield* fs.writeFileString(
        path.join(directory, `${nativeId}.jsonl`),
        JSON.stringify({
          type: "user",
          uuid: "baseline",
          parentUuid: null,
          cwd: "/work",
          timestamp: "1970-01-01T00:00:00.001Z",
          message: { role: "user", content: "Earlier" },
        }) + "\n",
      );
      const stored = path.join(root, "stored notes.txt");
      yield* fs.writeFileString(stored, "native");
      class Client extends FixtureClient {
        readonly replies: string[] = [];
        override async reply(_job: NativeClaudeSession, text: string) {
          this.replies.push(text);
        }
      }
      const client = new Client("background");
      const adapter = yield* makeNativeClaudeAdapter({
        instanceId,
        configDirectory: root,
        binaryPath: "never-executed",
        environment: {},
        client,
        resolveAttachmentPath: (attachment) => (attachment.id === "owned-file" ? stored : null),
      });
      const runtime = yield* adapter.openSession({
        threadId,
        providerSessionId: ProviderSessionId.make("attachment-session"),
        modelSelection,
        runtimePolicy,
      });
      const now = DateTime.makeUnsafe(1);
      const providerThread: OrchestrationV2ProviderThread = {
        id: ProviderThreadId.make("attachment-thread"),
        driver: ProviderDriverKind.make("claudeAgent"),
        providerInstanceId: instanceId,
        providerSessionId: runtime.providerSessionId,
        appThreadId: threadId,
        nativeThreadRef: {
          driver: ProviderDriverKind.make("claudeAgent"),
          nativeId,
          strength: "strong",
        },
        nativeConversationHeadRef: null,
        status: "idle",
        ownerNodeId: null,
        firstRunOrdinal: null,
        lastRunOrdinal: null,
        handoffIds: [],
        forkedFrom: null,
        createdAt: now,
        updatedAt: now,
      };
      const turn: ProviderAdapter.ProviderAdapterV2TurnInput = {
        threadId,
        providerThread,
        modelSelection,
        runtimePolicy,
        runId: RunId.make("attachment-run"),
        runOrdinal: 1,
        providerTurnOrdinal: 1,
        attemptId: RunAttemptId.make("attachment-attempt"),
        rootNodeId: NodeId.make("attachment-root"),
        appThread: fixtureAppThread(now, providerThread.id),
        message: {
          createdBy: "user",
          creationSource: "web",
          messageId: MessageId.make("attachment-message"),
          text: "Inspect",
          attachments: [
            {
              id: "owned-file",
              type: "file",
              name: "notes.txt",
              mimeType: "text/plain",
              sizeBytes: 6,
            },
          ],
        },
      };
      const invalid = yield* Effect.exit(
        runtime.startTurn({
          ...turn,
          message: {
            ...turn.message,
            attachments: [{ ...turn.message.attachments[0]!, id: "not-owned" }],
          },
        }),
      );
      expect(Exit.isFailure(invalid)).toBe(true);
      expect(client.replies).toEqual([]);
      yield* runtime.startTurn(turn);
      const expectedText = `Inspect\n\n@"${stored}"`;
      expect(client.replies).toEqual([expectedText]);
      const pending = { baseline: new Set(["baseline"]), expectedText };
      expect(
        nativeClaudeMatchesPendingTurn(
          { id: "native-new", items: [{ type: "user", text: expectedText }] },
          pending,
        ),
      ).toBe(true);
      expect(
        nativeClaudeMatchesPendingTurn(
          { id: "cli-new", items: [{ type: "user", text: "Inspect" }] },
          pending,
        ),
      ).toBe(false);
      expect(
        nativeClaudeMatchesPendingTurn(
          { id: "baseline", items: [{ type: "user", text: expectedText }] },
          pending,
        ),
      ).toBe(false);
    }).pipe(Effect.scoped, Effect.provide(Layer.mergeAll(NodeServices.layer, IdAllocator.layer))),
);

it.effect(
  "forks a complete idle native session with durable independent identity and rejects unsupported fork changes",
  () =>
    Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem;
      const path = yield* Path.Path;
      const root = yield* fs.makeTempDirectoryScoped();
      const project = path.join(root, "projects", "workspace");
      yield* fs.makeDirectory(project, { recursive: true });
      yield* fs.writeFileString(
        path.join(project, `${nativeId}.jsonl`),
        [
          { type: "user", uuid: "fork-seed-user", parentUuid: null, message: { content: "Seed" } },
          {
            type: "assistant",
            uuid: "fork-seed-answer",
            parentUuid: "fork-seed-user",
            message: {
              model: "claude-test",
              content: [{ type: "text", text: "Native answer" }],
              stop_reason: "end_turn",
            },
          },
        ]
          .map((entry) =>
            JSON.stringify({
              ...entry,
              sessionId: nativeId,
              cwd: "/work",
              timestamp: "2026-10-29T00:00:00Z",
            }),
          )
          .join("\n") + "\n",
      );
      const newId = "fedcba98-1234-1234-1234-123456789abc";
      class ForkClient extends FixtureClient {
        calls: Parameters<NativeClaudeClient["forkBackground"]>[0][] = [];
        replies: string[] = [];
        override async forkBackground(
          request: Parameters<NativeClaudeClient["forkBackground"]>[0],
        ) {
          this.calls.push(request);
          return { ...this.inventory[0]!, id: "fedcba98", sessionId: newId, pid: 73 };
        }
        override async listSessions() {
          return this.calls.length
            ? [
                ...this.inventory,
                { ...this.inventory[0]!, id: "fedcba98", sessionId: newId, pid: 73 },
              ]
            : this.inventory;
        }
        override async reply(_job: NativeClaudeSession, text: string) {
          this.replies.push(text);
        }
      }
      const client = new ForkClient("background");
      const adapter = yield* makeNativeClaudeAdapter({
        instanceId,
        configDirectory: root,
        binaryPath: "never-executed",
        environment: {},
        client,
      });
      const session = yield* adapter.openSession({
        threadId,
        providerSessionId: ProviderSessionId.make("fork-runtime"),
        modelSelection,
        runtimePolicy,
      });
      const now = DateTime.makeUnsafe(1);
      const source: OrchestrationV2ProviderThread = {
        id: ProviderThreadId.make("fork-source"),
        driver: ProviderDriverKind.make("claudeAgent"),
        providerInstanceId: instanceId,
        providerSessionId: session.providerSessionId,
        appThreadId: threadId,
        ownerNodeId: null,
        nativeThreadRef: {
          driver: ProviderDriverKind.make("claudeAgent"),
          nativeId,
          strength: "strong",
        },
        nativeConversationHeadRef: null,
        status: "idle",
        firstRunOrdinal: null,
        lastRunOrdinal: null,
        handoffIds: [],
        forkedFrom: null,
        createdAt: now,
        updatedAt: now,
      };
      const targetThreadId = ThreadId.make("fork-target");
      const request = {
        sourceProviderThread: source,
        targetThreadId,
        modelSelection,
        runtimePolicy,
      };
      const fork = yield* session.forkThread(request);
      expect(fork.nativeThreadRef?.nativeId).toBe(newId);
      expect(fork.nativeMetadata).toEqual({ claudeForkIdentityNamespace: newId });
      expect(fork.forkedFrom).toEqual({ providerThreadId: source.id });
      expect(fork.appThreadId).toBe(targetThreadId);
      expect(source.nativeMetadata).toBeUndefined();
      expect(client.replies).toEqual([]);
      expect(client.calls).toHaveLength(1);
      const capabilities = yield* adapter.getCapabilities();
      expect(capabilities.threads.canForkThread).toBe(true);
      expect(capabilities.threads.canForkFromTurn).toBe(false);
      for (const invalid of [
        { ...request, providerTurnId: "unsupported-turn" as never },
        { ...request, modelSelection: { ...modelSelection, model: "different-model" } },
        { ...request, runtimePolicy: { ...runtimePolicy, cwd: "/other-worktree" } },
      ])
        expect(Exit.isFailure(yield* Effect.exit(session.forkThread(invalid)))).toBe(true);
      expect(client.calls).toHaveLength(1);
      // Native fork writes its copied JSONL lazily; the first explicit target prompt is still admitted.
      yield* session.startTurn({
        providerThread: fork,
        threadId: targetThreadId,
        modelSelection,
        runtimePolicy,
        runId: RunId.make("fork-run"),
        runOrdinal: 1,
        providerTurnOrdinal: 1,
        attemptId: RunAttemptId.make("fork-attempt"),
        rootNodeId: NodeId.make("fork-root"),
        appThread: { ...fixtureAppThread(now, fork.id), id: targetThreadId },
        message: {
          createdBy: "user",
          creationSource: "web",
          messageId: MessageId.make("fork-input"),
          text: "Independent input",
          attachments: [],
        },
      });
      expect(client.replies).toEqual(["Independent input"]);
      expect(client.resumes).toEqual([newId]);
    }).pipe(Effect.scoped, Effect.provide(Layer.mergeAll(NodeServices.layer, IdAllocator.layer))),
);

it.effect(
  "binds a new native MCP worker, rotates its credential on runtime recovery, and gives forks independent launch ownership",
  () =>
    Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem;
      const path = yield* Path.Path;
      const temporary = yield* fs.makeTempDirectoryScoped();
      const root = yield* fs.realPath(temporary);
      yield* fs.makeDirectory(path.join(root, "projects", "workspace"), { recursive: true });
      yield* fs.writeFileString(path.join(root, "projects", "workspace", `${nativeId}.jsonl`), "");
      const ledger = new NativeClaudeMcpBindings(path.join(root, "bindings"));
      const constraints: { metadataKey: string; sessionId: string }[] = [];
      const targetThreadId = ThreadId.make("mcp-fork-target");
      const makeConfig = (owner: ThreadId, generation: string): McpProviderSessionConfig => ({
        environmentId: EnvironmentId.make("mcp-test"),
        threadId: owner,
        providerInstanceId: instanceId,
        providerSessionId: generation,
        endpoint: "http://127.0.0.1:1234/mcp",
        authorizationHeader: `Bearer ${generation}`,
        browserToolsAvailable: false,
        bindNativeSession: (constraint) =>
          Effect.sync(() => {
            constraints.push(constraint);
            return true;
          }),
      });
      let currentConfig = makeConfig(threadId, "first");
      const forkConfig = makeConfig(targetThreadId, "target");
      class Client extends FixtureClient {
        readonly launches: Parameters<NativeClaudeClient["startBackground"]>[0][] = [];
        readonly forks: Parameters<NativeClaudeClient["forkBackground"]>[0][] = [];
        private async save(job: NativeClaudeSession, mcpConfigPath: string) {
          const directory = path.join(root, "jobs", job.id!);
          await NodeFSP.mkdir(directory, { recursive: true });
          await NodeFSP.writeFile(
            path.join(directory, "state.json"),
            JSON.stringify({
              sessionId: job.sessionId,
              daemonShort: job.id,
              respawnFlags: ["--mcp-config", mcpConfigPath],
            }),
          );
        }
        override async startBackground(
          input: Parameters<NativeClaudeClient["startBackground"]>[0],
        ) {
          this.launches.push(input);
          const job = this.inventory[0]!;
          if (input.mcpConfigPath) {
            await expect(
              readClaudeMcpRelayCredential({
                launchDirectory: path.dirname(input.mcpConfigPath),
                nativeSessionId: nativeId,
                discovery: false,
              }),
            ).rejects.toThrow("not bound");
            await this.save(job, input.mcpConfigPath);
          }
          return job;
        }
        override async hasMcpConfig(job: NativeClaudeSession, expected: string) {
          const raw = await NodeFSP.readFile(
            path.join(root, "jobs", job.id!, "state.json"),
            "utf8",
          );
          return JSON.parse(raw).respawnFlags[1] === expected;
        }
        override async forkBackground(input: Parameters<NativeClaudeClient["forkBackground"]>[0]) {
          this.forks.push(input);
          const job = {
            ...this.inventory[0]!,
            id: "fedcba98",
            sessionId: "fedcba98-1234-1234-1234-123456789abc",
            pid: 73,
          };
          await this.save(job, input.mcpConfigPath!);
          Object.assign(this, { inventory: [...this.inventory, job] });
          return job;
        }
      }
      const client = new Client("background");
      const options = {
        instanceId,
        configDirectory: root,
        binaryPath: "never-executed",
        environment: {},
        client,
        mcp: {
          bindings: ledger,
          readConfig: (id: ThreadId) =>
            Effect.succeed(id === targetThreadId ? forkConfig : currentConfig),
          launcher: (directory: string) => ({ command: "node", args: ["relay", directory] }),
        },
      };
      const adapter = yield* makeNativeClaudeAdapter(options);
      const runtime = yield* adapter.openSession({
        threadId,
        providerSessionId: ProviderSessionId.make("mcp-session"),
        modelSelection,
        runtimePolicy,
      });
      expect(runtime.providerSession.capabilities.tools.supportsMcpTools).toBe(true);
      expect(nativeClaudeCapabilities.tools.supportsMcpTools).toBe(false);
      const nativeThread = yield* runtime.ensureThread({ threadId, modelSelection, runtimePolicy });
      expect(client.launches).toHaveLength(1);
      const sourcePath = client.launches[0]!.mcpConfigPath!;
      const sourceLaunch = path.dirname(sourcePath);
      expect(constraints).toContainEqual({
        metadataKey: "t3/claudeSessionID",
        sessionId: nativeId,
      });
      expect(
        (yield* Effect.promise(() =>
          readClaudeMcpRelayCredential({
            launchDirectory: sourceLaunch,
            nativeSessionId: nativeId,
            discovery: false,
          }),
        )).providerSessionId,
      ).toBe("first");
      expect(yield* fs.readFileString(sourcePath)).not.toContain("Bearer");
      currentConfig = makeConfig(threadId, "second");
      const recoveredAdapter = yield* makeNativeClaudeAdapter(options);
      const recoveredRuntime = yield* recoveredAdapter.openSession({
        threadId,
        providerSessionId: ProviderSessionId.make("mcp-restarted-session"),
        modelSelection,
        runtimePolicy,
      });
      const sourceState = path.join(root, "jobs", "1234abcd", "state.json");
      yield* fs.writeFileString(
        sourceState,
        JSON.stringify({ sessionId: nativeId, daemonShort: "1234abcd", respawnFlags: [] }),
      );
      yield* recoveredRuntime.resumeThread({ threadId, providerThread: nativeThread });
      expect(
        (yield* Effect.promise(() =>
          readClaudeMcpRelayCredential({
            launchDirectory: sourceLaunch,
            nativeSessionId: nativeId,
            discovery: false,
          }),
        )).providerSessionId,
      ).toBe("first");
      yield* fs.writeFileString(
        sourceState,
        JSON.stringify({
          sessionId: nativeId,
          daemonShort: "1234abcd",
          respawnFlags: ["--mcp-config", sourcePath],
        }),
      );
      yield* recoveredRuntime.resumeThread({ threadId, providerThread: nativeThread });
      expect(client.launches).toHaveLength(1);
      expect(
        (yield* Effect.promise(() =>
          readClaudeMcpRelayCredential({
            launchDirectory: sourceLaunch,
            nativeSessionId: nativeId,
            discovery: false,
          }),
        )).providerSessionId,
      ).toBe("second");
      const forked = yield* recoveredRuntime.forkThread!({
        sourceProviderThread: nativeThread,
        targetThreadId,
      });
      expect(client.forks[0]?.expectedSourceMcpConfigPath).toBe(sourcePath);
      const targetPath = client.forks[0]!.mcpConfigPath!;
      expect(targetPath).not.toBe(sourcePath);
      expect(
        (yield* Effect.promise(() =>
          readClaudeMcpRelayCredential({
            launchDirectory: path.dirname(targetPath),
            nativeSessionId: forked.nativeThreadRef!.nativeId!,
            discovery: false,
          }),
        )).threadId,
      ).toBe(targetThreadId);
      yield* Effect.promise(() =>
        ledger.revoke({ nativeSessionId: nativeId, expectedProviderSessionId: "second" }),
      );
      yield* recoveredRuntime.resumeThread({ threadId, providerThread: nativeThread });
      yield* Effect.promise(() =>
        expect(
          readClaudeMcpRelayCredential({
            launchDirectory: sourceLaunch,
            nativeSessionId: nativeId,
            discovery: false,
          }),
        ).rejects.toThrow("unavailable"),
      );
    }).pipe(Effect.scoped, Effect.provide(Layer.mergeAll(NodeServices.layer, IdAllocator.layer))),
);

it.effect.each([
  "complete",
  "child",
  "child-siblings",
  "detach",
  "recover",
  "recover-completed",
  "recover-after-read-error",
] as const)("streams native background Bash after its model turn and handles %s", (mode) =>
  Effect.gen(function* () {
    const completedRecovery = mode === "recover-completed" || mode === "recover-after-read-error";
    let failCommands = mode === "recover-after-read-error";
    if (failCommands || mode.startsWith("child")) {
      const original = NativeClaudeSessions.prototype.backgroundCommands;
      yield* Effect.acquireRelease(
        Effect.sync(() =>
          vi
            .spyOn(NativeClaudeSessions.prototype, "backgroundCommands")
            .mockImplementation(function (this: NativeClaudeSessions, session) {
              return failCommands
                ? Promise.reject(new Error("temporary native transcript read failure"))
                : original.call(this, session).then((commands) =>
                    mode.startsWith("child")
                      ? commands.flatMap((command) => [
                          { ...command, parentToolId: "parent-agent-tool", agentId: "a123child" },
                          ...(mode === "child-siblings"
                            ? [
                                {
                                  ...command,
                                  parentToolId: "second-parent-agent-tool",
                                  agentId: "a456child",
                                },
                              ]
                            : []),
                        ])
                      : commands,
                  );
            }),
        ),
        (spy) => Effect.sync(() => spy.mockRestore()),
      );
    }
    const fs = yield* FileSystem.FileSystem;
    const path = yield* Path.Path;
    const temporary = yield* fs.makeTempDirectoryScoped();
    const root = yield* Effect.promise(() => NodeFSP.realpath(temporary));
    const project = path.join(root, "projects", "workspace");
    const nativeRoot = path.join(root, `claude-${process.getuid?.() ?? 0}`);
    const output = path.join(nativeRoot, "workspace", nativeId, "tasks", "bg123.output");
    yield* Effect.promise(() =>
      NodeFSP.mkdir(path.dirname(output), { recursive: true, mode: 0o700 }),
    );
    yield* fs.makeDirectory(project, { recursive: true });
    yield* fs.writeFileString(output, "FIRST\n");
    const records: Array<Record<string, unknown>> = [
      {
        type: "user",
        uuid: "bg-user",
        parentUuid: null,
        message: { content: "Run native background command" },
      },
      {
        type: "assistant",
        uuid: "bg-call",
        parentUuid: "bg-user",
        message: {
          content: [
            {
              type: "tool_use",
              id: "bg-tool",
              name: "Bash",
              input: { command: "printf markers", run_in_background: true },
            },
          ],
          stop_reason: "tool_use",
        },
      },
      {
        type: "user",
        uuid: "bg-result",
        parentUuid: "bg-call",
        toolUseResult: { backgroundTaskId: "bg123", stdout: "", stderr: "", interrupted: false },
        message: {
          content: [
            {
              type: "tool_result",
              tool_use_id: "bg-tool",
              content: `Command running in background with ID: bg123. Output is being written to: ${output}.`,
            },
          ],
        },
      },
      {
        type: "assistant",
        uuid: "bg-done",
        parentUuid: "bg-result",
        message: { content: "Model turn ended", stop_reason: "end_turn" },
      },
    ];
    if (completedRecovery)
      records.push({
        type: "user",
        uuid: "already-completed",
        parentUuid: "bg-done",
        promptSource: "system",
        turnOrigin: "task_notification",
        origin: { kind: "task-notification", producer: "session-task" },
        message: {
          content: `<task-notification>\n<task-id>bg123</task-id>\n<tool-use-id>bg-tool</tool-use-id>\n<output-file>${output}</output-file>\n<status>completed</status>\n</task-notification>`,
        },
      });
    const file = path.join(project, `${nativeId}.jsonl`);
    const save = () =>
      fs.writeFileString(
        file,
        records
          .map((record) =>
            JSON.stringify({
              ...record,
              sessionId: nativeId,
              cwd: "/work",
              timestamp: "2026-10-29T00:00:00Z",
            }),
          )
          .join("\n") + "\n",
      );
    yield* save();
    const client = new FixtureClient("background");
    Object.assign(client.inventory[0]!, { state: "done" });
    const now = DateTime.makeUnsafe(1);
    const providerThreadId = ProviderThreadId.make("bg-thread");
    const providerThread: OrchestrationV2ProviderThread = {
      id: providerThreadId,
      driver: ProviderDriverKind.make("claudeAgent"),
      providerInstanceId: instanceId,
      providerSessionId: ProviderSessionId.make("bg-session"),
      appThreadId: threadId,
      nativeThreadRef: {
        driver: ProviderDriverKind.make("claudeAgent"),
        nativeId,
        strength: "strong",
      },
      nativeConversationHeadRef: null,
      status: mode === "recover" ? "idle" : "active",
      ownerNodeId: null,
      firstRunOrdinal: null,
      lastRunOrdinal: null,
      handoffIds: [],
      forkedFrom: null,
      createdAt: now,
      updatedAt: now,
    };
    let adoptions = 0;
    const adapter = yield* makeNativeClaudeAdapter({
      instanceId,
      configDirectory: root,
      binaryPath: "never-executed",
      environment: {},
      client,
    });
    const owner: ProviderAdapter.ProviderAdapterV2TurnInput = {
      threadId,
      providerThread,
      modelSelection,
      runtimePolicy,
      runId: RunId.make("bg-run"),
      runOrdinal: 1,
      providerTurnOrdinal: 1,
      attemptId: RunAttemptId.make("bg-attempt"),
      rootNodeId: NodeId.make("bg-root"),
      appThread: fixtureAppThread(now, providerThreadId),
      message: {
        createdBy: "user",
        creationSource: "provider",
        messageId: MessageId.make("bg-message"),
        text: "Run native background command",
        attachments: [],
      },
    };
    let recoveries = 0;
    const runtime = yield* adapter.openSession({
      threadId,
      providerSessionId: providerThread.providerSessionId!,
      modelSelection,
      runtimePolicy,
      onExternalTurn: () => {
        adoptions++;
        return Effect.succeed(completedRecovery ? undefined : owner);
      },
      recoverNativeBackgroundTurn: (request) => {
        recoveries++;
        expect(request).toMatchObject({
          nativeThreadId: nativeId,
          nativeTurnId: "bg-user",
          tasks: [
            {
              nativeToolId: "bg-tool",
              nativeTaskId: "bg123",
              status: completedRecovery ? "completed" : "running",
            },
          ],
        });
        return Effect.succeed({
          turn: owner,
          providerTurnId: ProviderTurnId.make("original-admission-turn"),
          tasks: [{ nativeToolId: "bg-tool", nativeTaskId: "bg123" }],
        });
      },
    });
    const events: ProviderAdapter.ProviderAdapterV2Event[] = [];
    const terminal = yield* Deferred.make<void>();
    const commandRunning = yield* Deferred.make<void>();
    const first = yield* Deferred.make<void>();
    const second = yield* Deferred.make<void>();
    const completed = yield* Deferred.make<void>();
    yield* runtime.events.pipe(
      Stream.runForEach((event) =>
        Effect.gen(function* () {
          events.push(event);
          if (
            event.type === "turn_item.updated" &&
            event.turnItem.type === "command_execution" &&
            event.turnItem.status === "running"
          )
            yield* Deferred.succeed(commandRunning, undefined);
          if (event.type === "turn.terminal") yield* Deferred.succeed(terminal, undefined);
          if (event.type === "command_output.delta" && event.chunk.includes("FIRST"))
            yield* Deferred.succeed(first, undefined);
          if (event.type === "command_output.delta" && event.chunk.includes("SECOND"))
            yield* Deferred.succeed(second, undefined);
          if (
            event.type === "turn_item.updated" &&
            event.turnItem.type === "command_execution" &&
            event.turnItem.status === "completed"
          )
            yield* Deferred.succeed(completed, undefined);
        }),
      ),
      Effect.forkScoped,
    );
    yield* runtime.resumeThread({ threadId, providerThread });
    if (mode === "recover-after-read-error") {
      expect(recoveries).toBe(0);
      failCommands = false;
      yield* runtime.resumeThread({
        threadId,
        providerThread: { ...providerThread, status: "idle" },
      });
    }
    if (mode.startsWith("recover")) {
      expect(recoveries).toBe(1);
      yield* Deferred.await(commandRunning);
    } else yield* Deferred.await(terminal);
    const command = events.find(
      (event) => event.type === "turn_item.updated" && event.turnItem.type === "command_execution",
    );
    expect(command).toMatchObject({
      type: "turn_item.updated",
      turnItem: {
        type: "command_execution",
        status: "running",
        runId: "bg-run",
        input: "printf markers",
        ...(mode.startsWith("recover") ? { providerTurnId: "original-admission-turn" } : {}),
      },
    });
    if (mode.startsWith("child")) {
      const ids = yield* IdAllocator.IdAllocatorV2;
      expect(command?.type === "turn_item.updated" && command.turnItem.parentItemId).toBe(
        ids.derive.turnItemFromProviderItem({
          driver: ProviderDriverKind.make("claudeAgent"),
          nativeItemId: nativeSessionProjectionId(providerThread, "parent-agent-tool"),
        }),
      );
      expect(
        command?.type === "turn_item.updated" && command.turnItem.nativeItemRef?.nativeId,
      ).toContain("a123child");
    }
    yield* Deferred.await(first);
    if (mode === "child-siblings") {
      const commands = events.filter(
        (event) =>
          event.type === "turn_item.updated" &&
          event.turnItem.type === "command_execution" &&
          event.turnItem.status === "running",
      );
      expect(
        new Set(commands.map((event) => event.type === "turn_item.updated" && event.turnItem.id))
          .size,
      ).toBe(2);
      expect(
        new Set(
          commands.map(
            (event) => event.type === "turn_item.updated" && event.turnItem.parentItemId,
          ),
        ).size,
      ).toBe(2);
    }
    if (completedRecovery) {
      yield* Deferred.await(completed);
      const final = events.findLast(
        (event) =>
          event.type === "turn_item.updated" && event.turnItem.type === "command_execution",
      );
      expect(final).toMatchObject({
        turnItem: { status: "completed", output: "FIRST\n", runId: "bg-run" },
      });
      expect(events.some((event) => event.type === "turn.terminal")).toBe(false);
      return;
    }
    yield* Effect.promise(() => NodeFSP.appendFile(output, "SECOND\n"));
    yield* TestClock.adjust("100 millis");
    yield* Deferred.await(second);
    if (mode === "detach") {
      expect(typeof runtime.unloadThread).toBe("function");
      yield* runtime.unloadThread!({ providerThread });
      const count = events.filter((event) => event.type === "command_output.delta").length;
      yield* Effect.promise(() => NodeFSP.appendFile(output, "DETACHED\n"));
      yield* TestClock.adjust("100 millis");
      expect(events.filter((event) => event.type === "command_output.delta")).toHaveLength(count);
      expect(yield* runtime.hasPendingBackgroundWorkForThread!(providerThread)).toBe(false);
      expect(client.resumes).toEqual([]);
      return;
    }
    records.push({
      type: "user",
      uuid: "bg-notification",
      parentUuid: "bg-done",
      promptSource: "system",
      turnOrigin: "task_notification",
      origin: { kind: "task-notification", producer: "session-task" },
      message: {
        content: `<task-notification>\n<task-id>bg123</task-id>\n<tool-use-id>bg-tool</tool-use-id>\n<output-file>${output}</output-file>\n<status>completed</status>\n</task-notification>`,
      },
    });
    yield* save();
    yield* runtime.resumeThread({ threadId, providerThread });
    yield* TestClock.adjust("100 millis");
    yield* Deferred.await(completed);
    const final = events.findLast(
      (event) => event.type === "turn_item.updated" && event.turnItem.type === "command_execution",
    );
    expect(final).toMatchObject({
      turnItem: { status: "completed", output: "FIRST\nSECOND\n", runId: "bg-run" },
    });
    expect(final?.type === "turn_item.updated" && final.turnItem.id).toBe(
      command?.type === "turn_item.updated" && command.turnItem.id,
    );
    expect(adoptions).toBe(mode === "recover" ? 0 : 1);
    if (mode === "recover")
      expect(events.some((event) => event.type === "turn.terminal")).toBe(false);
    expect(client.resumes).toEqual([]);
  }).pipe(Effect.scoped, Effect.provide(Layer.mergeAll(NodeServices.layer, IdAllocator.layer))),
);

it.effect("keeps live capture and terminal intake flowing while native inventory is stalled", () =>
  Effect.gen(function* () {
    const fs = yield* FileSystem.FileSystem;
    const path = yield* Path.Path;
    const root = yield* fs.makeTempDirectoryScoped();
    const project = path.join(root, "projects", "workspace");
    yield* fs.makeDirectory(project, { recursive: true });
    const records = [
      { type: "user", uuid: "live-user", parentUuid: null, message: { content: "Run foreground" } },
      {
        type: "assistant",
        uuid: "live-call",
        parentUuid: "live-user",
        message: {
          content: [
            {
              type: "tool_use",
              id: "live-tool",
              name: "Bash",
              input: { command: "run foreground" },
            },
          ],
          stop_reason: "tool_use",
        },
      },
    ];
    yield* fs.writeFileString(
      path.join(project, `${nativeId}.jsonl`),
      records
        .map((record) =>
          JSON.stringify({
            ...record,
            sessionId: nativeId,
            cwd: "/work",
            timestamp: "2026-10-29T00:00:00Z",
          }),
        )
        .join("\n") + "\n",
    );
    let pending: (() => void) | undefined;
    const blocked = new Promise<void>((resolve) => {
      pending = resolve;
    });
    let release: (() => void) | undefined;
    const stalled = new Promise<void>((resolve) => {
      release = resolve;
    });
    let sendState: (() => void) | undefined;
    const stateReady = new Promise<void>((resolve) => {
      sendState = resolve;
    });
    let sendStream: (() => void) | undefined;
    const streamReady = new Promise<void>((resolve) => {
      sendStream = resolve;
    });
    let streamConsumed = false;
    let stall = false;
    class StalledInventoryClient extends FixtureClient {
      override async *subscribe(_job: NativeClaudeSession, signal?: AbortSignal) {
        yield { type: "snapshot", record: { sessionId: nativeId } };
        await stateReady;
        yield { type: "state", patch: { tempo: "active" } };
        await streamReady;
        yield { type: "stream", line: "TERMINAL_PROGRESS" };
        streamConsumed = true;
        await new Promise<void>((resolve) => {
          if (signal?.aborted) resolve();
          else signal?.addEventListener("abort", () => resolve(), { once: true });
        });
      }
      override async listSessions() {
        if (stall) {
          pending?.();
          await stalled;
        }
        return this.inventory;
      }
    }
    yield* Effect.addFinalizer(() =>
      Effect.sync(() => {
        release?.();
        sendState?.();
        sendStream?.();
      }),
    );
    const client = new StalledInventoryClient("background");
    Object.assign(client.inventory[0]!, { status: "busy", state: "working" });
    let output = "FIRST";
    let delivered = "";
    let captureDirectory: unknown;
    const spy = vi
      .spyOn(NativeClaudeSessionOutput.prototype, "sample")
      .mockImplementation(async function (this: NativeClaudeSessionOutput) {
        captureDirectory = Reflect.get(this, "temporaryDirectory");
        if (delivered === output) return [];
        delivered = output;
        return [{ source: "workspace/live.output", chunk: output, output, truncated: false }];
      });
    yield* Effect.addFinalizer(() => Effect.sync(() => spy.mockRestore()));
    const now = DateTime.makeUnsafe(1);
    const providerThread: OrchestrationV2ProviderThread = {
      id: ProviderThreadId.make("live-thread"),
      driver: ProviderDriverKind.make("claudeAgent"),
      providerInstanceId: instanceId,
      providerSessionId: ProviderSessionId.make("live-session"),
      appThreadId: threadId,
      nativeThreadRef: {
        driver: ProviderDriverKind.make("claudeAgent"),
        nativeId,
        strength: "strong",
      },
      nativeConversationHeadRef: null,
      status: "active",
      ownerNodeId: null,
      firstRunOrdinal: null,
      lastRunOrdinal: null,
      handoffIds: [],
      forkedFrom: null,
      createdAt: now,
      updatedAt: now,
    };
    const owner: ProviderAdapter.ProviderAdapterV2TurnInput = {
      threadId,
      providerThread,
      modelSelection,
      runtimePolicy,
      runId: RunId.make("live-run"),
      runOrdinal: 1,
      providerTurnOrdinal: 1,
      attemptId: RunAttemptId.make("live-attempt"),
      rootNodeId: NodeId.make("live-root"),
      appThread: fixtureAppThread(now, providerThread.id),
      message: {
        createdBy: "user",
        creationSource: "provider",
        messageId: MessageId.make("live-message"),
        text: "Run foreground",
        attachments: [],
      },
    };
    const adapter = yield* makeNativeClaudeAdapter({
      instanceId,
      configDirectory: root,
      binaryPath: "never-executed",
      // Stock Claude task spools ignore the process TMPDIR override.
      environment: { TMPDIR: path.join(root, "unrelated-host-temp") },
      client,
    });
    const runtime = yield* adapter.openSession({
      threadId,
      providerSessionId: providerThread.providerSessionId!,
      modelSelection,
      runtimePolicy,
      onExternalTurn: () => Effect.succeed(owner),
    });
    const events: ProviderAdapter.ProviderAdapterV2Event[] = [];
    const first = yield* Deferred.make<void>();
    const second = yield* Deferred.make<void>();
    yield* runtime.events.pipe(
      Stream.runForEach((event) =>
        Effect.gen(function* () {
          events.push(event);
          if (event.type === "command_output.delta" && event.chunk === "FIRST")
            yield* Deferred.succeed(first, undefined);
          if (event.type === "command_output.delta" && event.chunk === "SECOND")
            yield* Deferred.succeed(second, undefined);
        }),
      ),
      Effect.forkScoped,
    );
    yield* runtime.resumeThread({ threadId, providerThread });
    yield* Deferred.await(first);
    expect(captureDirectory).toBe("/tmp");
    stall = true;
    sendState?.();
    yield* Effect.promise(() => blocked);
    sendStream?.();
    output = "SECOND";
    yield* TestClock.adjust("250 millis");
    yield* TestClock.adjust("250 millis");
    const receivedTerminal = events.some(
      (event) => event.type === "command_output.delta" && event.chunk.includes("TERMINAL_PROGRESS"),
    );
    const consumedBeforeRelease = streamConsumed;
    const deliveredBeforeRelease = delivered;
    stall = false;
    release?.();
    expect(deliveredBeforeRelease).toBe("SECOND");
    expect(consumedBeforeRelease).toBe(true);
    expect(receivedTerminal).toBe(true);
    yield* Deferred.await(second);
    expect(
      events.some((event) => event.type === "command_output.delta" && event.chunk === "SECOND"),
    ).toBe(true);
    stall = false;
    release?.();
    yield* runtime.unloadThread!({ providerThread });
    const detachedCount = events.length;
    output = "DETACHED";
    yield* TestClock.adjust("250 millis");
    expect(delivered).toBe("SECOND");
    expect(events).toHaveLength(detachedCount);
  }).pipe(Effect.scoped, Effect.provide(Layer.mergeAll(NodeServices.layer, IdAllocator.layer))),
);
