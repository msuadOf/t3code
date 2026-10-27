import * as Layer from "effect/Layer";
import * as NodeServices from "@effect/platform-node/NodeServices";
import * as FileSystem from "effect/FileSystem";
import * as Path from "effect/Path";
import * as ProviderAdapter from "@t3tools/provider-core/server/ProviderAdapter";
import { describe, expect, it } from "@effect/vitest";
import {
  ProviderInstanceId,
  ProviderSessionId,
  ProviderThreadId,
  ProviderDriverKind,
  ThreadId,
  ProjectId,
  RunId,
  RunAttemptId,
  NodeId,
  MessageId,
} from "@t3tools/contracts";
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
      // Closing the observer's scope owns no worker teardown or input operation.
      expect((yield* adapter.getCapabilities()).sessions.runtimeSurvivesHostRestart).toBe(true);
    }).pipe(Effect.scoped, Effect.provide(Layer.mergeAll(NodeServices.layer, IdAllocator.layer))),
);
