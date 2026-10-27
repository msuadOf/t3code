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
            appThread: {
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
              activeProviderThreadId: result.id,
              lineage: { rootThreadId: threadId, parentThreadId: null, relationshipToParent: null },
              forkedFrom: null,
              createdAt: now,
              updatedAt: now,
              archivedAt: null,
              settledOverride: null,
              settledAt: null,
              lastVisitedAt: null,
              deletedAt: null,
            },
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
