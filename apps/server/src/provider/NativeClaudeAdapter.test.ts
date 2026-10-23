import { describe, expect, it } from "@effect/vitest";
import {
  ProviderInstanceId,
  ProviderSessionId,
  ProviderThreadId,
  ProviderDriverKind,
  ThreadId,
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
        const adapter = yield* makeNativeClaudeAdapter({
          instanceId,
          binaryPath: "never-executed",
          configDirectory: "/nonexistent-claude-fixture",
          environment: {},
          client: new FixtureClient("background"),
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
    "refuses a live foreground session and explains backgrounding instead of spawning a second writer",
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
    expect(nativeClaudeCapabilities.turns.supportsInterrupt).toBe(false);
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
