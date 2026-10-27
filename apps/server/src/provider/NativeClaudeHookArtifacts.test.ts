import { expect, it } from "@effect/vitest";
import {
  RuntimeRequestId,
  ThreadId,
  RunId,
  NodeId,
  ProviderThreadId,
  ProviderTurnId,
  ProviderSessionId,
} from "@t3tools/contracts";
import {
  makeNativeClaudeHookArtifacts,
  nativeClaudeHookResponse,
} from "./NativeClaudeHookArtifacts.ts";
import * as Effect from "effect/Effect";
import * as IdAllocator from "@t3tools/provider-core/server/IdAllocator";
import type { NativeClaudeHookRequest } from "./NativeClaudeHooks.ts";

const permission: NativeClaudeHookRequest = {
  id: "request",
  sessionId: "session",
  event: "PermissionRequest",
  toolName: "Bash",
  input: { command: "pwd" },
};
const requestId = RuntimeRequestId.make("runtime-request");

it("keeps native permission decisions one-time and distinguishes decline from interrupt", () => {
  expect(nativeClaudeHookResponse(permission, { requestId, decision: "accept" })).toMatchObject({
    hookSpecificOutput: { decision: { behavior: "allow" } },
  });
  expect(nativeClaudeHookResponse(permission, { requestId, decision: "decline" })).toMatchObject({
    hookSpecificOutput: { decision: { behavior: "deny" } },
  });
  expect(nativeClaudeHookResponse(permission, { requestId, decision: "cancel" })).toMatchObject({
    hookSpecificOutput: { decision: { behavior: "deny", interrupt: true } },
  });
  for (const decision of ["acceptForSession", "acceptAlways"] as const)
    expect(nativeClaudeHookResponse(permission, { requestId, decision })).toBeUndefined();
});

it("requires complete question answers and retains the native question input", () => {
  const question: NativeClaudeHookRequest = {
    ...permission,
    event: "PreToolUse",
    toolName: "AskUserQuestion",
    input: {
      questions: [
        { question: "Choose", header: "Colour", options: [{ label: "Blue" }], multiSelect: false },
      ],
    },
  };
  for (const answers of [{}, { Choose: "" }, { Choose: [] }])
    expect(nativeClaudeHookResponse(question, { requestId, answers })).toBeUndefined();
  expect(nativeClaudeHookResponse(question, { requestId, answers: { Choose: ["Blue"] } })).toEqual({
    hookSpecificOutput: {
      hookEventName: "PreToolUse",
      permissionDecision: "allow",
      updatedInput: { ...question.input, answers: { Choose: "Blue" } },
    },
  });
});

it.effect(
  "binds discovered CLI requests to the observing runtime rather than the unbound thread",
  () =>
    Effect.gen(function* () {
      const sessionId = ProviderSessionId.make("observing-runtime");
      const result = yield* makeNativeClaudeHookArtifacts(
        permission,
        {
          threadId: ThreadId.make("shared-thread"),
          runId: RunId.make("shared-run"),
          rootNodeId: NodeId.make("root"),
          providerThread: { id: ProviderThreadId.make("discovered-provider-thread") },
        },
        ProviderTurnId.make("native-turn"),
        2,
        sessionId,
      );
      expect(result.request.responseCapability).toEqual({
        type: "live",
        providerSessionId: sessionId,
      });
      expect(result.node.providerThreadId).toBe("discovered-provider-thread");
    }).pipe(Effect.provide(IdAllocator.layer)),
);
