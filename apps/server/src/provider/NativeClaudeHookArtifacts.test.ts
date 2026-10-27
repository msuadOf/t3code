import { expect, it } from "@effect/vitest";
import { RuntimeRequestId } from "@t3tools/contracts";
import { nativeClaudeHookResponse } from "./NativeClaudeHookArtifacts.ts";
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
