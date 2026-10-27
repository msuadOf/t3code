import {
  ProviderDriverKind,
  type OrchestrationV2ExecutionNode,
  type OrchestrationV2RuntimeRequest,
  type OrchestrationV2TurnItem,
  type ProviderTurnId,
  type ProviderSessionId,
} from "@t3tools/contracts";
import * as ProviderAdapter from "@t3tools/provider-core/server/ProviderAdapter";
import * as IdAllocator from "@t3tools/provider-core/server/IdAllocator";
import * as Effect from "effect/Effect";
import * as DateTime from "effect/DateTime";
import * as Schema from "effect/Schema";
import {
  claudeUserInputQuestions,
  claudeSdkUserInputAnswers,
} from "../orchestration-v2/Adapters/ClaudeAdapterV2.ts";
import type { NativeClaudeHookRequest } from "./NativeClaudeHooks.ts";

const encodeJson = Schema.encodeSync(Schema.fromJsonString(Schema.Unknown));

const driver = ProviderDriverKind.make("claudeAgent");

export function nativeClaudeHookResponse(
  hook: NativeClaudeHookRequest,
  response: ProviderAdapter.ProviderAdapterV2RuntimeRequestResponseInput,
): Record<string, unknown> | undefined {
  if (hook.event === "PreToolUse") {
    const questions = claudeUserInputQuestions(hook.input);
    const answers = response.answers;
    if (
      !answers ||
      !questions.length ||
      questions.some((q) => {
        const value = answers[q.id];
        return (
          !Object.hasOwn(answers, q.id) ||
          (typeof value === "string" ? !value.trim() : !Array.isArray(value) || !value.length)
        );
      })
    )
      return undefined;
    return {
      hookSpecificOutput: {
        hookEventName: "PreToolUse",
        permissionDecision: "allow",
        updatedInput: { ...hook.input, answers: claudeSdkUserInputAnswers(answers) },
      },
    };
  }
  // This path grants one native request; persistent permission policy stays with the CLI.
  if (
    !response.decision ||
    response.decision === "acceptAlways" ||
    response.decision === "acceptForSession"
  )
    return undefined;
  return {
    hookSpecificOutput: {
      hookEventName: "PermissionRequest",
      decision: {
        behavior: response.decision === "accept" ? "allow" : "deny",
        ...(response.decision === "cancel" ? { interrupt: true } : {}),
      },
    },
  };
}

export const makeNativeClaudeHookArtifacts = Effect.fn("NativeClaudeHookArtifacts.make")(function* (
  hook: NativeClaudeHookRequest,
  owner: Pick<ProviderAdapter.ProviderAdapterV2TurnInput, "threadId" | "runId" | "rootNodeId"> & {
    readonly providerThread: Pick<
      ProviderAdapter.ProviderAdapterV2TurnInput["providerThread"],
      "id"
    >;
  },
  providerTurnId: ProviderTurnId,
  ordinal: number,
  providerSessionId: ProviderSessionId,
) {
  const allocator = yield* IdAllocator.IdAllocatorV2;
  const createdAt = yield* DateTime.now;
  const id = yield* allocator.allocate.runtimeRequest({
    driver,
    providerTurnId,
    nativeRequestId: hook.id,
  });
  const nodeId = allocator.derive.approvalNode({ requestId: id });
  const questions = hook.event === "PreToolUse" ? claudeUserInputQuestions(hook.input) : undefined;
  const kind = questions
    ? "user_input"
    : hook.toolName === "Bash"
      ? "command"
      : ["Write", "Edit", "NotebookEdit"].includes(hook.toolName)
        ? "file-change"
        : ["Read", "Glob", "Grep"].includes(hook.toolName)
          ? "file-read"
          : "permission";
  const nativeItemRef = { driver, nativeId: hook.id, strength: "strong" as const };
  const node: OrchestrationV2ExecutionNode = {
    id: nodeId,
    threadId: owner.threadId,
    runId: owner.runId,
    parentNodeId: owner.rootNodeId,
    rootNodeId: owner.rootNodeId,
    kind: questions ? "user_input_request" : "approval_request",
    status: "waiting",
    countsForRun: false,
    providerThreadId: owner.providerThread.id,
    providerTurnId,
    nativeItemRef,
    runtimeRequestId: id,
    checkpointScopeId: null,
    startedAt: createdAt,
    completedAt: null,
  };
  const request: OrchestrationV2RuntimeRequest = {
    id,
    nodeId,
    providerTurnId,
    nativeRequestRef: nativeItemRef,
    kind,
    status: "pending",
    responseCapability: {
      type: "live",
      providerSessionId: providerSessionId,
    },
    createdAt,
    resolvedAt: null,
  };
  const item: OrchestrationV2TurnItem = {
    id: allocator.derive.approvalTurnItem({ requestId: id }),
    threadId: owner.threadId,
    runId: owner.runId,
    nodeId,
    providerThreadId: owner.providerThread.id,
    providerTurnId,
    nativeItemRef,
    parentItemId: null,
    ordinal,
    status: "waiting",
    title: hook.toolName,
    startedAt: createdAt,
    completedAt: null,
    updatedAt: createdAt,
    ...(questions
      ? { type: "user_input_request" as const, requestId: id, questions: [...questions] }
      : {
          type: "approval_request" as const,
          requestId: id,
          requestKind: kind as "command" | "file-change" | "file-read" | "permission",
          options: [
            { decision: "accept", label: "Allow once" },
            { decision: "decline", label: "Deny" },
            { decision: "cancel", label: "Cancel turn" },
          ],
          prompt: `${hook.toolName}: ${encodeJson(hook.input)}`,
        }),
  };
  return { node, request, item };
});
