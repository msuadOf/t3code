import { describe, expect, it } from "@effect/vitest";
import type { NativeHistoryItem } from "@t3tools/provider-core/server/nativeSessions";
import { nativeClaudeHistory } from "./NativeClaudeSessions.ts";
import { nativeClaudePlan } from "./NativeClaudePlanning.ts";

const tool = (input: Partial<NativeHistoryItem>): NativeHistoryItem => ({
  id: "tool",
  type: "tool",
  title: "TodoWrite",
  status: "completed",
  text: "",
  ...input,
});

describe("native Claude planning fields", () => {
  it("uses actual TodoWrite result, including completion and clearing, rather than the attempted input", () => {
    expect(
      nativeClaudePlan(tool({ input: { todos: [{ content: "Attempt", status: "pending" }] } })),
    ).toBeUndefined();
    expect(
      nativeClaudePlan(
        tool({
          input: { todos: [{ content: "Attempt", status: "pending" }] },
          result: {
            newTodos: [
              { content: "Actual", status: "in_progress", activeForm: "Working" },
              { content: "Done", status: "completed" },
            ],
          },
        }),
      ),
    ).toEqual({
      kind: "todo_list",
      steps: [
        { id: "todo-0", text: "Actual", status: "running" },
        { id: "todo-1", text: "Done", status: "completed" },
      ],
    });
    expect(nativeClaudePlan(tool({ result: { newTodos: [] } }))).toEqual({
      kind: "todo_list",
      steps: [],
    });
    for (const status of ["running", "failed", "cancelled"] as const)
      expect(nativeClaudePlan(tool({ status, result: { newTodos: [] } }))).toBeUndefined();
    expect(
      nativeClaudePlan(tool({ result: { newTodos: [{ content: "Unknown", status: "maybe" }] } })),
    ).toBeUndefined();
  });

  it("projects only explicit ExitPlanMode plan fields and preserves the native edited result", () => {
    expect(
      nativeClaudePlan(
        tool({ title: "ExitPlanMode", status: "running", input: { plan: "# Proposed" } }),
      ),
    ).toEqual({ kind: "proposed_plan", markdown: "# Proposed" });
    expect(
      nativeClaudePlan(
        tool({ title: "ExitPlanMode", input: { plan: "Original" }, result: { plan: "Edited" } }),
      ),
    ).toEqual({ kind: "proposed_plan", markdown: "Edited" });
    expect(
      nativeClaudePlan(
        tool({
          title: "ExitPlanMode",
          text: "# Output prose",
          input: { planFilePath: "/do-not-read" },
        }),
      ),
    ).toBeUndefined();
    expect(
      nativeClaudePlan(tool({ title: "Bash", result: { plan: "not a plan tool" } })),
    ).toBeUndefined();
    expect(
      nativeClaudePlan(
        tool({ title: "ExitPlanMode", input: { plan: "Attempt" }, result: { plan: null } }),
      ),
    ).toBeUndefined();
  });

  it("retains structured toolUseResult from canonical JSONL pairing", () => {
    const result = nativeClaudeHistory([
      { type: "user", uuid: "prompt", parentUuid: null, message: { content: "work" } },
      {
        type: "assistant",
        uuid: "call",
        parentUuid: "prompt",
        message: {
          content: [{ type: "tool_use", id: "todo", name: "TodoWrite", input: { todos: [] } }],
          stop_reason: "tool_use",
        },
      },
      {
        type: "user",
        uuid: "result",
        parentUuid: "call",
        toolUseResult: {
          oldTodos: [],
          newTodos: [{ content: "Native task", status: "pending", activeForm: "Working" }],
        },
        message: {
          content: [{ type: "tool_result", tool_use_id: "todo", content: "native receipt" }],
        },
      },
    ]);
    expect(nativeClaudePlan(result[0]!.items[1]!)).toEqual({
      kind: "todo_list",
      steps: [{ id: "todo-0", text: "Native task", status: "pending" }],
    });
  });
});
