import { expect, it } from "vite-plus/test";
import { nativeClaudeBackgroundCommands } from "./NativeClaudeBackgroundCommands.ts";
import type { NativeHistoryTurn } from "@t3tools/provider-core/server/nativeSessions";
const sessionId = "11111111-2222-4333-8444-555555555555";
const outputPath = `/private/tmp/claude-501/project/${sessionId}/tasks/task.output`;
const history: readonly NativeHistoryTurn[] = [
  {
    id: "turn",
    status: "completed",
    startedAt: 1,
    completedAt: 2,
    items: [
      {
        type: "tool",
        id: "tool",
        title: "Bash",
        input: { command: "echo hello" },
        text: "",
        status: "completed",
      },
    ],
  },
];
const call = {
  type: "assistant",
  sessionId,
  message: { content: [{ type: "tool_use", id: "tool", name: "Bash" }] },
};
const receipt = {
  type: "user",
  sessionId,
  toolUseResult: { backgroundTaskId: "task" },
  message: {
    content: [
      {
        type: "tool_result",
        tool_use_id: "tool",
        content: `Command running in background with ID: task. Output is being written to: ${outputPath}. You will be notified.`,
      },
    ],
  },
};
const notification = {
  type: "user",
  sessionId,
  origin: { kind: "task-notification", producer: "session-task" },
  turnOrigin: "task_notification",
  promptSource: "system",
  timestamp: "2026-10-29T00:00:00Z",
  message: {
    content: `<task-notification>\n<task-id>task</task-id>\n<tool-use-id>tool</tool-use-id>\n<output-file>${outputPath}</output-file>\n<status>completed</status>\n</task-notification>`,
  },
};
it("retains running background command after its parent model turn ends", () => {
  expect(nativeClaudeBackgroundCommands(sessionId, history, [call, receipt])).toMatchObject([
    {
      turnId: "turn",
      toolId: "tool",
      taskId: "task",
      outputPath,
      status: "running",
      command: "echo hello",
    },
  ]);
  expect(
    nativeClaudeBackgroundCommands(sessionId, history, [call, receipt, notification]),
  ).toMatchObject([{ status: "completed", completedAt: Date.parse(notification.timestamp) }]);
});
it("requires native receipt and notification provenance rather than matching user XML", () => {
  expect(nativeClaudeBackgroundCommands(sessionId, history, [call])).toEqual([]);
  expect(
    nativeClaudeBackgroundCommands(sessionId, history, [call, { ...receipt, sessionId: "other" }]),
  ).toEqual([]);
  expect(
    nativeClaudeBackgroundCommands(sessionId, history, [
      call,
      receipt,
      { ...notification, origin: { kind: "human" } },
    ]),
  ).toMatchObject([{ status: "running" }]);
  expect(
    nativeClaudeBackgroundCommands(sessionId, history, [
      call,
      receipt,
      {
        ...notification,
        message: {
          content: notification.message.content.replace("<tool-use-id>tool", "<tool-use-id>other"),
        },
      },
    ]),
  ).toMatchObject([{ status: "running" }]);
});

it("rejects ambiguous native receipts for the same original Bash tool", () => {
  const other = {
    ...receipt,
    toolUseResult: { backgroundTaskId: "other-task" },
    message: {
      content: [
        {
          type: "tool_result",
          tool_use_id: "tool",
          content: `Output is being written to: ${outputPath.replace("task.output", "other-task.output")}.`,
        },
      ],
    },
  };
  expect(nativeClaudeBackgroundCommands(sessionId, history, [call, receipt, other])).toEqual([]);
});
