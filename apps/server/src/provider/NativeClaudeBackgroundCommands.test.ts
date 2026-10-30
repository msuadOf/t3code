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

it("accepts native joined background notices with explicit owned envelope", () => {
  const { turnOrigin: _turnOrigin, promptSource: _promptSource, ...native } = notification;
  const text = notification.message.content;
  const wrapped = {
    ...native,
    isMeta: true,
    joinedNoticeLengths: [text.length],
    joinedNoticeOwn: [true],
    joinedNoticeRunIds: ["native-run"],
    message: { content: `Native system notice wrapper\n\n${text}` },
  };
  expect(
    nativeClaudeBackgroundCommands(sessionId, history, [call, receipt, wrapped])[0]?.status,
  ).toBe("completed");
  for (const invalid of [
    { ...wrapped, isMeta: false },
    { ...wrapped, joinedNoticeOwn: [false] },
    { ...wrapped, joinedNoticeLengths: [text.length - 1] },
    { ...wrapped, joinedNoticeRunIds: [] },
    { ...wrapped, origin: { kind: "user" } },
  ])
    expect(
      nativeClaudeBackgroundCommands(sessionId, history, [call, receipt, invalid])[0]?.status,
    ).toBe("running");
});

it("finishes both background commands in an owned joined notification without accepting foreign notices", () => {
  const secondPath = outputPath.replace("/task.output", "/tasktwo.output");
  const secondText = notification.message.content
    .replaceAll(">task<", ">tasktwo<")
    .replaceAll(">tool<", ">tool-two<")
    .replaceAll(outputPath, secondPath);
  const firstText = notification.message.content;
  const { turnOrigin: _turnOrigin, promptSource: _promptSource, ...native } = notification;
  const joined = {
    ...native,
    isMeta: true,
    joinedNoticeLengths: [firstText.length, secondText.length],
    joinedNoticeOwn: [true, true],
    joinedNoticeRunIds: ["first-run", "second-run"],
    message: { content: `Native wrapper\n\n${firstText}\n\n${secondText}` },
  };
  const entries = [
    call,
    receipt,
    { ...call, message: { content: [{ type: "tool_use", id: "tool-two", name: "Bash" }] } },
    {
      ...receipt,
      toolUseResult: { backgroundTaskId: "tasktwo" },
      message: {
        content: [
          {
            type: "tool_result",
            tool_use_id: "tool-two",
            content: `Output is being written to: ${secondPath}.`,
          },
        ],
      },
    },
  ];
  const turns = [
    { ...history[0]!, items: [...history[0]!.items, { ...history[0]!.items[0]!, id: "tool-two" }] },
  ];
  expect(
    nativeClaudeBackgroundCommands(sessionId, turns, [...entries, joined]).map(
      (command) => command.status,
    ),
  ).toEqual(["completed", "completed"]);
  expect(
    nativeClaudeBackgroundCommands(sessionId, turns, [
      ...entries,
      { ...joined, joinedNoticeOwn: [true, false] },
    ]).map((command) => command.status),
  ).toEqual(["completed", "running"]);
});
