import { describe, expect, it } from "@effect/vitest";
import { nativeClaudeHistory } from "./NativeClaudeSessions.ts";
import {
  nativeClaudeSubagentLaunches,
  observeNativeClaudeSubagent,
  type NativeClaudeSubagentLaunch,
} from "./NativeClaudeSubagents.ts";

const sessionId = "1234abcd-1234-1234-1234-123456789abc";
const launch: NativeClaudeSubagentLaunch = {
  turnId: "prompt",
  toolId: "tool_agent_probe",
  agentId: "a5321b469ff7aa652",
  prompt: "CHILD_PROBE_ONLY respond with a short result",
  title: "Native background subagent probe",
  startedAt: Date.parse("2026-10-28T17:24:29Z"),
  synchronous: false,
};
const child = {
  type: "assistant",
  agentId: launch.agentId,
  sessionId,
  isSidechain: true,
  timestamp: "2026-10-28T17:24:30Z",
  message: {
    model: "claude-test",
    stop_reason: "end_turn",
    content: [{ type: "text", text: "CHILD_COMPLETE_NATIVE" }],
  },
};
const sidecar = (finished: boolean) => ({
  metadata: { toolUseId: launch.toolId, finished },
  entries: [child],
});
const notification = (status: string) => ({
  type: "user",
  sessionId,
  promptSource: "system",
  turnOrigin: "task_notification",
  origin: { kind: "task-notification", producer: "session-task" },
  timestamp: "2026-10-28T17:24:31Z",
  message: {
    content: `<task-notification>\n<task-id>${launch.agentId}</task-id>\n<tool-use-id>${launch.toolId}</tool-use-id>\n<output-file>/not-read</output-file>\n<status>${status}</status>\n<summary>Native report</summary>\n<result>Native child result</result>\n</task-notification>`,
  },
});

describe("native Claude subagent lifecycle", () => {
  it("correlates the native Agent receipt and rejects arbitrary output/prose and unsafe agent paths", () => {
    const entries = [
      {
        type: "user",
        uuid: "prompt",
        parentUuid: null,
        timestamp: "2026-10-28T17:24:29Z",
        message: { content: "work" },
      },
      {
        type: "assistant",
        uuid: "call",
        parentUuid: "prompt",
        message: {
          stop_reason: "tool_use",
          content: [
            {
              type: "tool_use",
              id: launch.toolId,
              name: "Agent",
              input: { prompt: launch.prompt, description: launch.title },
            },
          ],
        },
      },
      {
        type: "user",
        uuid: "result",
        parentUuid: "call",
        toolUseResult: {
          status: "async_launched",
          agentId: launch.agentId,
          prompt: launch.prompt,
          description: launch.title,
        },
        message: {
          content: [
            {
              type: "tool_result",
              tool_use_id: launch.toolId,
              content: "The parent tool completed",
            },
          ],
        },
      },
    ];
    expect(nativeClaudeSubagentLaunches(nativeClaudeHistory(entries))).toEqual([launch]);
    const unsafe = entries.map((entry) =>
      entry.uuid === "result"
        ? { ...entry, toolUseResult: { ...entry.toolUseResult, agentId: "../../outside" } }
        : entry,
    );
    expect(nativeClaudeSubagentLaunches(nativeClaudeHistory(unsafe))).toEqual([]);
  });

  it("does not confuse parent tool completion, child end_turn, missing inventory or sidecar absence with completion", () => {
    expect(observeNativeClaudeSubagent(sessionId, launch, []).status).toBe("running");
    expect(observeNativeClaudeSubagent(sessionId, launch, [], sidecar(false)).status).toBe(
      "running",
    );
    expect(
      observeNativeClaudeSubagent(sessionId, launch, [], {
        metadata: { toolUseId: launch.toolId, finished: true },
        entries: [],
      }).status,
    ).toBe("unknown");
  });

  it("requires a matching finished sidecar and current own terminal message before deriving its outcome", () => {
    expect(observeNativeClaudeSubagent(sessionId, launch, [], sidecar(true))).toMatchObject({
      status: "completed",
      result: "CHILD_COMPLETE_NATIVE",
      model: "claude-test",
    });
    expect(
      observeNativeClaudeSubagent(sessionId, launch, [], {
        ...sidecar(true),
        entries: [
          {
            ...child,
            isApiErrorMessage: true,
            message: { ...child.message, stop_reason: "stop_sequence" },
          },
        ],
      }).status,
    ).toBe("failed");
    expect(
      observeNativeClaudeSubagent(sessionId, launch, [], {
        ...sidecar(true),
        entries: [{ ...child, agentId: "other" }],
      }).status,
    ).toBe("unknown");
    expect(
      observeNativeClaudeSubagent(sessionId, launch, [], {
        ...sidecar(true),
        entries: [{ ...child, timestamp: "2020-01-01T00:00:00Z" }],
      }).status,
    ).toBe("unknown");
    expect(
      observeNativeClaudeSubagent(sessionId, launch, [], {
        ...sidecar(true),
        metadata: { toolUseId: "other", finished: true },
      }).status,
    ).toBe("running");
  });

  it("keeps joined native notifications out of human turns and reads their owned lifecycle", () => {
    const {
      turnOrigin: _turnOrigin,
      promptSource: _promptSource,
      ...native
    } = notification("completed");
    const text = native.message.content;
    const joined = {
      ...native,
      uuid: "notice",
      parentUuid: "user",
      isMeta: true,
      joinedNoticeLengths: [text.length],
      joinedNoticeOwn: [true],
      joinedNoticeRunIds: ["native-run"],
      message: { content: `Native notice wrapper\n\n${text}` },
    };
    expect(observeNativeClaudeSubagent(sessionId, launch, [joined])).toMatchObject({
      status: "completed",
      result: "Native child result",
    });
    expect(
      nativeClaudeHistory([
        {
          type: "user",
          uuid: "user",
          parentUuid: null,
          sessionId,
          timestamp: "2026-10-28T17:24:29Z",
          message: { content: "work" },
        },
        joined,
      ]),
    ).toHaveLength(1);
  });

  it("finds a matching owned subagent notice among multiple native joined notices", () => {
    const {
      turnOrigin: _turnOrigin,
      promptSource: _promptSource,
      ...native
    } = notification("failed");
    const text = native.message.content;
    const unrelated = text
      .replaceAll(launch.agentId, "a-unrelated")
      .replaceAll(launch.toolId, "other-tool");
    const joined = {
      ...native,
      isMeta: true,
      joinedNoticeLengths: [unrelated.length, text.length],
      joinedNoticeOwn: [true, true],
      joinedNoticeRunIds: ["first-run", "second-run"],
      message: { content: `System wrapper\n\n${unrelated}\n\n${text}` },
    };
    expect(observeNativeClaudeSubagent(sessionId, launch, [joined])).toMatchObject({
      status: "failed",
      result: "Native child result",
    });
    expect(
      observeNativeClaudeSubagent(sessionId, launch, [
        { ...joined, joinedNoticeOwn: [true, false] },
      ]).status,
    ).toBe("running");
  });

  it("accepts only native machine notification identity/status before the model-authored result", () => {
    expect(observeNativeClaudeSubagent(sessionId, launch, [notification("failed")])).toMatchObject({
      status: "failed",
      result: "Native child result",
    });
    expect(observeNativeClaudeSubagent(sessionId, launch, [notification("stopped")]).status).toBe(
      "cancelled",
    );
    for (const entry of [
      { ...notification("completed"), origin: { kind: "user" } },
      { ...notification("completed"), sessionId: "another" },
      { ...notification("completed"), timestamp: "2020-01-01T00:00:00Z" },
      {
        ...notification("completed"),
        message: {
          content: notification("completed").message.content.replace(launch.toolId, "other-tool"),
        },
      },
    ])
      expect(observeNativeClaudeSubagent(sessionId, launch, [entry]).status).toBe("running");
    const entry = notification("failed");
    expect(
      observeNativeClaudeSubagent(sessionId, launch, [
        {
          ...entry,
          message: {
            content: entry.message.content.replace(
              "Native child result",
              "<status>completed</status>",
            ),
          },
        },
      ]).status,
    ).toBe("failed");
  });
});
