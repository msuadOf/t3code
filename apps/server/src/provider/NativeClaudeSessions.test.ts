import * as NodeServices from "@effect/platform-node/NodeServices";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Path from "effect/Path";
import { NativeClaudeClient } from "./NativeClaudeClient.ts";
import { NativeClaudeSessions } from "./NativeClaudeSessions.ts";
import { describe, expect, it } from "@effect/vitest";
import { decodeNativeClaudeTranscript, nativeClaudeHistory } from "./NativeClaudeSessions.ts";

describe("Claude native transcript projection", () => {
  it("ignores an incomplete live tail but refuses corrupted persisted lines", () => {
    expect(decodeNativeClaudeTranscript('{"type":"user"}\n{"type":')).toEqual([{ type: "user" }]);
    expect(() => decodeNativeClaudeTranscript('{bad}\n{"type":"user"}\n')).toThrow();
  });

  it("follows the current branch and pairs tool outputs without inventing independent turns", () => {
    const history = nativeClaudeHistory([
      {
        type: "user",
        uuid: "prompt",
        parentUuid: null,
        timestamp: "2026-10-23T00:00:00Z",
        message: { content: "hello" },
      },
      {
        type: "assistant",
        uuid: "abandoned",
        parentUuid: "prompt",
        message: { content: [{ type: "text", text: "discarded" }] },
      },
      {
        type: "assistant",
        uuid: "thinking",
        parentUuid: "prompt",
        message: {
          content: [
            { type: "thinking", thinking: "reasoning" },
            { type: "tool_use", id: "tool", name: "Read", input: { file_path: "/work/a" } },
          ],
          stop_reason: "tool_use",
        },
      },
      {
        type: "user",
        uuid: "result",
        parentUuid: "thinking",
        message: {
          content: [{ type: "tool_result", tool_use_id: "tool", content: "file content" }],
        },
      },
      { type: "system", uuid: "bridge", parentUuid: "result" },
      {
        type: "assistant",
        uuid: "done",
        parentUuid: "bridge",
        timestamp: "2026-10-23T00:00:01Z",
        message: { content: [{ type: "text", text: "done" }], stop_reason: "end_turn" },
      },
    ]);
    expect(history).toHaveLength(1);
    expect(history[0]?.items.map((item) => item.type)).toEqual([
      "user",
      "reasoning",
      "tool",
      "assistant",
    ]);
    expect(history[0]?.items[2]).toMatchObject({
      id: "tool",
      status: "completed",
      text: "file content",
    });
    expect(history[0]?.items.some((item) => item.text === "discarded")).toBe(false);
    expect(history[0]?.completedAt).toBe(Date.parse("2026-10-23T00:00:01Z"));
  });
});

it("keeps a live user turn and an unfinished tool turn running until an end_turn record", () => {
  const first = {
    type: "user",
    uuid: "user",
    parentUuid: null,
    timestamp: "2026-10-23T00:00:00Z",
    message: { content: "run" },
  };
  expect(nativeClaudeHistory([first])[0]).toMatchObject({ status: "running", completedAt: null });
  const tool = {
    type: "assistant",
    uuid: "tool-turn",
    parentUuid: "user",
    message: {
      content: [{ type: "tool_use", id: "tool", name: "Bash", input: { command: "pwd" } }],
      stop_reason: "tool_use",
    },
  };
  expect(nativeClaudeHistory([first, tool])[0]).toMatchObject({
    status: "running",
    completedAt: null,
  });
});

it.effect(
  "reuses discovery inventory and treats idle status as a completed turn while worker state remains working",
  () =>
    Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem;
      const path = yield* Path.Path;
      const root = yield* fs.makeTempDirectoryScoped();
      const directory = path.join(root, "projects", "workspace");
      yield* fs.makeDirectory(directory, { recursive: true });
      const sessionId = "11111111-2222-4333-8444-555555555555";
      yield* fs.writeFileString(
        path.join(directory, `${sessionId}.jsonl`),
        JSON.stringify({
          type: "user",
          uuid: "user",
          parentUuid: null,
          sessionId,
          cwd: "/work",
          message: { content: "hello" },
        }) +
          "\n" +
          JSON.stringify({
            type: "assistant",
            uuid: "assistant",
            parentUuid: "user",
            sessionId,
            timestamp: "2026-10-23T00:00:01Z",
            message: { content: [{ type: "text", text: "done" }], stop_reason: "end_turn" },
          }) +
          "\n",
      );
      let reads = 0;
      class Client extends NativeClaudeClient {
        override async listSessions() {
          reads += 1;
          return [
            {
              kind: "background" as const,
              cwd: "/work",
              startedAt: 1,
              sessionId,
              id: "11111111",
              status: "idle",
              state: "working",
            },
          ];
        }
      }
      const source = new NativeClaudeSessions(root, new Client());
      const sessions = yield* Effect.promise(() => source.list());
      expect(sessions).toHaveLength(1);
      const history = yield* Effect.promise(() => source.history(sessionId));
      expect(history[0]).toMatchObject({
        status: "completed",
        completedAt: Date.parse("2026-10-23T00:00:01Z"),
      });
      yield* Effect.promise(() => source.history(sessionId));
      expect(reads).toBe(1);
    }).pipe(Effect.scoped, Effect.provide(NodeServices.layer)),
);
