import * as NodeServices from "@effect/platform-node/NodeServices";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Path from "effect/Path";
import { NativeClaudeClient, type NativeClaudeSession } from "./NativeClaudeClient.ts";
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
      input: { file_path: "/work/a" },
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
  expect(nativeClaudeHistory([first, tool])[0]?.items[1]).toMatchObject({
    input: { command: "pwd" },
    text: "",
  });
  expect(
    nativeClaudeHistory([
      first,
      { ...tool, message: { content: [{ type: "text", text: "partial response" }] } },
    ])[0],
  ).toMatchObject({ status: "running", completedAt: null });
});

it.effect(
  "discovers only daemon background sessions and preserves history for already observed threads",
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
      let inventory: readonly NativeClaudeSession[] = [
        {
          kind: "background",
          cwd: "/work",
          startedAt: 1,
          sessionId,
          id: "11111111",
          status: "idle",
          state: "blocked",
        },
      ];
      class Client extends NativeClaudeClient {
        override async listSessions() {
          reads += 1;
          return inventory;
        }
      }
      const source = new NativeClaudeSessions(root, new Client());
      const sessions = yield* Effect.promise(() => source.list());
      expect(sessions[0]).toMatchObject({ sharingState: "shared" });
      expect(sessions).toHaveLength(1);
      expect(sessions[0]?.active).toBe(false);
      const history = yield* Effect.promise(() => source.history(sessionId));
      expect(history[0]).toMatchObject({
        status: "completed",
        completedAt: Date.parse("2026-10-23T00:00:01Z"),
      });
      yield* Effect.promise(() => source.history(sessionId));
      expect(reads).toBe(1);
      expect(sessions[0]?.title).toBe("hello");
      const background = inventory[0]!;
      inventory = [{ ...background, name: "Native generated title" }];
      expect((yield* Effect.promise(() => source.list()))[0]?.title).toBe("Native generated title");
      inventory = [{ ...background, name: "Renamed in Claude" }];
      expect((yield* Effect.promise(() => source.list()))[0]?.title).toBe("Renamed in Claude");
      inventory = [{ ...background, name: background.id! }];
      expect((yield* Effect.promise(() => source.list()))[0]?.title).toBe("hello");
      inventory = [{ ...background, name: sessionId }];
      expect((yield* Effect.promise(() => source.list()))[0]?.title).toBe("hello");
      yield* fs.writeFileString(
        path.join(directory, `${sessionId}.jsonl`),
        (yield* fs.readFileString(path.join(directory, `${sessionId}.jsonl`))) +
          JSON.stringify({ type: "custom-title", customTitle: "Saved custom title" }) +
          "\n",
      );
      expect((yield* Effect.promise(() => source.list()))[0]?.title).toBe("Saved custom title");
      inventory = [{ kind: "interactive", cwd: "/work", startedAt: 1, sessionId, pid: 42 }];
      const file = path.join(directory, `${sessionId}.jsonl`);
      yield* fs.writeFileString(
        file,
        (yield* fs.readFileString(file)) +
          JSON.stringify({
            type: "user",
            uuid: "foreground-user",
            parentUuid: "assistant",
            sessionId,
            cwd: "/work",
            message: { content: "continue from terminal" },
          }) +
          "\n",
      );
      expect((yield* Effect.promise(() => source.available()))[0]?.sharingState).toBe("foreground");
      const foreground = yield* Effect.promise(() => source.list());
      expect(foreground).toEqual([]);
      expect((yield* Effect.promise(() => source.history(sessionId))).at(-1)).toMatchObject({
        status: "running",
        completedAt: null,
      });
      inventory = [
        {
          kind: "background",
          cwd: "/work",
          startedAt: 1,
          sessionId,
          id: "11111111",
          state: "stopped",
        },
      ];
      expect(yield* Effect.promise(() => source.list())).toHaveLength(1);
      inventory = [];
      expect((yield* Effect.promise(() => source.available()))[0]).toMatchObject({
        id: sessionId,
        sharingState: "history",
      });
      expect(yield* Effect.promise(() => source.list())).toEqual([]);
      expect((yield* Effect.promise(() => source.history(sessionId))).at(-1)).toMatchObject({
        status: "interrupted",
        completedAt: null,
      });
    }).pipe(Effect.scoped, Effect.provide(NodeServices.layer)),
);

it("follows an assistant leaf flushed before its user and attachment ancestors", () => {
  const history = nativeClaudeHistory([
    {
      type: "assistant",
      uuid: "answer",
      parentUuid: "attachment",
      message: {
        content: [
          { type: "tool_use", id: "question", name: "AskUserQuestion", input: { questions: [] } },
        ],
        stop_reason: "tool_use",
      },
    },
    { type: "user", uuid: "prompt", parentUuid: null, message: { content: "hello" } },
    { type: "attachment", uuid: "attachment", parentUuid: "prompt" },
  ]);
  expect(history).toHaveLength(1);
  expect(history[0]?.id).toBe("prompt");
  expect(history[0]?.items.map((item) => item.type)).toEqual(["user", "tool"]);
});

it("ends the owning turn on a native interruption record without inventing another prompt", () => {
  const prompt = {
    type: "user",
    uuid: "prompt",
    parentUuid: null,
    promptId: "native-prompt",
    message: { content: "run" },
    timestamp: 10,
  };
  const assistant = {
    type: "assistant",
    uuid: "assistant",
    parentUuid: "prompt",
    message: {
      id: "native-message",
      content: [{ type: "tool_use", id: "tool", name: "Bash", input: { command: "pwd" } }],
      stop_reason: "tool_use",
    },
  };
  const marker = {
    type: "user",
    uuid: "cancel",
    parentUuid: "assistant",
    interruptedMessageId: "native-message",
    promptId: "native-prompt",
    timestamp: 20,
    message: { content: [{ type: "text", text: "[Request interrupted by user for tool use]" }] },
  };
  const history = nativeClaudeHistory([prompt, assistant, marker]);
  expect(history).toHaveLength(1);
  expect(history[0]).toMatchObject({ id: "prompt", status: "interrupted", completedAt: 20 });
  expect(history[0]?.items.at(-1)).toMatchObject({ id: "tool", status: "cancelled" });
  const nativeEscape = nativeClaudeHistory([
    prompt,
    assistant,
    { ...marker, interruptedMessageId: undefined },
  ]);
  expect(nativeEscape).toHaveLength(1);
  expect(nativeEscape[0]).toMatchObject({ status: "interrupted", completedAt: 20 });
  const literal = nativeClaudeHistory([
    prompt,
    assistant,
    {
      ...marker,
      interruptedMessageId: undefined,
      promptId: "a-new-human-prompt",
      promptSource: "typed",
      origin: { kind: "human" },
    },
  ]);
  expect(literal).toHaveLength(2);
  expect(literal[1]?.items[0]?.text).toBe("[Request interrupted by user for tool use]");
});

it.effect("discovers a lazy background handoff only with a matching local job record", () =>
  Effect.gen(function* () {
    const fs = yield* FileSystem.FileSystem;
    const path = yield* Path.Path;
    const root = yield* fs.makeTempDirectoryScoped();
    const id = "12345678-2222-4333-8444-555555555555";
    class Client extends NativeClaudeClient {
      override async listSessions(): Promise<readonly NativeClaudeSession[]> {
        return [
          {
            kind: "background",
            id: "12345678",
            sessionId: id,
            cwd: "/work",
            startedAt: 1,
            status: "idle",
          },
        ];
      }
    }
    const source = new NativeClaudeSessions(root, new Client());
    expect(yield* Effect.promise(() => source.available())).toEqual([]);
    const job = path.join(root, "jobs", "12345678");
    yield* fs.makeDirectory(job, { recursive: true });
    const state = { sessionId: id, daemonShort: "12345678", cwd: "/wrong" };
    yield* fs.writeFileString(path.join(job, "state.json"), JSON.stringify(state));
    expect(yield* Effect.promise(() => source.list())).toEqual([]);
    yield* fs.writeFileString(
      path.join(job, "state.json"),
      JSON.stringify({ ...state, cwd: "/work", intent: "Handed off" }),
    );
    expect(yield* Effect.promise(() => source.list())).toMatchObject([
      { id, sharingState: "shared", cwd: "/work", active: false, historyAvailable: false },
    ]);
    expect(yield* Effect.promise(() => source.history(id))).toEqual([]);
  }).pipe(Effect.provide(NodeServices.layer), Effect.scoped),
);

it.effect("confirms only persisted native model message IDs with a bounded recent set", () =>
  Effect.gen(function* () {
    const fs = yield* FileSystem.FileSystem;
    const path = yield* Path.Path;
    const root = yield* fs.makeTempDirectoryScoped();
    const sessionId = "11111111-1111-4111-8111-111111111111";
    const project = path.join(root, "projects", "test");
    yield* fs.makeDirectory(project, { recursive: true });
    const file = path.join(project, `${sessionId}.jsonl`);
    const entries = Array.from({ length: 260 }, (_, index) => ({
      type: "assistant",
      sessionId,
      uuid: `entry-${index}`,
      message: { id: `model-${index}` },
    }));
    entries.push({ type: "assistant", sessionId, uuid: "duplicate", message: { id: "model-259" } });
    entries.push({ type: "user", sessionId, uuid: "not-model", message: { id: "user-id" } });
    entries.push({
      type: "assistant",
      sessionId: "other-session",
      uuid: "wrong-session",
      message: { id: "wrong-model" },
    });
    yield* fs.writeFileString(
      file,
      entries.map((entry) => JSON.stringify(entry)).join("\n") + "\n",
    );
    const source = new NativeClaudeSessions(root, new NativeClaudeClient());
    const ids = yield* Effect.promise(() => source.confirmedModelMessageIds(sessionId));
    expect(ids).toHaveLength(256);
    expect(ids[0]).toBe("model-259");
    expect(ids.at(-1)).toBe("model-4");
    yield* fs.writeFileString(
      file,
      JSON.stringify({ type: "assistant", sessionId, message: { id: "saved-later" } }) + "\n",
    );
    expect(yield* Effect.promise(() => source.confirmedModelMessageIds(sessionId))).toEqual([
      "saved-later",
    ]);
  }).pipe(Effect.scoped, Effect.provide(NodeServices.layer)),
);
