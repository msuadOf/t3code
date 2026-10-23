import { Agent, Model, Provider, SessionMessage } from "@opencode/client/effect";
import { assert, it } from "@effect/vitest";
import * as DateTime from "effect/DateTime";
import { mapNativeHistory } from "./nativeSessions.ts";

const at = (value: number) => DateTime.makeUnsafe(value);
const user = (id: string, text: string, created: number): SessionMessage.User => ({
  type: "user",
  id: SessionMessage.ID.make(id),
  text,
  time: { created: at(created) },
});
const assistant = (
  content: SessionMessage.Assistant["content"],
  completed?: number,
): SessionMessage.Assistant => ({
  type: "assistant",
  id: SessionMessage.ID.make("msg_reply"),
  agent: Agent.ID.make("build"),
  model: { providerID: Provider.ID.make("opencode"), id: Model.ID.make("model") },
  content,
  time: { created: at(20), ...(completed === undefined ? {} : { completed: at(completed) }) },
});

it("preserves native user identity, reasoning, tool output and terminal execution outcome", () => {
  const result = mapNativeHistory(
    [
      user("msg_user", "Explain", 10),
      assistant(
        [
          { type: "reasoning", text: "Think" },
          {
            type: "tool",
            id: "tool_native",
            name: "read",
            state: {
              status: "completed",
              input: { path: "a" },
              content: [{ type: "text", text: "File contents" }],
            },
            time: { created: at(21), completed: at(22) },
          },
          { type: "text", text: "Answer" },
        ],
        30,
      ),
      {
        type: "idle",
        id: SessionMessage.ID.make("msg_idle"),
        outcome: "interrupted",
        time: { created: at(40) },
      },
    ],
    false,
  );
  assert.equal(result[0]?.id, "msg_user");
  assert.equal(result[0]?.status, "interrupted");
  assert.deepEqual(
    result[0]?.items.map((item) => item.id),
    ["msg_user", "msg_reply:reasoning:0", "tool_native", "msg_reply:text:0"],
  );
  assert.equal(result[0]?.completedAt, 40);
  assert.deepEqual(
    result[0]?.items.map(({ type, text }) => ({ type, text })),
    [
      { type: "user", text: "Explain" },
      { type: "reasoning", text: "Think" },
      { type: "tool", text: "File contents" },
      { type: "assistant", text: "Answer" },
    ],
  );
});

it("marks only the latest execution running when attaching mid-stream", () => {
  const result = mapNativeHistory(
    [
      user("msg_first", "First", 10),
      {
        type: "idle",
        id: SessionMessage.ID.make("msg_idle"),
        outcome: "succeeded",
        time: { created: at(15) },
      },
      user("msg_second", "Second", 20),
      assistant([{ type: "reasoning", text: "Working" }]),
    ],
    true,
  );
  assert.equal(result[0]?.status, "completed");
  assert.equal(result[1]?.id, "msg_second");
  assert.equal(result[1]?.status, "running");
  assert.equal(result[1]?.completedAt, null);
  assert.equal(result[1]?.items.at(-1)?.status, "running");
});
