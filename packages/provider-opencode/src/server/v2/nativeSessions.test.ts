import { Agent, Model, Provider, SessionMessage } from "@opencode/client/effect";
import { assert, it } from "@effect/vitest";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Stream from "effect/Stream";
import { ProviderInstanceId } from "@t3tools/contracts";
import type { OpenCode2StreamEvent } from "./OpenCode2Client.ts";
import type { OpenCode2Connection, OpenCode2Server } from "./OpenCode2Server.ts";
import { makeNativeSessions, mapNativeHistory } from "./nativeSessions.ts";

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

it.effect.each(["observerEvents", "events"] as const)(
  "wakes via %s on lifecycle events, ignores streamed text, and cancels the subscription",
  (eventSource) =>
    Effect.gen(function* () {
      let closed = false;
      const events = Stream.fromIterable([
        { type: "t3.observer.text", data: {} },
        { type: "session.created", data: {} },
        { type: "session.execution.started", data: {} },
        { type: "session.execution.succeeded", data: {} },
        { type: "session.deleted", data: {} },
        { type: "t3.observer.gap", data: {} },
        { type: "t3.observer.ready", data: {} },
      ] as unknown as OpenCode2StreamEvent[]).pipe(
        Stream.concat(Stream.never),
        Stream.ensuring(
          Effect.sync(() => {
            closed = true;
          }),
        ),
      );
      const server = {
        withConnection: <A, E, R>(
          use: (connection: OpenCode2Connection) => Effect.Effect<A, E, R>,
        ) => use({ [eventSource]: Effect.succeed(events) } as unknown as OpenCode2Connection),
      } satisfies OpenCode2Server["Service"];
      const changes = makeNativeSessions(server, ProviderInstanceId.make("test")).changes!;
      const result = yield* changes.pipe(Stream.take(6), Stream.runCollect);
      assert.equal(result.length, 6);
      assert.isTrue(closed);
    }),
);
