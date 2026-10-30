import { expect, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as IdAllocator from "@t3tools/provider-core/server/IdAllocator";
import type * as ProviderAdapter from "@t3tools/provider-core/server/ProviderAdapter";
import { ProviderTurnId } from "@t3tools/contracts";
import { makeNativeClaudeTerminalView } from "./NativeClaudeTerminalView.ts";

it.effect("publishes replacement screens only for live captures and ends them once", () =>
  Effect.gen(function* () {
    const events: ProviderAdapter.ProviderAdapterV2Event[] = [];
    const owner = {
      threadId: "thread",
      runId: "run",
      rootNodeId: "root",
      providerThread: { id: "provider", driver: "claudeAgent", providerInstanceId: "instance" },
    } as ProviderAdapter.ProviderAdapterV2TurnInput;
    yield* Effect.gen(function* () {
      const ids = yield* IdAllocator.IdAllocatorV2;
      const view = makeNativeClaudeTerminalView({
        ids,
        itemOrdinal: () => 0,
        emit: (e) =>
          Effect.sync(() => {
            events.push(e);
          }),
      });
      const turn = ProviderTurnId.make("turn");
      view.accept("session", { type: "stream", line: "progress 10%" });
      yield* view.sample("session", owner, turn, "old", 0, true);
      expect(events).toHaveLength(0);
      yield* view.sample("session", owner, turn, "live", 0, false);
      expect(events.at(-1)).toMatchObject({
        type: "command_output.delta",
        replace: true,
        chunk: "progress 10%",
      });
      const count = events.length;
      yield* view.sample("session", owner, turn, "live", 0, false);
      expect(events).toHaveLength(count);
      view.accept("session", { type: "stream", line: "\r\u001b[2Kprogress 90%" });
      yield* view.sample("session", owner, turn, "live", 0, false);
      expect(events.at(-1)).toMatchObject({ replace: true, chunk: "progress 90%" });
      view.accept("session", { type: "snapshot", record: { sessionId: "session" } });
      yield* view.sample("session", owner, turn, "live", 0, true);
      const ended = events.length;
      view.accept("session", { type: "stream", line: "later turn" });
      yield* view.sample("session", owner, turn, "live", 0, true);
      expect(events).toHaveLength(ended);
      view.drop("session");
    }).pipe(Effect.provide(IdAllocator.layer));
  }),
);
