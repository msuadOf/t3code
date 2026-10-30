// @effect-diagnostics nodeBuiltinImport:off -- Isolated native session spool fixtures.
import * as NodeFSP from "node:fs/promises";
import * as NodePath from "node:path";
import * as NodeOS from "node:os";
import { expect, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as IdAllocator from "@t3tools/provider-core/server/IdAllocator";
import type * as ProviderAdapter from "@t3tools/provider-core/server/ProviderAdapter";
import { ProviderTurnId } from "@t3tools/contracts";
import { makeNativeClaudeSessionStreams } from "./NativeClaudeSessionStreams.ts";
const sessionId = "11111111-2222-4333-8444-555555555555";
it.effect(
  "captures only live turns, emits incremental distinct sources, and never replays ended captures",
  () =>
    Effect.gen(function* () {
      const fixture = yield* Effect.acquireRelease(
        Effect.tryPromise(async () => {
          const base = await NodeFSP.realpath(
            await NodeFSP.mkdtemp(NodePath.join(NodeOS.tmpdir(), "session-stream-")),
          );
          const root = NodePath.join(base, `claude-${process.getuid!()}`);
          await NodeFSP.mkdir(root, { mode: 0o700 });
          const dir = NodePath.join(root, "project", sessionId, "tasks");
          await NodeFSP.mkdir(dir, { recursive: true });
          return { base, dir };
        }),
        ({ base }) => Effect.promise(() => NodeFSP.rm(base, { recursive: true, force: true })),
      );
      const { base, dir } = fixture;
      const events: ProviderAdapter.ProviderAdapterV2Event[] = [];
      // The sampler consumes only routing identity; the provider owns all other turn fields.
      const owner = {
        threadId: "thread",
        runId: "run-one",
        rootNodeId: "root",
        providerThread: { id: "provider", driver: "claudeAgent", providerInstanceId: "instance" },
      } as ProviderAdapter.ProviderAdapterV2TurnInput;
      yield* Effect.tryPromise(async () => {
        await NodeFSP.writeFile(NodePath.join(dir, "one.output"), "first", { mode: 0o600 });
        await NodeFSP.writeFile(NodePath.join(dir, "two.output"), "second", { mode: 0o600 });
      });
      const ids = yield* IdAllocator.IdAllocatorV2;
      const streams = makeNativeClaudeSessionStreams({
        ids,
        temporaryDirectory: base,
        itemOrdinal: () => 0,
        emit: (e) =>
          Effect.sync(() => {
            events.push(e);
          }),
      });
      const turn = ProviderTurnId.make("turn-one");
      yield* streams.sample(sessionId, owner, turn, "old-finished", 0, true);
      expect(events).toHaveLength(0);
      yield* streams.sample(sessionId, owner, turn, "one", 0, false);
      expect(events.filter((e) => e.type === "command_output.delta")).toHaveLength(2);
      const count = events.length;
      yield* streams.sample(sessionId, owner, turn, "one", 0, false);
      expect(events).toHaveLength(count);
      yield* streams.sample(sessionId, owner, turn, "one", 0, true);
      const ended = events.length;
      yield* streams.sample(sessionId, owner, turn, "one", 0, true);
      yield* streams.sample(sessionId, owner, turn, "one", 0, false);
      expect(events).toHaveLength(ended);
      yield* streams.sample(
        sessionId,
        { ...owner, runId: "run-two" as typeof owner.runId },
        ProviderTurnId.make("turn-two"),
        "two",
        0,
        false,
      );
      const items = events.filter((e) => e.type === "turn_item.updated");
      expect(items.at(-1)).toMatchObject({
        turnItem: {
          runId: "run-two",
          parentItemId: null,
          title: "Session output (tool not identified)",
        },
      });
      expect(
        new Set(events.filter((e) => e.type === "command_output.delta").map((e) => e.itemId)).size,
      ).toBe(4);
    }).pipe(Effect.scoped, Effect.provide(IdAllocator.layer)),
);
