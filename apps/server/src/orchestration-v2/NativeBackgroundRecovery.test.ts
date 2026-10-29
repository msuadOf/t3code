import { assert, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as Deferred from "effect/Deferred";
import * as Queue from "effect/Queue";
import * as Stream from "effect/Stream";
import * as DateTime from "effect/DateTime";
import * as IdAllocator from "@t3tools/provider-core/server/IdAllocator";
import type * as ProviderAdapter from "@t3tools/provider-core/server/ProviderAdapter";
import type { OrchestrationV2ThreadProjection } from "@t3tools/contracts";
import { makeNativeBackgroundRecovery } from "./NativeBackgroundRecovery.ts";

it.effect(
  "recovers only exact background children of an already completed native run without reopening it",
  () =>
    Effect.gen(function* () {
      const ids = yield* IdAllocator.IdAllocatorV2;
      const queue = yield* Queue.unbounded<ProviderAdapter.ProviderAdapterV2Event>();
      const done = yield* Deferred.make<void>();
      let subscriptions = 0,
        closes = 0;
      const closed = yield* Queue.unbounded<void>();
      const writes: ProviderAdapter.ProviderAdapterV2Event[] = [];
      const session = {
        driver: "claudeAgent",
        instanceId: "claude",
        providerSessionId: "runtime",
        providerSession: { cwd: "/work" },
        subscribeEvents: Effect.sync(() => {
          subscriptions++;
          return {
            events: Stream.fromQueue(queue),
            close: Effect.gen(function* () {
              closes++;
              yield* Queue.offer(closed, undefined);
            }),
          };
        }),
      } as unknown as ProviderAdapter.ProviderAdapterV2SessionRuntime;
      const now = DateTime.makeUnsafe(1);
      let projection = {
        thread: { id: "thread", runtimeMode: "full-access", interactionMode: "default" },
        providerThreads: [
          {
            id: "provider",
            appThreadId: "thread",
            driver: "claudeAgent",
            providerInstanceId: "claude",
            nativeThreadRef: { nativeId: "native" },
          },
        ],
        providerTurns: [
          {
            id: "provider-turn",
            providerThreadId: "provider",
            nativeTurnRef: { nativeId: "native-turn" },
            runAttemptId: "attempt",
            ordinal: 1,
            nodeId: "node",
          },
        ],
        attempts: [
          { id: "attempt", runId: "run", providerThreadId: "provider", rootNodeId: "node" },
        ],
        runs: [
          {
            id: "run",
            threadId: "thread",
            providerInstanceId: "claude",
            providerThreadId: "provider",
            rootNodeId: "node",
            ordinal: 1,
            userMessageId: "user",
            modelSelection: { instanceId: "claude", model: "test" },
            status: "completed",
          },
        ],
        messages: [{ id: "user", text: "original prompt", attachments: [] }],
        turnItems: [],
        checkpointScopes: [],
      } as unknown as OrchestrationV2ThreadProjection;
      const childId = ids.derive.turnItemFromProviderItem({
        driver: session.driver,
        nativeItemId: "background-command:native:tool:task",
      });
      const recover = yield* makeNativeBackgroundRecovery({
        ids,
        store: { getThreadProjection: () => Effect.succeed(projection) },
        ingestor: {
          ingestNormalized: ({ event }) =>
            Effect.gen(function* () {
              writes.push(event);
              if (event.type === "turn_item.updated" && event.turnItem.status === "completed")
                yield* Deferred.succeed(done, undefined);
              return [];
            }),
        },
        isObserved: () => false,
      });
      const input = {
        session,
        threadId: "thread",
        nativeThreadId: "native",
        nativeTurnId: "native-turn",
        tasks: [{ nativeToolId: "tool", nativeTaskId: "task", status: "running" }],
      } as unknown as Parameters<typeof recover>[0];
      const originalProjection = projection;
      projection = {
        ...projection,
        providerTurns: projection.providerTurns.map((turn) => ({ ...turn, nodeId: "wrong-root" })),
      } as unknown as OrchestrationV2ThreadProjection;
      assert.equal(yield* recover(input), undefined);
      projection = originalProjection;
      assert.equal(yield* recover({ ...input, nativeThreadId: "other" }), undefined);
      assert.equal(yield* recover({ ...input, nativeTurnId: "other" }), undefined);
      assert.equal(
        yield* recover({
          ...input,
          session: { ...session, instanceId: "other" as typeof session.instanceId },
        }),
        undefined,
      );
      assert.equal(
        yield* recover({
          ...input,
          session: { ...session, subscribeEvents: undefined } as unknown as typeof session,
        }),
        undefined,
      );
      projection = {
        ...originalProjection,
        runs: originalProjection.runs.map((run) => ({ ...run, status: "rolled_back" as const })),
      };
      assert.equal(yield* recover(input), undefined);
      projection = originalProjection;
      const owner = yield* recover(input);
      assert.equal(owner?.turn.runId, "run");
      assert.equal(owner?.providerTurnId, "provider-turn");
      yield* recover(input);
      assert.equal(subscriptions, 1);
      const item = {
        id: childId,
        type: "command_execution",
        threadId: "thread",
        runId: "run",
        nodeId: "node",
        providerThreadId: "provider",
        providerTurnId: "provider-turn",
        parentItemId: ids.derive.turnItemFromProviderItem({
          driver: session.driver,
          nativeItemId: "tool",
        }),
        status: "running",
        input: "echo hi",
        updatedAt: now,
      };
      for (const patch of [
        { threadId: "other" },
        { runId: "other" },
        { nodeId: "other" },
        { providerThreadId: "other" },
        { providerTurnId: "other" },
        { id: "other-task" },
      ])
        yield* Queue.offer(queue, {
          type: "turn_item.updated",
          driver: session.driver,
          turnItem: { ...item, ...patch },
        } as ProviderAdapter.ProviderAdapterV2Event);
      yield* Queue.offer(queue, {
        type: "turn_item.updated",
        driver: session.driver,
        turnItem: item,
      } as ProviderAdapter.ProviderAdapterV2Event);
      yield* Queue.offer(queue, {
        type: "turn_item.updated",
        driver: session.driver,
        turnItem: { ...item, status: "completed", output: "done" },
      } as ProviderAdapter.ProviderAdapterV2Event);
      yield* Deferred.await(done);
      yield* Queue.take(closed);
      assert.equal(closes, 1);
      assert.equal(writes.length, 2);
      assert.equal(projection.runs[0]?.status, "completed");
      assert.equal(
        yield* recover({
          ...input,
          tasks: [{ nativeToolId: "other", nativeTaskId: "old", status: "completed" }],
        }),
        undefined,
      );
      assert.equal(subscriptions, 1);
      projection = {
        ...projection,
        turnItems: [item],
      } as unknown as OrchestrationV2ThreadProjection;
      const nativeCompleted = {
        ...input,
        tasks: [{ nativeToolId: "tool", nativeTaskId: "task", status: "completed" as const }],
      };
      assert.equal((yield* recover(nativeCompleted))?.turn.runId, "run");
      assert.equal(subscriptions, 2);
      yield* Queue.offer(queue, {
        type: "turn_item.updated",
        driver: session.driver,
        turnItem: { ...item, status: "completed", output: "recovered final" },
      } as ProviderAdapter.ProviderAdapterV2Event);
      yield* Queue.take(closed);
      assert.equal(writes.length, 3);
      projection = {
        ...projection,
        turnItems: [{ ...item, status: "completed" }],
      } as unknown as OrchestrationV2ThreadProjection;
      assert.equal(yield* recover(nativeCompleted), undefined);
      assert.equal(yield* recover(input), undefined);
      projection = { ...projection, turnItems: [] };
      const normalObserverRecovery = yield* makeNativeBackgroundRecovery({
        ids,
        store: { getThreadProjection: () => Effect.succeed(projection) },
        ingestor: {
          ingestNormalized: () => Effect.die("normal observer already owns persistence"),
        },
        isObserved: () => true,
      });
      assert.equal((yield* normalObserverRecovery(input))?.turn.runId, "run");
      assert.equal(subscriptions, 2);
      assert.equal(projection.runs[0]?.status, "completed");
    }).pipe(Effect.scoped, Effect.provide(IdAllocator.layer)),
);
