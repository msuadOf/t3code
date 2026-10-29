import { assert, it } from "@effect/vitest";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Fiber from "effect/Fiber";
import * as Queue from "effect/Queue";
import * as TestClock from "effect/testing/TestClock";
import { tailShellOutput } from "./shellOutput.ts";

it.effect(
  "uses native cursors, drains backlog, waits when caught up, and resumes without duplicates",
  () =>
    Effect.gen(function* () {
      const requests = yield* Queue.unbounded<number>();
      const output = yield* Queue.unbounded<{ chunk: string; truncated: boolean }>();
      const ready = yield* Deferred.make<void>();
      const state = { cursor: undefined as number | undefined };
      const read = (cursor: number) =>
        Effect.gen(function* () {
          yield* Queue.offer(requests, cursor);
          if (cursor === Number.MAX_SAFE_INTEGER)
            return { output: "", cursor: 70000, size: 70000, truncated: false };
          if (cursor === 4464) return { output: "α", cursor: 5000, size: 70000, truncated: false };
          if (cursor === 5000) return { output: "β", cursor: 70000, size: 70000, truncated: false };
          yield* Deferred.succeed(ready, undefined);
          return { output: "", cursor: 70000, size: 70000, truncated: false };
        });
      const fiber = yield* tailShellOutput({
        state,
        read,
        onChunk: (chunk, truncated) =>
          Queue.offer(output, { chunk, truncated }).pipe(Effect.asVoid),
      }).pipe(Effect.forkScoped);
      assert.deepEqual(
        [yield* Queue.take(requests), yield* Queue.take(requests), yield* Queue.take(requests)],
        [Number.MAX_SAFE_INTEGER, 4464, 5000],
      );
      assert.deepEqual(
        [yield* Queue.take(output), yield* Queue.take(output)],
        [
          { chunk: "α", truncated: true },
          { chunk: "β", truncated: false },
        ],
      );
      yield* TestClock.adjust("100 millis");
      yield* Deferred.await(ready);
      assert.equal(yield* Queue.take(requests), 70000);
      yield* Fiber.interrupt(fiber);
      assert.equal(state.cursor, 70000);
      const resumed = yield* tailShellOutput({
        state,
        read,
        onChunk: (chunk, truncated) =>
          Queue.offer(output, { chunk, truncated }).pipe(Effect.asVoid),
      }).pipe(Effect.forkScoped);
      assert.equal(yield* Queue.take(requests), 70000);
      yield* Fiber.interrupt(resumed);
      assert.equal(yield* Queue.size(output), 0);
    }).pipe(Effect.scoped),
);

it.effect("interrupts a blocked read or callback and treats a read error as a stopped tail", () =>
  Effect.gen(function* () {
    const entered = yield* Deferred.make<void>();
    const blockedRead = yield* tailShellOutput({
      state: { cursor: 0 },
      read: () => Deferred.succeed(entered, undefined).pipe(Effect.andThen(Effect.never)),
      onChunk: () => Effect.void,
    }).pipe(Effect.forkScoped);
    yield* Deferred.await(entered);
    yield* Fiber.interrupt(blockedRead);

    const callbackEntered = yield* Deferred.make<void>();
    const blockedCallback = yield* tailShellOutput({
      state: { cursor: 0 },
      read: () => Effect.succeed({ output: "x", cursor: 1, size: 1, truncated: false }),
      onChunk: () =>
        Deferred.succeed(callbackEntered, undefined).pipe(Effect.andThen(Effect.never)),
    }).pipe(Effect.forkScoped);
    yield* Deferred.await(callbackEntered);
    yield* Fiber.interrupt(blockedCallback);

    yield* tailShellOutput({
      state: { cursor: 0 },
      read: () => Effect.fail("not found"),
      onChunk: () => Effect.void,
    });
  }).pipe(Effect.scoped),
);
