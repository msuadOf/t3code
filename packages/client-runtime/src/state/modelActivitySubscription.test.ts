import { it, expect } from "@effect/vitest";
import { ModelActivityError, type ModelActivityFrame } from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Fiber from "effect/Fiber";
import * as Stream from "effect/Stream";
import * as Queue from "effect/Queue";
import * as TestClock from "effect/testing/TestClock";
import { emptyModelActivityState, reduceModelActivity } from "./modelActivity.ts";
import { recoverModelActivity } from "./modelActivitySubscription.ts";

const snapshot = (text: string): ModelActivityFrame => ({
  kind: "snapshot",
  available: true,
  epoch: "same-worker",
  seq: 1,
  oldestSeq: 1,
  after: 0,
  gap: false,
  confirmedMessageIds: [],
  events: [
    {
      seq: 1,
      sessionId: "native",
      requestId: "request",
      messageId: "message",
      type: "content_block_start",
      data: { index: 0, content_block: { type: "text", text } },
    },
  ],
});

it.effect("recovers a failed read with a fresh snapshot while retaining the partial reply", () =>
  Effect.scoped(
    Effect.gen(function* () {
      const frames = yield* Queue.unbounded<ModelActivityFrame, ModelActivityError>();
      let reads = 0;
      const source = Stream.suspend(() => {
        reads++;
        return reads === 1
          ? Stream.concat(
              Stream.make(snapshot("prefix")),
              Stream.fail(new ModelActivityError({ reason: "read-failed" })),
            )
          : Stream.concat(Stream.make(snapshot("prefix and recovered tail")), Stream.never);
      });
      yield* recoverModelActivity(source).pipe(
        Stream.runForEach((frame) => Queue.offer(frames, frame)),
        Effect.catch((error) => Queue.fail(frames, error)),
        Effect.forkScoped,
      );
      let state = reduceModelActivity(emptyModelActivityState, yield* Queue.take(frames));
      const partial = state.requests;
      state = reduceModelActivity(state, yield* Queue.take(frames));
      expect(state.connectionState).toBe("disconnected");
      expect(state.requests).toBe(partial);
      expect(reads).toBe(1);
      yield* TestClock.adjust(250);
      state = reduceModelActivity(state, yield* Queue.take(frames));
      expect(reads).toBe(2);
      expect(state.connectionState).toBe("connected");
      expect(state.requests[0]?.blocks[0]?.text).toBe("prefix and recovered tail");
      expect(state.requests).toHaveLength(1);
    }),
  ),
);

it.effect.each(["outside-root", "unavailable"] as const)(
  "does not retry a permanent %s response",
  (reason) =>
    Effect.gen(function* () {
      const error = new ModelActivityError({ reason });
      let reads = 0;
      const result = yield* recoverModelActivity(
        Stream.suspend(() => {
          reads++;
          return Stream.fail(error);
        }),
      ).pipe(Stream.runCollect, Effect.result);
      expect(result._tag).toBe("Failure");
      if (result._tag === "Failure") expect(result.failure).toBe(error);
      expect(reads).toBe(1);
    }),
);

it.effect("backs off repeated failed reads and resets the delay after a valid snapshot", () =>
  Effect.scoped(
    Effect.gen(function* () {
      const frames = yield* Queue.unbounded<ModelActivityFrame, ModelActivityError>();
      let reads = 0;
      const source = Stream.suspend(() => {
        reads++;
        const failure = Stream.fail(new ModelActivityError({ reason: "read-failed" }));
        return reads === 3 ? Stream.concat(Stream.make(snapshot("recovered")), failure) : failure;
      });
      yield* recoverModelActivity(source).pipe(
        Stream.runForEach((frame) => Queue.offer(frames, frame)),
        Effect.catch((error) => Queue.fail(frames, error)),
        Effect.forkScoped,
      );
      expect((yield* Queue.take(frames)).kind).toBe("disconnected");
      yield* TestClock.adjust(249);
      expect(reads).toBe(1);
      yield* TestClock.adjust(1);
      expect((yield* Queue.take(frames)).kind).toBe("disconnected");
      yield* TestClock.adjust(499);
      expect(reads).toBe(2);
      yield* TestClock.adjust(1);
      expect((yield* Queue.take(frames)).kind).toBe("snapshot");
      expect((yield* Queue.take(frames)).kind).toBe("disconnected");
      yield* TestClock.adjust(249);
      expect(reads).toBe(3);
      yield* TestClock.adjust(1);
      expect((yield* Queue.take(frames)).kind).toBe("disconnected");
      expect(reads).toBe(4);
    }),
  ),
);

it.effect("cancels a pending retry when the view unsubscribes", () =>
  Effect.scoped(
    Effect.gen(function* () {
      const frames = yield* Queue.unbounded<ModelActivityFrame>();
      let reads = 0;
      const consumer = yield* recoverModelActivity(
        Stream.suspend(() => {
          reads++;
          return Stream.fail(new ModelActivityError({ reason: "read-failed" }));
        }),
      ).pipe(
        Stream.runForEach((frame) => Queue.offer(frames, frame)),
        Effect.forkScoped,
      );
      expect((yield* Queue.take(frames)).kind).toBe("disconnected");
      yield* Fiber.interrupt(consumer);
      yield* TestClock.adjust("1 minute");
      expect(reads).toBe(1);
      expect((yield* Queue.poll(frames))._tag).toBe("None");
    }),
  ),
);

it.effect("caps repeated read-failure backoff at five seconds", () =>
  Effect.scoped(
    Effect.gen(function* () {
      const frames = yield* Queue.unbounded<ModelActivityFrame>();
      let reads = 0;
      yield* recoverModelActivity(
        Stream.suspend(() => {
          reads++;
          return Stream.fail(new ModelActivityError({ reason: "read-failed" }));
        }),
      ).pipe(
        Stream.runForEach((frame) => Queue.offer(frames, frame)),
        Effect.forkScoped,
      );
      expect((yield* Queue.take(frames)).kind).toBe("disconnected");
      for (const delay of [250, 500, 1000, 2000, 4000, 5000, 5000]) {
        const previousReads = reads;
        yield* TestClock.adjust(delay - 1);
        expect(reads).toBe(previousReads);
        yield* TestClock.adjust(1);
        expect((yield* Queue.take(frames)).kind).toBe("disconnected");
        expect(reads).toBe(previousReads + 1);
      }
    }),
  ),
);
