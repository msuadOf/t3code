import { it, expect } from "@effect/vitest";
import { ProviderDriverKind, ProviderInstanceId } from "@t3tools/contracts";
import { ProviderDriverError } from "@t3tools/provider-core/server/errors";
import type { ProviderInstance } from "@t3tools/provider-core/server/driver";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Fiber from "effect/Fiber";
import * as Queue from "effect/Queue";
import * as TestClock from "effect/testing/TestClock";
import * as PubSub from "effect/PubSub";
import * as Stream from "effect/Stream";
import { nativeSessionSyncTriggers } from "./NativeSessionWakeups.ts";

it.effect("reads native watcher inventory before emitting the initial sync", () =>
  Effect.scoped(
    Effect.gen(function* () {
      const changes = yield* PubSub.unbounded<void>();
      let inventories = 0;
      const first = yield* nativeSessionSyncTriggers({
        listInstances: Effect.sync(() => {
          inventories++;
          return [];
        }),
        subscribeChanges: PubSub.subscribe(changes),
      }).pipe(Stream.take(1), Stream.runCollect);
      expect(first).toHaveLength(1);
      expect(inventories).toBe(1);
    }),
  ),
);

it.effect("wakes immediately on native changes and cancels replaced subscriptions", () =>
  Effect.scoped(
    Effect.gen(function* () {
      const registryChanges = yield* PubSub.unbounded<void>();
      const nativeChanges = yield* Queue.unbounded<void>();
      const scans = yield* Queue.unbounded<void>();
      const subscribed = yield* Deferred.make<void>();
      const closed = yield* Deferred.make<void>();
      let instances: ReadonlyArray<ProviderInstance> = [
        {
          driverKind: ProviderDriverKind.make("opencode"),
          enabled: true,
          nativeSessions: {
            list: Effect.succeed([]),
            history: () => Effect.succeed([]),
            changes: Stream.unwrap(
              Deferred.succeed(subscribed, undefined).pipe(
                Effect.as(Stream.fromQueue(nativeChanges)),
              ),
            ).pipe(Stream.ensuring(Deferred.succeed(closed, undefined))),
          },
        } as unknown as ProviderInstance,
      ];
      const consumer = yield* nativeSessionSyncTriggers({
        listInstances: Effect.sync(() => instances),
        subscribeChanges: PubSub.subscribe(registryChanges),
      }).pipe(
        Stream.runForEach(() => Queue.offer(scans, undefined)),
        Effect.forkScoped,
      );
      yield* Queue.take(scans);
      yield* Deferred.await(subscribed);
      yield* Queue.offer(nativeChanges, undefined);
      yield* Queue.take(scans);
      yield* TestClock.adjust(2999);
      expect((yield* Queue.poll(scans))._tag).toBe("None");
      yield* TestClock.adjust(1);
      yield* Queue.take(scans);
      instances = [];
      yield* PubSub.publish(registryChanges, undefined);
      yield* Queue.take(scans);
      yield* Deferred.await(closed);
      yield* Queue.offer(nativeChanges, undefined);
      yield* TestClock.adjust(0);
      expect((yield* Queue.poll(scans))._tag).toBe("None");
      yield* Fiber.interrupt(consumer);
      yield* TestClock.adjust("1 minute");
      expect((yield* Queue.poll(scans))._tag).toBe("None");
    }),
  ),
);

it.effect("does not subscribe disabled providers", () =>
  Effect.scoped(
    Effect.gen(function* () {
      const registryChanges = yield* PubSub.unbounded<void>();
      const scans = yield* Queue.unbounded<void>();
      let subscribed = false;
      const instance = {
        driverKind: ProviderDriverKind.make("opencode"),
        enabled: false,
        nativeSessions: {
          list: Effect.succeed([]),
          history: () => Effect.succeed([]),
          changes: Stream.fromEffect(
            Effect.sync(() => {
              subscribed = true;
            }),
          ),
        },
      } as unknown as ProviderInstance;
      yield* nativeSessionSyncTriggers({
        listInstances: Effect.succeed([instance]),
        subscribeChanges: PubSub.subscribe(registryChanges),
      }).pipe(
        Stream.runForEach(() => Queue.offer(scans, undefined)),
        Effect.forkScoped,
      );
      yield* Queue.take(scans);
      yield* TestClock.adjust(0);
      expect(subscribed).toBe(false);
    }),
  ),
);

it.effect.each(["failure", "eof"] as const)("resubscribes after native watcher %s", (mode) =>
  Effect.scoped(
    Effect.gen(function* () {
      const registryChanges = yield* PubSub.unbounded<void>();
      const scans = yield* Queue.unbounded<void>();
      let attempts = 0;
      const watcher = Stream.suspend(() => {
        attempts++;
        if (attempts > 1) return Stream.concat(Stream.make(undefined), Stream.never);
        return mode === "eof"
          ? Stream.empty
          : Stream.fail(
              new ProviderDriverError({
                driver: ProviderDriverKind.make("opencode"),
                instanceId: ProviderInstanceId.make("test"),
                detail: "fixture disconnected",
              }),
            );
      });
      const instance = {
        driverKind: ProviderDriverKind.make("opencode"),
        enabled: true,
        nativeSessions: { changes: watcher },
      } as unknown as ProviderInstance;
      yield* nativeSessionSyncTriggers({
        listInstances: Effect.succeed([instance]),
        subscribeChanges: PubSub.subscribe(registryChanges),
      }).pipe(
        Stream.runForEach(() => Queue.offer(scans, undefined)),
        Effect.forkScoped,
      );
      yield* Queue.take(scans);
      yield* TestClock.adjust(0);
      expect(attempts).toBe(1);
      yield* TestClock.adjust(999);
      expect(attempts).toBe(1);
      yield* TestClock.adjust(1);
      yield* Queue.take(scans);
      expect(attempts).toBe(2);
    }),
  ),
);
