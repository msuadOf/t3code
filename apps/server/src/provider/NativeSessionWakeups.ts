import type * as ProviderInstanceRegistry from "./ProviderInstanceRegistry.ts";
import * as Effect from "effect/Effect";
import * as Schedule from "effect/Schedule";
import * as Stream from "effect/Stream";

export const nativeSessionSyncTriggers = (
  registry: Pick<
    ProviderInstanceRegistry.ProviderInstanceRegistry["Service"],
    "listInstances" | "subscribeChanges"
  >,
) =>
  Stream.unwrap(
    Effect.gen(function* () {
      const changes = yield* registry.subscribeChanges;
      return Stream.concat(Stream.make(undefined), Stream.fromSubscription(changes)).pipe(
        Stream.mapEffect(() => registry.listInstances),
        Stream.switchMap((instances) => {
          const watchers = instances.flatMap((instance) => {
            if (
              !instance.enabled ||
              instance.driverKind === "codex" ||
              !instance.nativeSessions?.changes
            )
              return [];
            return [
              instance.nativeSessions.changes.pipe(
                Stream.catch(() => Stream.empty),
                // Native services can restart; periodic inventory scans remain the fallback.
                Stream.repeat(Schedule.spaced("1 second")),
              ),
            ];
          });
          return Stream.mergeAll([Stream.make(undefined), Stream.tick("3 seconds"), ...watchers], {
            concurrency: "unbounded",
          });
        }),
      );
    }),
  ).pipe(
    // These are refresh requests, so one pending rescan covers any burst while a scan runs.
    Stream.buffer({ capacity: 1, strategy: "sliding" }),
  );
