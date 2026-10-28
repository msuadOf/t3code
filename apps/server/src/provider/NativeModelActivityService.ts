import {
  ModelActivityError,
  type ModelActivityFrame,
  type ModelActivitySubscribeInput,
} from "@t3tools/contracts";
import { expandHomePath } from "@t3tools/provider-core/server/pathExpansion";
import * as HostProcess from "@t3tools/shared/HostProcess";
import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Schema from "effect/Schema";
import * as Path from "effect/Path";
import * as Stream from "effect/Stream";
import * as ServerConfig from "../config.ts";
import * as ProjectionStore from "../orchestration-v2/ProjectionStore.ts";
import * as ServerSettings from "../serverSettings.ts";
import * as ProviderInstanceRegistry from "./ProviderInstanceRegistry.ts";
import { discoverClaudeModelObserver, observeClaudeModelActivity } from "./ClaudeModelObserver.ts";
import {
  readClaudeModelObserverBinding,
  isClaudeModelObserverBindingCurrent,
} from "./ClaudeModelObserverBindings.ts";
import { isWithinSharedRoot } from "./NativeCodexSessionSync.ts";

export class NativeModelActivityService extends Context.Service<
  NativeModelActivityService,
  {
    readonly subscribe: (
      input: ModelActivitySubscribeInput,
    ) => Stream.Stream<ModelActivityFrame, ModelActivityError>;
  }
>()("t3/provider/NativeModelActivityService") {}
const unavailable: ModelActivityFrame = {
  kind: "snapshot",
  available: false,
  epoch: "",
  seq: 0,
  oldestSeq: 0,
  after: 0,
  gap: false,
  events: [],
  confirmedMessageIds: [],
};
const isModelActivityError = Schema.is(ModelActivityError);
const readError = () => new ModelActivityError({ reason: "read-failed" });
const make = Effect.gen(function* () {
  const registry = yield* ProviderInstanceRegistry.ProviderInstanceRegistry;
  const store = yield* ProjectionStore.ProjectionStoreV2;
  const settingsService = yield* ServerSettings.ServerSettingsService;
  const config = yield* ServerConfig.ServerConfig;
  const path = yield* Path.Path;
  const home = yield* HostProcess.HomeDirectory;
  const prepare = Effect.fn("NativeModelActivityService.prepare")(function* (
    input: ModelActivitySubscribeInput,
  ) {
    const thread = yield* store.getThread(input.threadId);
    const context = yield* store.getThreadProviderContext(
      input.threadId,
      thread.providerInstanceId,
    );
    const native = context.providerThreads.find(
      (value) => value.id === thread.activeProviderThreadId,
    );
    if (native?.driver !== "claudeAgent" || !native.nativeThreadRef) return;
    const instance = yield* registry.getInstance(native.providerInstanceId);
    const source = instance?.nativeSessions;
    if (!instance?.enabled || instance.driverKind !== "claudeAgent" || !source) return;
    const sessions = yield* source.list;
    const session = sessions.find((value) => value.id === native.nativeThreadRef?.nativeId);
    if (!session) return;
    const settings = yield* settingsService.getSettings;
    const raw = settings.providerInstances[instance.instanceId]?.config;
    const root =
      raw &&
      typeof raw === "object" &&
      "sharedSessionRoot" in raw &&
      typeof raw.sharedSessionRoot === "string"
        ? expandHomePath(raw.sharedSessionRoot.trim(), home)
        : undefined;
    if (!isWithinSharedRoot(path, session.cwd, root))
      return yield* new ModelActivityError({ reason: "outside-root" });
    const binding = yield* Effect.tryPromise({
      try: () =>
        readClaudeModelObserverBinding({
          stateDir: config.stateDir,
          instanceId: instance.instanceId,
          nativeSessionId: session.id,
        }),
      catch: readError,
    });
    if (
      !binding ||
      !source.identityDirectory ||
      path.resolve(binding.configDirectory) !== path.resolve(source.identityDirectory)
    )
      return;
    const current = yield* Effect.tryPromise({
      try: () =>
        isClaudeModelObserverBindingCurrent({
          stateDir: config.stateDir,
          instanceId: instance.instanceId,
          nativeSessionId: session.id,
          configDirectory: source.identityDirectory!,
        }),
      catch: readError,
    });
    if (!current) return;
    const observer = yield* Effect.tryPromise({
      try: () =>
        discoverClaudeModelObserver({
          stateDir: binding.observerStateDir,
          upstreamUrl: binding.upstreamUrl,
        }),
      catch: readError,
    });
    if (observer) return { source, session, observer };
  });
  return NativeModelActivityService.of({
    subscribe: (input) =>
      Stream.unwrap(
        Effect.gen(function* () {
          const selected = yield* prepare(input).pipe(
            Effect.mapError((error) => (isModelActivityError(error) ? error : readError())),
          );
          if (!selected) return Stream.make(unavailable);
          const controller = new AbortController();
          yield* Effect.addFinalizer(() => Effect.sync(() => controller.abort()));
          let epoch = selected.observer.epoch;
          const pending = new Set<string>();
          const confirmed = new Set<string>();
          const observeId = (id: string) => {
            if (!confirmed.has(id)) pending.add(id);
            while (pending.size > 256) pending.delete(pending.values().next().value!);
          };
          const readConfirmed = selected.source.confirmedModelMessageIds;
          const confirmation = Effect.suspend(() =>
            readConfirmed && pending.size > 0
              ? readConfirmed(selected.session.id).pipe(
                  Effect.map((ids): ModelActivityFrame => {
                    const added = ids.filter((id) => pending.has(id) && !confirmed.has(id));
                    for (const id of added) {
                      confirmed.add(id);
                      while (confirmed.size > 256)
                        confirmed.delete(confirmed.values().next().value!);
                      pending.delete(id);
                    }
                    return { kind: "confirmed", epoch, messageIds: added };
                  }),
                  Effect.mapError(readError),
                )
              : Effect.succeed<ModelActivityFrame>({ kind: "confirmed", epoch, messageIds: [] }),
          );
          const responses = Stream.fromAsyncIterable(
            observeClaudeModelActivity({
              ...selected.observer,
              sessionId: selected.session.id,
              ...(input.after === undefined ? {} : { after: input.after }),
              signal: controller.signal,
            }),
            readError,
          ).pipe(
            Stream.mapEffect((frame): Effect.Effect<ModelActivityFrame, ModelActivityError> => {
              if (frame.type === "observer.snapshot" && "events" in frame) {
                epoch = frame.epoch;
                for (const event of frame.events) if (event.messageId) observeId(event.messageId);
                return (
                  readConfirmed
                    ? readConfirmed(selected.session.id)
                    : Effect.succeed<ReadonlyArray<string>>([])
                ).pipe(
                  Effect.map((ids): ModelActivityFrame => {
                    for (const id of ids) {
                      confirmed.add(id);
                      while (confirmed.size > 256)
                        confirmed.delete(confirmed.values().next().value!);
                      pending.delete(id);
                    }
                    return {
                      kind: "snapshot",
                      available: true,
                      epoch,
                      seq: frame.seq,
                      oldestSeq: frame.oldestSeq,
                      after: input.after ?? 0,
                      gap: frame.gap,
                      events: frame.events,
                      confirmedMessageIds: ids.filter((id) =>
                        frame.events.some((event) => event.messageId === id),
                      ),
                    };
                  }),
                  Effect.mapError(readError),
                );
              }
              if (!("requestId" in frame)) return Effect.fail(readError());
              if (frame.messageId) observeId(frame.messageId);
              return Effect.succeed({ kind: "event", epoch, event: frame });
            }),
          );
          // Native JSONL may commit after message_stop; unfinished confirmation needs an independent read.
          const confirmations = Stream.tick("1 second").pipe(
            Stream.mapEffect(() => confirmation),
            Stream.filter((frame) => frame.kind === "confirmed" && frame.messageIds.length > 0),
          );
          return Stream.merge(responses, confirmations, { haltStrategy: "left" });
        }),
      ),
  });
});
export const layer = Layer.effect(NativeModelActivityService, make);
