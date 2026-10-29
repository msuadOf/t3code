import {
  ModelActivityError,
  type ModelActivityFrame,
  type ModelActivitySubscribeInput,
} from "@t3tools/contracts";
import type {
  NativeSession,
  NativeSessionSource,
} from "@t3tools/provider-core/server/nativeSessions";
import * as EventSink from "../orchestration-v2/EventSink.ts";
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
  const events = yield* EventSink.EventSinkV2;
  const registry = yield* ProviderInstanceRegistry.ProviderInstanceRegistry;
  const store = yield* ProjectionStore.ProjectionStoreV2;
  const settingsService = yield* ServerSettings.ServerSettingsService;
  const config = yield* ServerConfig.ServerConfig;
  const path = yield* Path.Path;
  const home = yield* HostProcess.HomeDirectory;
  const prepare = Effect.fn("NativeModelActivityService.prepare")(function* (
    input: ModelActivitySubscribeInput,
    cache: {
      current:
        | { readonly source: NativeSessionSource; readonly session: NativeSession }
        | undefined;
    },
  ) {
    const thread = yield* store.getThread(input.threadId);
    const context = yield* store.getThreadProviderContext(
      input.threadId,
      thread.providerInstanceId,
    );
    const native = context.providerThreads.find(
      (value) => value.id === thread.activeProviderThreadId,
    );
    if (native?.driver !== "claudeAgent" || !native.nativeThreadRef) {
      cache.current = undefined;
      return;
    }
    const instance = yield* registry.getInstance(native.providerInstanceId);
    const source = instance?.nativeSessions;
    if (!instance?.enabled || instance.driverKind !== "claudeAgent" || !source) {
      cache.current = undefined;
      return;
    }
    const session =
      cache.current?.source === source &&
      cache.current.session.id === native.nativeThreadRef.nativeId
        ? cache.current.session
        : (yield* source.list).find((value) => value.id === native.nativeThreadRef?.nativeId);
    if (!session) return;
    cache.current = { source, session };
    const settings = yield* settingsService.getSettings;
    const configured = settings.providerInstances[instance.instanceId];
    const raw = configured?.config;
    if (
      !configured ||
      configured.enabled === false ||
      (raw && typeof raw === "object" && "enabled" in raw && raw.enabled === false)
    )
      return;
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
    return { source, session, observer };
  });
  type Selected = NonNullable<Effect.Success<ReturnType<typeof prepare>>>;
  type Connected = Selected & { readonly observer: NonNullable<Selected["observer"]> };
  const observeSelected = (selected: Connected, after?: number, recovering = false) =>
    Stream.unwrap(
      Effect.gen(function* () {
        const controller = new AbortController();
        yield* Effect.addFinalizer(() => Effect.sync(() => controller.abort()));
        let epoch = `${selected.observer.epoch}:${selected.session.id}`;
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
                    while (confirmed.size > 256) confirmed.delete(confirmed.values().next().value!);
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
            ...(after === undefined ? {} : { after: after }),
            signal: controller.signal,
          }),
          readError,
        ).pipe(
          Stream.mapEffect((frame): Effect.Effect<ModelActivityFrame, ModelActivityError> => {
            if (frame.type === "observer.snapshot" && "events" in frame) {
              epoch = `${frame.epoch}:${selected.session.id}`;
              for (const event of frame.events) if (event.messageId) observeId(event.messageId);
              return (
                readConfirmed
                  ? readConfirmed(selected.session.id)
                  : Effect.succeed<ReadonlyArray<string>>([])
              ).pipe(
                Effect.map((ids): ModelActivityFrame => {
                  for (const id of ids) {
                    confirmed.add(id);
                    while (confirmed.size > 256) confirmed.delete(confirmed.values().next().value!);
                    pending.delete(id);
                  }
                  return {
                    kind: "snapshot",
                    available: true,
                    epoch,
                    seq: frame.seq,
                    oldestSeq: frame.oldestSeq,
                    after: after ?? 0,
                    gap: frame.gap || recovering,
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
    ).pipe(Stream.scoped);
  return NativeModelActivityService.of({
    subscribe: (input) =>
      Stream.unwrap(
        Effect.gen(function* () {
          const registryChanges = yield* registry.subscribeChanges;
          const settingsChanges = yield* settingsService.subscribeChanges;
          const cache: {
            current:
              | { readonly source: NativeSessionSource; readonly session: NativeSession }
              | undefined;
          } = { current: undefined };
          let connected = false;
          const followThread = Stream.unwrap(
            Effect.gen(function* () {
              // Capture before the initial read: EventSink replays commits that precede its live subscription.
              const afterSequence = yield* events
                .latestSequence({ threadId: input.threadId })
                .pipe(Effect.mapError(readError));
              const changes = events.stream({ threadId: input.threadId, afterSequence }).pipe(
                Stream.filter(
                  ({ event }) =>
                    event.type === "provider-thread.updated" ||
                    event.type === "provider-session.attached" ||
                    event.type === "provider-session.updated" ||
                    event.type === "provider-session.detached" ||
                    event.type === "thread.metadata-updated" ||
                    event.type === "thread.provider-switched" ||
                    event.type === "thread.model-selection-updated",
                ),
                Stream.map(() => undefined),
                Stream.mapError(readError),
              );
              return Stream.concat(Stream.make(undefined), changes).pipe(
                Stream.mapEffect(() =>
                  prepare(input, cache).pipe(
                    Effect.mapError((error) => (isModelActivityError(error) ? error : readError())),
                  ),
                ),
                Stream.changesWith(
                  (previous, next) =>
                    previous === next ||
                    (previous !== undefined &&
                      next !== undefined &&
                      previous.source === next.source &&
                      previous.session.id === next.session.id &&
                      previous.observer?.url === next.observer?.url &&
                      previous.observer?.epoch === next.observer?.epoch),
                ),
                Stream.switchMap((selected) => {
                  if (!selected) {
                    connected = true;
                    return Stream.make(unavailable);
                  }
                  const after = connected ? undefined : input.after;
                  connected = true;
                  let delay = 250;
                  let lastEpoch: string | undefined;
                  const reconnect = (
                    recovering: boolean,
                  ): Stream.Stream<ModelActivityFrame, ModelActivityError> =>
                    Stream.unwrap(
                      Effect.gen(function* () {
                        if (recovering) {
                          yield* Effect.sleep(delay);
                          delay = Math.min(delay * 2, 5000);
                        }
                        const current = recovering
                          ? yield* prepare(input, cache).pipe(
                              Effect.mapError((error) =>
                                isModelActivityError(error) ? error : readError(),
                              ),
                            )
                          : selected;
                        if (!current) return Stream.make(unavailable);
                        const observer = current.observer;
                        const response = observer
                          ? observeSelected(
                              { ...current, observer },
                              recovering ? undefined : after,
                              recovering,
                            ).pipe(
                              Stream.tap((frame) =>
                                frame.kind === "snapshot"
                                  ? Effect.sync(() => {
                                      delay = 250;
                                      lastEpoch = frame.epoch;
                                    })
                                  : Effect.void,
                              ),
                              Stream.catch(() => Stream.empty),
                            )
                          : Stream.empty;
                        return Stream.concat(
                          response,
                          Stream.suspend(() => {
                            const disconnected: ModelActivityFrame =
                              lastEpoch === undefined
                                ? {
                                    ...unavailable,
                                    available: true,
                                    connectionState: "disconnected",
                                    epoch: `offline:${current.session.id}`,
                                    gap: true,
                                  }
                                : { kind: "disconnected", epoch: lastEpoch };
                            return Stream.make(disconnected);
                          }),
                        ).pipe(Stream.concat(Stream.suspend(() => reconnect(true))));
                      }),
                    );
                  return reconnect(false);
                }),
              );
            }),
          );
          const authorizationChanges = Stream.merge(
            Stream.fromSubscription(registryChanges),
            settingsChanges.pipe(Stream.map(() => undefined)),
          );
          // Revoke the old observation before rechecking any changed provider configuration.
          return Stream.concat(Stream.make(undefined), authorizationChanges).pipe(
            Stream.switchMap(() => followThread),
          );
        }),
      ),
  });
});
export const layer = Layer.effect(NativeModelActivityService, make);
