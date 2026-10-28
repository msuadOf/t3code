// @effect-diagnostics nodeBuiltinImport:off - Isolated private native-routing fixtures.
import * as NodeFSP from "node:fs/promises";
import * as NodePath from "node:path";
import * as NodeOS from "node:os";
import { vi } from "vite-plus/test";
import {
  prepareClaudeModelObserverSettings,
  writeClaudeModelObserverBinding,
  setClaudeModelObserverEnabled,
} from "./ClaudeModelObserverBindings.ts";
import * as NodeServices from "@effect/platform-node/NodeServices";
import { assert, it } from "@effect/vitest";
import {
  ProviderInstanceId,
  ProviderThreadId,
  ThreadId,
  type OrchestrationV2AppThread,
  type OrchestrationV2ProviderThread,
} from "@t3tools/contracts";
import type { ProviderInstance } from "@t3tools/provider-core/server/driver";
import * as HostProcess from "@t3tools/shared/HostProcess";
import * as EventSink from "../orchestration-v2/EventSink.ts";
import * as PubSub from "effect/PubSub";
import * as Queue from "effect/Queue";
import * as Deferred from "effect/Deferred";
import * as Fiber from "effect/Fiber";
import * as Observer from "./ClaudeModelObserver.ts";
import type { OrchestrationV2StoredEvent } from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Stream from "effect/Stream";
import * as ServerConfig from "../config.ts";
import * as ProjectionStore from "../orchestration-v2/ProjectionStore.ts";
import * as ServerSettings from "../serverSettings.ts";
import * as ProviderInstanceRegistry from "./ProviderInstanceRegistry.ts";
import * as NativeModelActivityService from "./NativeModelActivityService.ts";

vi.mock("./ClaudeModelObserver.ts", () => ({
  ensureClaudeModelObserver: async () => ({
    modelBaseUrl: "http://127.0.0.1:12345/proxy/" + "a".repeat(64),
  }),
  discoverClaudeModelObserver: async () => ({
    url: "http://127.0.0.1:12345",
    token: "a".repeat(64),
    epoch: "fixture",
  }),
  observeClaudeModelActivity: async function* () {
    yield {
      type: "observer.snapshot",
      epoch: "fixture",
      seq: 0,
      oldestSeq: 1,
      gap: false,
      events: [],
    };
  },
}));

const instanceId = ProviderInstanceId.make("claude");
const threadId = ThreadId.make("thread");
const providerThreadId = ProviderThreadId.make("provider-thread");
const nativeId = "01234567-89ab-cdef-0123-456789abcdef";
const testLayer = (
  cwd: string,
  visible: boolean,
  options?: {
    stateDir: string;
    identityDirectory: string;
    currentNative?: () => string | null;
    changes?: Stream.Stream<OrchestrationV2StoredEvent>;
    onList?: () => void;
    registryChanges?: PubSub.PubSub<void>;
    enabled?: () => boolean;
    configuredDirectory?: () => string;
    sharedRoot?: () => string;
    settingsChanges?: Stream.Stream<import("@t3tools/contracts").ServerSettings>;
  },
) => {
  const thread = {
    id: threadId,
    providerInstanceId: instanceId,
    activeProviderThreadId: providerThreadId,
  } as OrchestrationV2AppThread;
  const native = {
    id: providerThreadId,
    providerInstanceId: instanceId,
    driver: "claudeAgent",
    nativeThreadRef: { nativeId },
  } as OrchestrationV2ProviderThread;
  const instance = {
    instanceId,
    driverKind: "claudeAgent",
    get enabled() {
      return options?.enabled?.() ?? true;
    },
    nativeSessions: {
      get identityDirectory() {
        return options?.configuredDirectory?.() ?? options?.identityDirectory;
      },
      list: Effect.sync(() => {
        options?.onList?.();
        return visible
          ? [
              {
                id: options?.currentNative?.() ?? nativeId,
                cwd,
                title: "Native",
                createdAt: 0,
                updatedAt: 0,
                archived: false,
                active: false,
              },
            ]
          : [];
      }),
    },
  } as unknown as ProviderInstance;
  return NativeModelActivityService.layer.pipe(
    Layer.provide(
      Layer.mergeAll(
        NodeServices.layer,
        Layer.mock(EventSink.EventSinkV2)({
          latestSequence: () => Effect.succeed(0),
          stream: () => options?.changes ?? Stream.never,
        }),
        Layer.succeed(HostProcess.HomeDirectory, "/host"),
        Layer.mock(ServerConfig.ServerConfig)({
          stateDir: options?.stateDir ?? "/unused-observer-test-state",
        } as ServerConfig.ServerConfig["Service"]),
        options?.sharedRoot
          ? Layer.mock(ServerSettings.ServerSettingsService)({
              getSettings: Effect.sync(
                () =>
                  ({
                    providerInstances: {
                      [instanceId]: {
                        driver: "claudeAgent",
                        config: { sharedSessionRoot: options.sharedRoot!() },
                      },
                    },
                  }) as import("@t3tools/contracts").ServerSettings,
              ),
              subscribeChanges: Effect.succeed(options.settingsChanges ?? Stream.never),
            })
          : ServerSettings.layerTest({
              providerInstances: {
                [instanceId]: { driver: "claudeAgent", config: { sharedSessionRoot: "/allowed" } },
              },
            }),
        Layer.mock(ProjectionStore.ProjectionStoreV2)({
          getThread: () => Effect.succeed(thread),
          getThreadProviderContext: () =>
            Effect.sync(() => ({
              thread,
              providerThreads: [
                {
                  ...native,
                  nativeThreadRef: options?.currentNative
                    ? options.currentNative()
                      ? {
                          ...native.nativeThreadRef!,
                          nativeId: options.currentNative()!,
                        }
                      : null
                    : native.nativeThreadRef,
                },
              ],
              providerSessions: [],
            })),
        }),
        Layer.mock(ProviderInstanceRegistry.ProviderInstanceRegistry)({
          getInstance: () => Effect.succeed(instance),
          subscribeChanges: options?.registryChanges
            ? PubSub.subscribe(options.registryChanges)
            : PubSub.unbounded<void>().pipe(Effect.flatMap(PubSub.subscribe)),
        }),
      ),
    ),
  );
};
it.effect("does not expose a native UUID missing from the currently configured source", () =>
  Effect.gen(function* () {
    const service = yield* NativeModelActivityService.NativeModelActivityService;
    const frames = yield* service.subscribe({ threadId }).pipe(Stream.take(1), Stream.runCollect);
    assert.strictEqual(frames.length, 1);
    assert.strictEqual(frames[0]?.kind, "snapshot");
    assert.isTrue(frames[0]?.kind === "snapshot" && !frames[0].available);
  }).pipe(Effect.provide(testLayer("/allowed/repo", false))),
);
it.effect("rejects sessions outside the provider directory before reading observer bindings", () =>
  Effect.gen(function* () {
    const service = yield* NativeModelActivityService.NativeModelActivityService;
    const result = yield* service
      .subscribe({ threadId })
      .pipe(Stream.take(1), Stream.runCollect, Effect.result);
    assert.strictEqual(result._tag, "Failure");
    if (result._tag === "Failure") assert.strictEqual(result.failure.reason, "outside-root");
  }).pipe(Effect.provide(testLayer("/private/repo", true))),
);

const routingFixture = Effect.acquireRelease(
  Effect.tryPromise(async () => {
    const stateDir = await NodeFSP.mkdtemp(NodePath.join(NodeOS.tmpdir(), "t3-model-service-"));
    const identityDirectory = NodePath.join(stateDir, "native");
    const prepared = await prepareClaudeModelObserverSettings({
      stateDir,
      instanceId,
      upstreamUrl: "http://127.0.0.1:10000/",
    });
    await writeClaudeModelObserverBinding({
      stateDir,
      instanceId,
      nativeSessionId: nativeId,
      configDirectory: identityDirectory,
      settingsPath: prepared.settingsPath,
      observerStateDir: prepared.observerStateDir,
      upstreamUrl: prepared.upstreamUrl,
    });
    const jobPath = NodePath.join(identityDirectory, "jobs", nativeId.slice(0, 8), "state.json");
    await NodeFSP.mkdir(NodePath.dirname(jobPath), { recursive: true });
    await NodeFSP.writeFile(
      jobPath,
      JSON.stringify({
        sessionId: nativeId,
        state: "stopped",
        respawnFlags: ["--settings", prepared.settingsPath],
      }),
    );
    return { stateDir, identityDirectory, jobPath, settingsPath: prepared.settingsPath };
  }).pipe(Effect.orDie),
  (fixture) => Effect.promise(() => NodeFSP.rm(fixture.stateDir, { recursive: true, force: true })),
);
const readFirst = Effect.gen(function* () {
  const service = yield* NativeModelActivityService.NativeModelActivityService;
  return yield* service.subscribe({ threadId }).pipe(Stream.take(1), Stream.runCollect);
});
it.effect(
  "requires the current native job's exact observer route before publishing model activity",
  () =>
    Effect.gen(function* () {
      const fixture = yield* routingFixture;
      const read = readFirst.pipe(Effect.provide(testLayer("/allowed/repo", true, fixture)));
      const valid = yield* read;
      assert.isTrue(valid[0]?.kind === "snapshot" && valid[0].available);
      yield* Effect.promise(() =>
        NodeFSP.writeFile(
          fixture.jobPath,
          JSON.stringify({ sessionId: nativeId, state: "stopped", respawnFlags: [] }),
        ),
      );
      const recreated = yield* read;
      assert.isTrue(recreated[0]?.kind === "snapshot" && !recreated[0].available);
      yield* Effect.promise(() =>
        NodeFSP.writeFile(
          fixture.jobPath,
          JSON.stringify({
            sessionId: nativeId,
            respawnFlags: ["--settings", fixture.settingsPath],
          }),
        ),
      );
      yield* Effect.promise(() =>
        NodeFSP.writeFile(
          fixture.settingsPath,
          JSON.stringify({ env: { ANTHROPIC_BASE_URL: "https://external.invalid/" } }),
        ),
      );
      const mismatched = yield* read;
      assert.isTrue(mismatched[0]?.kind === "snapshot" && !mismatched[0].available);
      yield* Effect.promise(() => NodeFSP.writeFile(fixture.settingsPath, "broken-json"));
      const invalid = yield* read.pipe(Effect.result);
      assert.strictEqual(invalid._tag, "Failure");
      if (invalid._tag === "Failure") {
        assert.strictEqual(invalid.failure._tag, "ModelActivityError");
        if (invalid.failure._tag === "ModelActivityError")
          assert.strictEqual(invalid.failure.reason, "read-failed");
      }
    }),
);

it.effect(
  "keeps an unbound subscription alive, switches native workers, and aborts observation on unmount",
  () =>
    Effect.gen(function* () {
      const fixture = yield* routingFixture;
      const secondId = "abcdef01-89ab-cdef-0123-456789abcdef";
      const prepared = yield* Effect.promise(() =>
        prepareClaudeModelObserverSettings({
          stateDir: fixture.stateDir,
          instanceId,
          upstreamUrl: "http://127.0.0.1:10000/",
        }),
      );
      yield* Effect.promise(async () => {
        await writeClaudeModelObserverBinding({
          stateDir: fixture.stateDir,
          instanceId,
          nativeSessionId: secondId,
          configDirectory: fixture.identityDirectory,
          settingsPath: prepared.settingsPath,
          observerStateDir: prepared.observerStateDir,
          upstreamUrl: prepared.upstreamUrl,
        });
        const secondJob = NodePath.join(
          fixture.identityDirectory,
          "jobs",
          secondId.slice(0, 8),
          "state.json",
        );
        await NodeFSP.mkdir(NodePath.dirname(secondJob), { recursive: true });
        await NodeFSP.writeFile(
          secondJob,
          JSON.stringify({
            sessionId: secondId,
            state: "stopped",
            respawnFlags: ["--settings", prepared.settingsPath],
          }),
        );
      });
      const changes = yield* Queue.unbounded<OrchestrationV2StoredEvent>();
      const frames = yield* Queue.unbounded<import("@t3tools/contracts").ModelActivityFrame>();
      const started = yield* Queue.unbounded<{ sessionId: string; signal: AbortSignal }>();
      const closed = yield* Queue.unbounded<string>();
      const liveSubscribed = yield* Deferred.make<void>();
      const validated = yield* Queue.unbounded<void>();
      yield* Effect.acquireRelease(
        Effect.sync(() =>
          vi.spyOn(Observer, "discoverClaudeModelObserver").mockImplementation(async () => {
            Queue.offerUnsafe(validated, undefined);
            return { url: "http://127.0.0.1:12345", token: "a".repeat(64), epoch: "fixture" };
          }),
        ),
        (spy) => Effect.sync(() => spy.mockRestore()),
      );
      let currentNative: string | null = null;
      let listCalls = 0;
      const observerSpy = yield* Effect.acquireRelease(
        Effect.sync(() =>
          vi
            .spyOn(Observer, "observeClaudeModelActivity")
            .mockImplementation(async function* (input) {
              const signal = input.signal!;
              const stopped = new Promise<void>((resolve) => {
                if (signal.aborted) resolve();
                else signal.addEventListener("abort", () => resolve(), { once: true });
              });
              Queue.offerUnsafe(started, { sessionId: input.sessionId, signal });
              try {
                yield {
                  type: "observer.snapshot",
                  epoch: "fixture",
                  seq: 0,
                  oldestSeq: 1,
                  gap: false,
                  events: [],
                };
                await stopped;
              } finally {
                Queue.offerUnsafe(closed, input.sessionId);
              }
            }),
        ),
        (spy) => Effect.sync(() => spy.mockRestore()),
      );
      const layer = testLayer("/allowed/repo", true, {
        ...fixture,
        currentNative: () => currentNative,
        onList: () => {
          listCalls++;
        },
        changes: Stream.unwrap(
          Deferred.succeed(liveSubscribed, undefined).pipe(Effect.as(Stream.fromQueue(changes))),
        ),
      });
      const consumer = yield* Effect.gen(function* () {
        const service = yield* NativeModelActivityService.NativeModelActivityService;
        yield* service
          .subscribe({ threadId })
          .pipe(Stream.runForEach((frame) => Queue.offer(frames, frame)));
      }).pipe(Effect.provide(layer), Effect.forkScoped);
      const initial = yield* Queue.take(frames);
      assert.isTrue(initial.kind === "snapshot" && !initial.available);
      yield* Deferred.await(liveSubscribed);
      assert.strictEqual(observerSpy.mock.calls.length, 0);
      assert.strictEqual(listCalls, 0);
      const changed = {
        sequence: 1,
        event: { type: "provider-thread.updated", threadId },
      } as OrchestrationV2StoredEvent;
      currentNative = nativeId;
      yield* Queue.offer(changes, changed);
      const first = yield* Queue.take(started);
      const active = yield* Queue.take(frames);
      assert.strictEqual(first.sessionId, nativeId);
      assert.isTrue(active.kind === "snapshot" && active.available);
      assert.strictEqual(listCalls, 1);
      yield* Queue.take(validated);
      yield* Queue.offer(changes, { ...changed, sequence: 2 });
      yield* Queue.take(validated);
      assert.strictEqual(listCalls, 1);
      yield* Queue.offer(changes, {
        sequence: 3,
        event: { type: "turn-item.updated", threadId },
      } as OrchestrationV2StoredEvent);
      const enableInput = {
        stateDir: fixture.stateDir,
        instanceId,
        nativeSessionId: nativeId,
        configDirectory: fixture.identityDirectory,
      };
      yield* Effect.promise(() =>
        setClaudeModelObserverEnabled({ ...enableInput, enabled: false }),
      );
      yield* Queue.offer(changes, {
        sequence: 4,
        event: { type: "thread.metadata-updated", threadId },
      } as OrchestrationV2StoredEvent);
      const disabled = yield* Queue.take(frames);
      assert.isTrue(disabled.kind === "snapshot" && !disabled.available);
      assert.isTrue(first.signal.aborted);
      assert.strictEqual(yield* Queue.take(closed), nativeId);
      assert.strictEqual(listCalls, 1);
      yield* Effect.promise(() => setClaudeModelObserverEnabled({ ...enableInput, enabled: true }));
      yield* Queue.offer(changes, {
        sequence: 5,
        event: { type: "thread.metadata-updated", threadId },
      } as OrchestrationV2StoredEvent);
      const resumed = yield* Queue.take(started);
      assert.strictEqual(resumed.sessionId, nativeId);
      const enabled = yield* Queue.take(frames);
      assert.isTrue(enabled.kind === "snapshot" && enabled.available);
      assert.strictEqual(listCalls, 1);
      currentNative = secondId;
      yield* Queue.offer(changes, { ...changed, sequence: 6 });
      const next = yield* Queue.take(started);
      assert.strictEqual(next.sessionId, secondId);
      assert.isTrue(resumed.signal.aborted);
      assert.strictEqual(yield* Queue.take(closed), nativeId);
      const switched = yield* Queue.take(frames);
      assert.isTrue(switched.kind === "snapshot" && switched.available);
      assert.strictEqual(listCalls, 2);
      assert.strictEqual(observerSpy.mock.calls.length, 3);
      yield* Fiber.interrupt(consumer);
      assert.isTrue(next.signal.aborted);
      assert.strictEqual(yield* Queue.take(closed), secondId);
    }),
);

it.effect("revokes observation on registry-only provider, directory, and shared-root changes", () =>
  Effect.gen(function* () {
    const fixture = yield* routingFixture;
    const registryChanges = yield* PubSub.unbounded<void>();
    const settingsNotifications =
      yield* Queue.unbounded<import("@t3tools/contracts").ServerSettings>();
    const frames = yield* Queue.unbounded<import("@t3tools/contracts").ModelActivityFrame>();
    const started = yield* Queue.unbounded<AbortSignal>();
    const closed = yield* Queue.unbounded<void>();
    let enabled = true;
    let directory = fixture.identityDirectory;
    let root = "/allowed";
    yield* Effect.acquireRelease(
      Effect.sync(() =>
        vi
          .spyOn(Observer, "observeClaudeModelActivity")
          .mockImplementation(async function* (input) {
            const signal = input.signal!;
            const stopped = new Promise<void>((resolve) => {
              if (signal.aborted) resolve();
              else signal.addEventListener("abort", () => resolve(), { once: true });
            });
            Queue.offerUnsafe(started, signal);
            try {
              yield {
                type: "observer.snapshot",
                epoch: "fixture",
                seq: 0,
                oldestSeq: 1,
                gap: false,
                events: [],
              };
              await stopped;
            } finally {
              Queue.offerUnsafe(closed, undefined);
            }
          }),
      ),
      (spy) => Effect.sync(() => spy.mockRestore()),
    );
    const layer = testLayer("/allowed/repo", true, {
      ...fixture,
      registryChanges,
      enabled: () => enabled,
      configuredDirectory: () => directory,
      sharedRoot: () => root,
      settingsChanges: Stream.fromQueue(settingsNotifications),
    });
    const consume = Effect.gen(function* () {
      const service = yield* NativeModelActivityService.NativeModelActivityService;
      return yield* service.subscribe({ threadId }).pipe(
        Stream.runForEach((frame) => Queue.offer(frames, frame)),
        Effect.result,
      );
    }).pipe(Effect.provide(layer));
    const consumer = yield* consume.pipe(Effect.forkScoped);
    const first = yield* Queue.take(started);
    const initial = yield* Queue.take(frames);
    assert.isTrue(initial.kind === "snapshot" && initial.available);
    enabled = false;
    yield* PubSub.publish(registryChanges, undefined);
    const disabled = yield* Queue.take(frames);
    assert.isTrue(disabled.kind === "snapshot" && !disabled.available);
    assert.isTrue(first.aborted);
    yield* Queue.take(closed);
    enabled = true;
    yield* PubSub.publish(registryChanges, undefined);
    const restored = yield* Queue.take(started);
    yield* Queue.take(frames);
    directory = NodePath.join(fixture.stateDir, "other-native-config");
    yield* PubSub.publish(registryChanges, undefined);
    const changedDirectory = yield* Queue.take(frames);
    assert.isTrue(changedDirectory.kind === "snapshot" && !changedDirectory.available);
    assert.isTrue(restored.aborted);
    yield* Queue.take(closed);
    directory = fixture.identityDirectory;
    yield* PubSub.publish(registryChanges, undefined);
    const last = yield* Queue.take(started);
    yield* Queue.take(frames);
    root = "/outside";
    yield* PubSub.publish(registryChanges, undefined);
    const result = yield* Fiber.join(consumer);
    assert.strictEqual(result._tag, "Failure");
    if (result._tag === "Failure") {
      assert.strictEqual(result.failure._tag, "ModelActivityError");
      if (result.failure._tag === "ModelActivityError")
        assert.strictEqual(result.failure.reason, "outside-root");
    }
    assert.isTrue(last.aborted);
    yield* Queue.take(closed);
    root = "/allowed";
    const settingsConsumer = yield* consume.pipe(Effect.forkScoped);
    const settingsSignal = yield* Queue.take(started);
    yield* Queue.take(frames);
    root = "/outside";
    yield* Queue.offer(settingsNotifications, {} as import("@t3tools/contracts").ServerSettings);
    const settingsResult = yield* Fiber.join(settingsConsumer);
    assert.strictEqual(settingsResult._tag, "Failure");
    if (settingsResult._tag === "Failure" && settingsResult.failure._tag === "ModelActivityError")
      assert.strictEqual(settingsResult.failure.reason, "outside-root");
    assert.isTrue(settingsSignal.aborted);
    yield* Queue.take(closed);
  }),
);
