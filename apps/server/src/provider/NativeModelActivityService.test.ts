// @effect-diagnostics nodeBuiltinImport:off - Isolated private native-routing fixtures.
import * as NodeFSP from "node:fs/promises";
import * as NodePath from "node:path";
import * as NodeOS from "node:os";
import { vi } from "vite-plus/test";
import {
  prepareClaudeModelObserverSettings,
  writeClaudeModelObserverBinding,
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
  options?: { stateDir: string; identityDirectory: string },
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
    enabled: true,
    nativeSessions: {
      ...(options ? { identityDirectory: options.identityDirectory } : {}),
      list: Effect.succeed(
        visible
          ? [
              {
                id: nativeId,
                cwd,
                title: "Native",
                createdAt: 0,
                updatedAt: 0,
                archived: false,
                active: false,
              },
            ]
          : [],
      ),
    },
  } as unknown as ProviderInstance;
  return NativeModelActivityService.layer.pipe(
    Layer.provide(
      Layer.mergeAll(
        NodeServices.layer,
        Layer.succeed(HostProcess.HomeDirectory, "/host"),
        Layer.mock(ServerConfig.ServerConfig)({
          stateDir: options?.stateDir ?? "/unused-observer-test-state",
        } as ServerConfig.ServerConfig["Service"]),
        ServerSettings.layerTest({
          providerInstances: {
            [instanceId]: { driver: "claudeAgent", config: { sharedSessionRoot: "/allowed" } },
          },
        }),
        Layer.mock(ProjectionStore.ProjectionStoreV2)({
          getThread: () => Effect.succeed(thread),
          getThreadProviderContext: () =>
            Effect.succeed({ thread, providerThreads: [native], providerSessions: [] }),
        }),
        Layer.mock(ProviderInstanceRegistry.ProviderInstanceRegistry)({
          getInstance: () => Effect.succeed(instance),
        }),
      ),
    ),
  );
};
it.effect("does not expose a native UUID missing from the currently configured source", () =>
  Effect.gen(function* () {
    const service = yield* NativeModelActivityService.NativeModelActivityService;
    const frames = yield* service.subscribe({ threadId }).pipe(Stream.runCollect);
    assert.strictEqual(frames.length, 1);
    assert.strictEqual(frames[0]?.kind, "snapshot");
    assert.isTrue(frames[0]?.kind === "snapshot" && !frames[0].available);
  }).pipe(Effect.provide(testLayer("/allowed/repo", false))),
);
it.effect("rejects sessions outside the provider directory before reading observer bindings", () =>
  Effect.gen(function* () {
    const service = yield* NativeModelActivityService.NativeModelActivityService;
    const result = yield* service.subscribe({ threadId }).pipe(Stream.runCollect, Effect.result);
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
      observerStateDir: prepared.observerStateDir,
      upstreamUrl: prepared.upstreamUrl,
    });
    const jobPath = NodePath.join(identityDirectory, "jobs", nativeId.slice(0, 8), "state.json");
    await NodeFSP.mkdir(NodePath.dirname(jobPath), { recursive: true });
    await NodeFSP.writeFile(
      jobPath,
      JSON.stringify({ sessionId: nativeId, respawnFlags: ["--settings", prepared.settingsPath] }),
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
          JSON.stringify({ sessionId: nativeId, respawnFlags: [] }),
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
      const invalid = yield* read.pipe(Effect.result);
      assert.strictEqual(invalid._tag, "Failure");
      if (invalid._tag === "Failure") {
        assert.strictEqual(invalid.failure._tag, "ModelActivityError");
        if (invalid.failure._tag === "ModelActivityError")
          assert.strictEqual(invalid.failure.reason, "read-failed");
      }
    }),
);
