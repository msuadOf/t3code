import * as NodeServices from "@effect/platform-node/NodeServices";
import { assert, it } from "@effect/vitest";
import { EnvironmentId, ProviderInstanceId, ThreadId } from "@t3tools/contracts";
import type { McpProviderSessionConfig } from "@t3tools/provider-core/server/mcpSession";
import * as FileSystem from "effect/FileSystem";
import * as Path from "effect/Path";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import {
  NativeClaudeMcpBindings,
  readClaudeMcpRelayCredential,
} from "./NativeClaudeMcpBindings.ts";
import { openNativeClaudeMcpHistory } from "./NativeClaudeMcpSharing.ts";
const nativeId = "aaaaaaaa-1234-1234-1234-123456789abc";
const instanceId = ProviderInstanceId.make("claude");
const threadId = ThreadId.make("history-owner");
const candidate = {
  id: nativeId,
  cwd: "/work",
  title: "History",
  createdAt: 1,
  updatedAt: 2,
  archived: false,
  active: false,
  sharingState: "history" as const,
};
const config = (generation: string): McpProviderSessionConfig => ({
  environmentId: EnvironmentId.make("test"),
  threadId,
  providerInstanceId: instanceId,
  providerSessionId: generation,
  endpoint: "http://127.0.0.1:1234/mcp",
  authorizationHeader: `Bearer ${generation}`,
  browserToolsAvailable: false,
  bindNativeSession: () => Effect.succeed(true),
});
const launcher = (directory: string) => ({ command: "node", args: ["relay", directory] });

it.effect(
  "reuses an owned historical launch and rotates its credential before starting its original UUID",
  () =>
    Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem;
      const path = yield* Path.Path;
      const root = yield* fs.realPath(yield* fs.makeTempDirectoryScoped());
      const bindings = new NativeClaudeMcpBindings(path.join(root, "bindings"));
      const prepared = yield* Effect.promise(() =>
        bindings.prepare({ config: config("old"), launcher }),
      );
      yield* Effect.promise(() =>
        bindings.bind({ prepared, nativeSessionId: nativeId, config: config("old") }),
      );
      let opened = 0;
      const sessions = {
        available: async () => [candidate],
        open: async (_id: string, options?: { readonly mcpConfigPath?: string }) => {
          opened++;
          assert.equal(options?.mcpConfigPath, prepared.mcpConfigPath);
          // Native initialize will read the new credential even though the old job roster was lost.
          assert.equal(
            (
              await readClaudeMcpRelayCredential({
                launchDirectory: prepared.launchDirectory,
                nativeSessionId: nativeId,
                discovery: true,
              })
            ).providerSessionId,
            "new",
          );
          return candidate;
        },
      };
      const input = {
        sessionId: nativeId,
        instanceId,
        options: { threadId },
        sessions,
        bindings,
        readConfig: () => Effect.succeed(config("new")),
        launcher,
      };
      yield* openNativeClaudeMcpHistory(input);
      assert.equal(opened, 1);
      yield* Effect.promise(() => bindings.revoke({ nativeSessionId: nativeId }));
      assert.isTrue(Exit.isFailure(yield* Effect.exit(openNativeClaudeMcpHistory(input))));
      assert.equal(opened, 1);
    }).pipe(Effect.scoped, Effect.provide(NodeServices.layer)),
);

it.effect("fails before native launch for missing or foreign historical credentials", () =>
  Effect.gen(function* () {
    const fs = yield* FileSystem.FileSystem;
    const path = yield* Path.Path;
    const root = yield* fs.realPath(yield* fs.makeTempDirectoryScoped());
    const bindings = new NativeClaudeMcpBindings(path.join(root, "bindings"));
    let opened = 0;
    const sessions = {
      available: async () => [candidate],
      open: async () => {
        opened++;
        return candidate;
      },
    };
    for (const current of [undefined, { ...config("foreign"), threadId: ThreadId.make("other") }]) {
      const result = yield* Effect.exit(
        openNativeClaudeMcpHistory({
          sessionId: nativeId,
          instanceId,
          options: { threadId },
          sessions,
          bindings,
          readConfig: () => Effect.succeed(current),
          launcher,
        }),
      );
      assert.isTrue(Exit.isFailure(result));
    }
    assert.equal(opened, 0);
  }).pipe(Effect.scoped, Effect.provide(NodeServices.layer)),
);
