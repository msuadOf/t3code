import * as NodeServices from "@effect/platform-node/NodeServices";
import { assert, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Path from "effect/Path";

import { makeClient, resolveConnection } from "./CodexDaemonConnection.ts";

it.live("starts only a missing managed daemon and never redirects an explicit endpoint", () =>
  Effect.gen(function* () {
    const fs = yield* FileSystem.FileSystem;
    const path = yield* Path.Path;
    const homePath = yield* fs.makeTempDirectoryScoped({ prefix: "codex-daemon-test-" });
    // Node is deliberately not Codex: a bootstrap attempt exits unsuccessfully
    // without starting any daemon or using the developer's Codex home.
    const managedError = yield* makeClient({
      homePath,
      binaryPath: process.execPath,
      environment: {},
    }).pipe(Effect.flip);
    assert.equal(managedError._tag, "CodexDaemonStartError");
    const explicitError = yield* makeClient({
      homePath,
      binaryPath: process.execPath,
      endpoint: `unix://${path.join(homePath, "explicit.sock")}`,
      environment: {},
    }).pipe(Effect.flip);
    assert.equal(explicitError._tag, "CodexAppServerTransportError");
  }).pipe(Effect.scoped, Effect.provide(NodeServices.layer)),
);

it.layer(NodeServices.layer)("Codex daemon connection selection", (it) => {
  it.effect("keeps configured endpoints authoritative and uses the effective provider home", () =>
    Effect.gen(function* () {
      const path = yield* Path.Path;
      const selected = resolveConnection(
        {
          homePath: "/configured-home",
          endpoint: " ws://localhost:9000 ",
          environment: {
            CODEX_HOME: "/ambient-home",
            T3CODE_CODEX_NATIVE_ENDPOINT: "ws://localhost:8000",
          },
        },
        path,
      );
      assert.equal(selected.endpoint, "ws://localhost:9000");
      assert.equal(selected.managed, false);
      assert.equal(selected.environment.CODEX_HOME, "/configured-home");
      const fromEnvironment = resolveConnection(
        {
          endpoint: " ",
          environment: {
            CODEX_HOME: "/ambient-home",
            T3CODE_CODEX_NATIVE_ENDPOINT: "ws://localhost:8000",
          },
        },
        path,
      );
      assert.equal(fromEnvironment.endpoint, "ws://localhost:8000");
      assert.equal(fromEnvironment.managed, false);
      const managed = resolveConnection({ environment: { CODEX_HOME: "/ambient-home" } }, path);
      assert.equal(
        managed.endpoint,
        "unix:///ambient-home/app-server-control/app-server-control.sock",
      );
      assert.equal(managed.managed, true);
    }),
  );
});
