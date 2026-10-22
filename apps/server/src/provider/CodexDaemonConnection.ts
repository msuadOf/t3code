import * as NodeOS from "node:os";
import * as Path from "effect/Path";

import * as CodexClient from "effect-codex-app-server/client";
import * as CodexDaemon from "effect-codex-app-server/daemon";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Schedule from "effect/Schedule";
import * as Schema from "effect/Schema";
import { ChildProcess, ChildProcessSpawner } from "effect/process";

export class CodexDaemonStartError extends Schema.TaggedError<CodexDaemonStartError>()(
  "CodexDaemonStartError",
  { cause: Schema.Defect() },
) {
  override get message(): string {
    return "Could not start the shared Codex daemon. Check the configured Codex executable and home directory.";
  }
}

export interface CodexDaemonConnectionInput {
  readonly binaryPath?: string;
  readonly homePath?: string;
  readonly endpoint?: string;
  readonly environment?: NodeJS.ProcessEnv;
}

export const resolveConnection = (input: CodexDaemonConnectionInput, path: Path.Path) => {
  const environment = input.environment ?? process.env;
  const homePath =
    input.homePath?.trim() || environment.CODEX_HOME || path.join(NodeOS.homedir(), ".codex");
  const configuredEndpoint =
    input.endpoint?.trim() || environment.T3CODE_CODEX_NATIVE_ENDPOINT?.trim();
  const socketPath = path.join(homePath, "app-server-control", "app-server-control.sock");
  return {
    homePath,
    endpoint: configuredEndpoint || `unix://${socketPath}`,
    socketPath,
    managed: !configuredEndpoint,
    environment: { ...environment, CODEX_HOME: homePath },
  };
};

/** A daemon belongs to Codex, so closing a T3 client never stops its process. */
export const makeClient = Effect.fn("CodexDaemonConnection.makeClient")(function* (
  input: CodexDaemonConnectionInput,
  options: CodexClient.CodexAppServerClientOptions = {},
) {
  const connection = resolveConnection(input, yield* Path.Path);
  const fileSystem = yield* FileSystem.FileSystem;
  const connect = CodexDaemon.makeStdio(connection.endpoint);
  const stdio = yield* connect.pipe(
    Effect.catchTags({
      CodexAppServerTransportError: (original) =>
        Effect.gen(function* () {
          // An explicit endpoint is authoritative. A missing default socket is
          // the only condition under which T3 starts Codex's managed daemon.
          if (!connection.managed || (yield* fileSystem.exists(connection.socketPath))) {
            return yield* original;
          }
          const spawner = yield* ChildProcessSpawner.ChildProcessSpawner;
          const handle = yield* spawner
            .spawn(
              ChildProcess.make(input.binaryPath || "codex", ["app-server", "daemon", "start"], {
                env: connection.environment,
                stdin: "ignore",
                stdout: "ignore",
                stderr: "ignore",
              }),
            )
            .pipe(Effect.mapError((cause) => new CodexDaemonStartError({ cause })));
          const code = yield* handle.exitCode.pipe(
            Effect.mapError((cause) => new CodexDaemonStartError({ cause })),
          );
          if (Number(code) !== 0)
            return yield* new CodexDaemonStartError({ cause: { exitCode: Number(code) } });
          return yield* connect.pipe(
            Effect.retry({ schedule: Schedule.exponential("100 millis"), times: 5 }),
          );
        }),
    }),
  );
  return yield* CodexClient.make(stdio, options);
});
