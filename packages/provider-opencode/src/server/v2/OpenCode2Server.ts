/**
 * A 2.x server serves every location from one process, so an instance shares
 * one server across all of its threads and directories.
 */
import * as Path from "effect/Path";
import * as FileSystem from "effect/FileSystem";
import * as HostProcess from "@t3tools/shared/HostProcess";
import { Service } from "@opencode/client/effect/service";
import type { OpenCodeClient } from "@opencode/client/effect";
import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as P from "effect/Predicate";
import * as Redacted from "effect/Redacted";
import * as HttpClientError from "effect/http/HttpClientError";

import * as OpenCodeRuntime from "../OpenCodeRuntime.ts";
import * as OpenCode2Client from "./OpenCode2Client.ts";
import { ensureObserver } from "./OpenCode2Observer.ts";
import * as HttpClient from "effect/http/HttpClient";

const INFO_TIMEOUT = "5 seconds";

export interface OpenCode2Connection extends OpenCode2Client.OpenCode2Api {
  readonly url: string;
  readonly version: string;
  readonly external: boolean;
  /** Session execution belongs to the native service, never to this T3 client. */
  readonly shared?: boolean;
  readonly observerEvents?: Effect.Effect<
    Effect.Success<OpenCode2Client.OpenCode2Api["events"]>,
    HttpClientError.HttpClientError | OpenCodeRuntime.OpenCodeRuntimeError
  >;
}

export class OpenCode2Server extends Context.Service<
  OpenCode2Server,
  {
    readonly withConnection: <A, E, R>(
      use: (connection: OpenCode2Connection) => Effect.Effect<A, E, R>,
    ) => Effect.Effect<A, E | OpenCodeRuntime.OpenCodeRuntimeError, R>;
  }
>()("@t3tools/provider-opencode/server/v2/OpenCode2Server") {}

const httpFailureOf = (cause: unknown): HttpClientError.HttpClientError | undefined => {
  if (HttpClientError.isHttpClientError(cause)) return cause;
  if (
    P.isTagged(cause, "ClientError") &&
    P.hasProperty(cause, "cause") &&
    HttpClientError.isHttpClientError(cause.cause)
  ) {
    return cause.cause;
  }
  return undefined;
};

// Empty 401s still reject passwords; undecodable 2xx may be 1.x HTML, not OpenCode 2.
const describeInfoFailure = (cause: unknown) => {
  const failure = httpFailureOf(cause);
  if (failure?.reason._tag === "TransportError") {
    return "Could not reach the OpenCode server.";
  }
  const status = failure?.response?.status;
  if (status === 401) return "The OpenCode server rejected the server password.";
  if (status !== undefined && (status < 200 || status >= 300)) {
    return `The OpenCode server returned HTTP ${status}.`;
  }
  return "The server is not an OpenCode 2 server.";
};

/** Only server info proves readiness; keep credential-bearing failure details out of client text. */
export const verifyServer = (client: OpenCodeClient) =>
  client.server.info().pipe(
    Effect.timeoutOrElse({
      duration: INFO_TIMEOUT,
      orElse: () =>
        Effect.fail(
          new OpenCodeRuntime.OpenCodeRuntimeError({
            operation: "server.info",
            detail: "Timed out waiting for the OpenCode server.",
          }),
        ),
    }),
    Effect.catchTags({
      UnauthorizedError: (cause) =>
        Effect.fail(
          new OpenCodeRuntime.OpenCodeRuntimeError({
            operation: "server.info",
            detail: "The OpenCode server rejected the server password.",
            cause,
          }),
        ),
    }),
    Effect.mapError((cause) =>
      OpenCodeRuntime.isOpenCodeRuntimeError(cause)
        ? cause
        : new OpenCodeRuntime.OpenCodeRuntimeError({
            operation: "server.info",
            detail: describeInfoFailure(cause),
            cause,
          }),
    ),
    Effect.map((info) => info.version),
  );

export const make = Effect.fn("OpenCode2Server.make")(function* (input: {
  readonly binaryPath: string;
  readonly serverUrl: string;
  readonly serverPassword: string;
  readonly directory: string;
  readonly environment: NodeJS.ProcessEnv;
  readonly observerStateDir?: string;
}) {
  const opencode = yield* OpenCode2Client.OpenCode2Client;
  const httpClient = yield* HttpClient.HttpClient;
  const connectTo = Effect.fn("OpenCode2Server.connectTo")(function* (
    url: string,
    password: Redacted.Redacted,
    external: boolean,
  ) {
    const api = yield* opencode.connect({ baseUrl: url, password });
    const version = yield* verifyServer(api.client);
    const observer =
      input.observerStateDir === undefined
        ? undefined
        : Effect.tryPromise({
            try: () =>
              ensureObserver({
                stateDir: input.observerStateDir!,
                baseUrl: url,
                password: Redacted.value(password),
              }),
            catch: (cause) =>
              new OpenCodeRuntime.OpenCodeRuntimeError({
                operation: "observer.start",
                detail: "Could not start the OpenCode event observer.",
                cause,
              }),
          });
    if (observer !== undefined) yield* observer;
    const observerEvents = observer?.pipe(
      Effect.flatMap((endpoint) => OpenCode2Client.observerEvents(httpClient, endpoint)),
    );
    return {
      ...api,
      ...(observerEvents === undefined ? {} : { observerEvents }),
      url,
      version,
      external,
      shared: true,
    } satisfies OpenCode2Connection;
  });
  let latest: OpenCode2Connection | undefined;
  const remember = (connection: OpenCode2Connection) =>
    Effect.sync(() => {
      latest = connection;
      return connection.version;
    });

  const serverUrl = input.serverUrl.trim();
  if (serverUrl.length > 0) {
    const connect = connectTo(serverUrl, Redacted.make(input.serverPassword), true).pipe(
      Effect.tap(remember),
    );
    return OpenCode2Server.of({
      withConnection: Effect.fn("OpenCode2Server.withConnection")(function (use) {
        return Effect.suspend(() => (latest === undefined ? connect : Effect.succeed(latest))).pipe(
          Effect.flatMap(use),
        );
      }),
    });
  }

  const path = yield* Path.Path;
  const home = yield* HostProcess.HomeDirectory;
  const fileSystem = yield* FileSystem.FileSystem;
  const options = {
    file: serviceRegistrationFile(path, input.environment, home),
    command: [input.binaryPath, "serve", "--service"],
    env: Object.fromEntries(
      Object.entries(input.environment).filter(
        (entry): entry is [string, string] => entry[1] !== undefined,
      ),
    ),
  };
  let previousPassword: string | undefined;
  return OpenCode2Server.of({
    withConnection: Effect.fn("OpenCode2Server.withConnection")(function* (use) {
      // Rediscover on every acquisition: native service restarts can rotate
      // the URL and credentials. Never stop the service on client teardown.
      const endpoint = yield* Service.ensure(options).pipe(
        Effect.provideService(FileSystem.FileSystem, fileSystem),
        Effect.mapError(
          (cause) =>
            new OpenCodeRuntime.OpenCodeRuntimeError({
              operation: "service.ensure",
              detail: "Could not connect to the shared OpenCode service.",
              cause,
            }),
        ),
      );
      const password = endpoint.auth?.password ?? "";
      if (latest === undefined || latest.url !== endpoint.url || previousPassword !== password) {
        latest = yield* connectTo(endpoint.url, Redacted.make(password), false);
        previousPassword = password;
      }
      return yield* use(latest);
    }),
  });
});

/** Match native CLI discovery, including per-instance XDG/HOME overrides. */
export const serviceRegistrationFile = (
  path: Path.Path,
  environment: NodeJS.ProcessEnv,
  home: string,
): string =>
  path.join(
    environment.XDG_STATE_HOME ??
      path.join(environment.HOME ?? environment.USERPROFILE ?? home, ".local", "state"),
    "opencode",
    "service.json",
  );
