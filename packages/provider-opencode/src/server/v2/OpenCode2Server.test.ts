// @effect-diagnostics-next-line nodeBuiltinImport:off -- A real native HTTP service verifies SDK discovery and ownership.
import * as NodeHttp from "node:http";
import * as NodeServices from "@effect/platform-node/NodeServices";
import { assert, it } from "@effect/vitest";
import * as HostProcess from "@t3tools/shared/HostProcess";
import * as Effect from "effect/Effect";
import * as Fiber from "effect/Fiber";
import * as FileSystem from "effect/FileSystem";
import * as Layer from "effect/Layer";
import * as Path from "effect/Path";
import * as TestClock from "effect/testing/TestClock";
import * as FetchHttpClient from "effect/http/FetchHttpClient";
import * as HttpClient from "effect/http/HttpClient";
import * as HttpClientResponse from "effect/http/HttpClientResponse";
import { describe } from "vite-plus/test";

import * as OpenCodeRuntime from "../OpenCodeRuntime.ts";
import * as OpenCodeServerLedger from "../OpenCodeServerLedger.ts";
import * as OpenCode2Client from "./OpenCode2Client.ts";
import * as OpenCode2Server from "./OpenCode2Server.ts";

// Shapes and content types observed on `opencode serve` 2.0.18 (pid and port replaced). 1.x
// answers /api/info with its web UI: 200 text/html.
const UNAUTHORIZED_BODY = '{"_tag":"UnauthorizedError","message":"Authentication required"}';
const SPA_BODY = "<!doctype html><html><head><title>OpenCode</title></head></html>";

const layerServerReplying = (reply: {
  readonly status: number;
  readonly contentType?: string;
  readonly body: string;
}) =>
  Layer.succeed(
    HttpClient.HttpClient,
    HttpClient.make((request) =>
      Effect.succeed(
        HttpClientResponse.fromWeb(
          request,
          new Response(reply.body, {
            status: reply.status,
            headers: reply.contentType === undefined ? {} : { "content-type": reply.contentType },
          }),
        ),
      ),
    ),
  );

const verify = (httpClient: Layer.Layer<HttpClient.HttpClient>, url = "http://127.0.0.1:4096") =>
  Effect.gen(function* () {
    const opencode = yield* OpenCode2Client.OpenCode2Client;
    const { client } = yield* opencode.connect({ baseUrl: url, password: "secret" });
    return yield* OpenCode2Server.verifyServer(client);
  }).pipe(Effect.provide(OpenCode2Client.layer.pipe(Layer.provide(httpClient))));

describe("OpenCode2Server.verifyServer", () => {
  it.effect("reports a rejected password, not a wrong server", () =>
    Effect.gen(function* () {
      const error = yield* verify(
        layerServerReplying({
          status: 401,
          contentType: "application/json",
          body: UNAUTHORIZED_BODY,
        }),
      ).pipe(Effect.flip);
      assert.include(error.detail, "rejected the server password");
    }),
  );

  // 1.x rejects a wrong password with an empty 401, which the client cannot decode.
  it.effect("reports an empty-body 401 as a rejected password", () =>
    Effect.gen(function* () {
      const error = yield* verify(layerServerReplying({ status: 401, body: "" })).pipe(Effect.flip);
      assert.include(error.detail, "rejected the server password");
    }),
  );

  it.effect("reports a server error as a server error, not as unreachable", () =>
    Effect.gen(function* () {
      for (const status of [500, 502]) {
        const error = yield* verify(
          layerServerReplying({ status, contentType: "text/plain", body: "upstream failed" }),
        ).pipe(Effect.flip);
        assert.include(error.detail, `returned HTTP ${status}`);
      }
    }),
  );

  it.effect("rejects a server that answers with the web UI's HTML", () =>
    Effect.gen(function* () {
      const error = yield* verify(
        layerServerReplying({ status: 200, contentType: "text/html", body: SPA_BODY }),
      ).pipe(Effect.flip);
      assert.include(error.detail, "is not an OpenCode 2 server");
    }),
  );
});

describe("OpenCode2Server error details", () => {
  const serverUrl = "http://user:url-secret@127.0.0.1:1/?token=query-secret";
  const detailFor = (httpClient: Layer.Layer<HttpClient.HttpClient>) =>
    Effect.gen(function* () {
      const server = yield* OpenCode2Server.make({
        binaryPath: "opencode",
        serverUrl,
        serverPassword: "secret",
        directory: "/project",
        environment: {},
      });
      const error = yield* server.withConnection(() => Effect.void).pipe(Effect.flip);
      return error.detail;
    }).pipe(
      Effect.provide(
        Layer.mergeAll(
          OpenCode2Client.layer.pipe(Layer.provide(httpClient)),
          OpenCodeRuntime.layer.pipe(Layer.provide(OpenCodeServerLedger.layerTest)),
        ).pipe(Layer.provideMerge(NodeServices.layer)),
      ),
    );

  it.effect("never include the server URL", () =>
    Effect.gen(function* () {
      const layerHanging = Layer.succeed(
        HttpClient.HttpClient,
        HttpClient.make(() => Effect.never),
      );
      const timedOut = yield* detailFor(layerHanging).pipe(Effect.forkChild);
      yield* TestClock.adjust("10 seconds");
      const details = [
        yield* detailFor(FetchHttpClient.layer),
        yield* detailFor(layerServerReplying({ status: 401, body: "" })),
        yield* detailFor(layerServerReplying({ status: 502, contentType: "text/plain", body: "" })),
        yield* detailFor(
          layerServerReplying({ status: 200, contentType: "text/html", body: SPA_BODY }),
        ),
        yield* Fiber.join(timedOut),
      ];
      assert.deepStrictEqual(details, [
        "Could not reach the OpenCode server.",
        "The OpenCode server rejected the server password.",
        "The OpenCode server returned HTTP 502.",
        "The server is not an OpenCode 2 server.",
        "Timed out waiting for the OpenCode server.",
      ]);
      for (const detail of details) {
        assert.notInclude(detail, "url-secret");
        assert.notInclude(detail, "query-secret");
        assert.notInclude(detail, "127.0.0.1");
      }
    }).pipe(Effect.provide(TestClock.layer())),
  );
});

describe("OpenCode2Server shared service", () => {
  it.effect("resolves the same registration file as the CLI for overridden homes", () =>
    Effect.gen(function* () {
      const path = yield* Path.Path;
      assert.equal(
        OpenCode2Server.serviceRegistrationFile(
          path,
          { XDG_STATE_HOME: "/state", HOME: "/other" },
          "/fallback",
        ),
        "/state/opencode/service.json",
      );
      assert.equal(
        OpenCode2Server.serviceRegistrationFile(path, { HOME: "/home/user" }, "/fallback"),
        "/home/user/.local/state/opencode/service.json",
      );
    }).pipe(Effect.provide(NodeServices.layer)),
  );

  it.live("reuses the CLI service and leaves it running after the T3 scope closes", () =>
    Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem;
      const path = yield* Path.Path;
      const directory = yield* fs.makeTempDirectoryScoped({ prefix: "t3-opencode-shared-" });
      const password = "private-native-service-password";
      const expected = "Basic " + Buffer.from("opencode:" + password).toString("base64");
      const http = NodeHttp.createServer((request, response) => {
        if (request.headers.authorization !== expected) {
          response.writeHead(401, { "content-type": "application/json" });
          response.end(UNAUTHORIZED_BODY);
          return;
        }
        response.writeHead(200, { "content-type": "application/json" });
        response.end(
          JSON.stringify({
            version: "2.0.28",
            pid: process.pid,
            urls: [],
            paths: { tmp: directory },
          }),
        );
      });
      yield* Effect.acquireRelease(
        Effect.promise(() => new Promise<void>((resolve) => http.listen(0, "127.0.0.1", resolve))),
        () =>
          Effect.promise(
            () =>
              new Promise<void>((resolve, reject) =>
                http.close((error) => (error ? reject(error) : resolve())),
              ),
          ),
      );
      const address = http.address();
      assert.isNotNull(address);
      assert.notEqual(typeof address, "string");
      if (address === null || typeof address === "string") return;
      const url = `http://127.0.0.1:${address.port}`;
      const environment = { ...(yield* HostProcess.Environment), XDG_STATE_HOME: directory };
      const registration = OpenCode2Server.serviceRegistrationFile(path, environment, directory);
      yield* fs.makeDirectory(path.dirname(registration), { recursive: true });
      yield* fs.writeFileString(
        registration,
        JSON.stringify({ id: "native-test", version: "2.0.28", url, pid: process.pid, password }),
        { mode: 0o600 },
      );
      yield* Effect.gen(function* () {
        const server = yield* OpenCode2Server.make({
          // Any attempt to spawn a private runtime would fail this test.
          binaryPath: "/does-not-exist/opencode",
          serverUrl: "",
          serverPassword: "",
          directory,
          environment,
        });
        const first = yield* server.withConnection((connection) => Effect.succeed(connection));
        const second = yield* server.withConnection((connection) => Effect.succeed(connection));
        assert.equal(first.version, "2.0.28");
        assert.equal(first.shared, true);
        assert.equal(first.external, false);
        assert.strictEqual(first.client, second.client);
      }).pipe(Effect.scoped);
      assert.isTrue(http.listening);
      const response = yield* HttpClient.get(`${url}/api/info`, {
        headers: { authorization: expected },
      });
      assert.equal(response.status, 200);
      assert.equal(
        yield* fs.readFileString(registration),
        JSON.stringify({ id: "native-test", version: "2.0.28", url, pid: process.pid, password }),
      );
    }).pipe(
      Effect.scoped,
      Effect.provide(
        OpenCode2Client.layer.pipe(
          Layer.provideMerge(FetchHttpClient.layer),
          Layer.provideMerge(NodeServices.layer),
        ),
      ),
    ),
  );
});
