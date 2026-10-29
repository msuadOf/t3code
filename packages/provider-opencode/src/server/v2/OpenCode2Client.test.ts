import { assert, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Redacted from "effect/Redacted";
import * as Stream from "effect/Stream";
import * as HttpClient from "effect/http/HttpClient";
import * as HttpClientResponse from "effect/http/HttpClientResponse";
import { describe } from "vite-plus/test";

import * as OpenCode2Client from "./OpenCode2Client.ts";

const INFO_BODY = '{"version":"2.0.18","pid":4242,"urls":[],"paths":{"tmp":"/tmp"}}';

const authorizationFor = (password: string | Redacted.Redacted) => {
  const seen: Array<string | undefined> = [];
  const layerCapturing = Layer.succeed(
    HttpClient.HttpClient,
    HttpClient.make((request) =>
      Effect.sync(() => {
        seen.push(request.headers.authorization);
        return HttpClientResponse.fromWeb(
          request,
          new Response(INFO_BODY, { headers: { "content-type": "application/json" } }),
        );
      }),
    ),
  );
  return Effect.gen(function* () {
    const opencode = yield* OpenCode2Client.OpenCode2Client;
    const { client } = yield* opencode.connect({ baseUrl: "http://127.0.0.1:4096", password });
    yield* client.server.info();
    return seen[0];
  }).pipe(Effect.provide(OpenCode2Client.layer.pipe(Layer.provide(layerCapturing))));
};

const utf8Basic = (password: string) =>
  `Basic ${Buffer.from(`opencode:${password}`, "utf8").toString("base64")}`;

describe("OpenCode2Client authorization", () => {
  // OpenCode decodes Basic credentials as UTF-8. Latin-1 (`btoa`) encoding gets
  // `pässwörd` rejected with a 401 and throws on characters such as `€`.
  it.effect("encodes non-ASCII passwords as UTF-8", () =>
    Effect.gen(function* () {
      assert.strictEqual(yield* authorizationFor("pässwörd"), utf8Basic("pässwörd"));
      assert.strictEqual(yield* authorizationFor("pass€word"), utf8Basic("pass€word"));
      assert.strictEqual(yield* authorizationFor(Redacted.make("pässwörd")), utf8Basic("pässwörd"));
    }),
  );
});

it.effect("preserves the missing-prefix marker through observer SSE decoding", () =>
  Effect.gen(function* () {
    const partial = {
      type: "t3.observer.text",
      data: {
        sessionID: "session",
        assistantMessageID: "message",
        ordinal: 0,
        kind: "text",
        text: "latest suffix",
        prefixMissing: true,
      },
    } as const;
    const http = HttpClient.make((request) =>
      Effect.succeed(
        HttpClientResponse.fromWeb(
          request,
          new Response(`data: ${JSON.stringify(partial)}\n\n`, {
            headers: { "content-type": "text/event-stream" },
          }),
        ),
      ),
    );
    const stream = yield* OpenCode2Client.observerEvents(http, {
      url: "http://127.0.0.1:4096",
      token: "test-only",
    });
    assert.deepEqual(yield* Stream.runCollect(stream), [partial]);
  }).pipe(Effect.scoped),
);
