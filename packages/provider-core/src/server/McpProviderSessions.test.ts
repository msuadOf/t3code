import { EnvironmentId, ProviderInstanceId, ThreadId } from "@t3tools/contracts";
import { expect, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import { McpProviderSessions, layer } from "./McpProviderSessions.ts";

it.effect("does not let cleanup of an old credential clear its replacement", () =>
  Effect.gen(function* () {
    const sessions = yield* McpProviderSessions;
    const config = {
      environmentId: EnvironmentId.make("test"),
      providerInstanceId: ProviderInstanceId.make("claude"),
      threadId: ThreadId.make("thread"),
      providerSessionId: "replacement",
      endpoint: "http://127.0.0.1/mcp",
      authorizationHeader: "Bearer fixture",
      browserToolsAvailable: false,
    };
    yield* sessions.set(config);
    yield* sessions.clear(config.threadId, "previous");
    expect(yield* sessions.read(config.threadId)).toEqual(config);
    yield* sessions.clear(config.threadId, config.providerSessionId);
    expect(yield* sessions.read(config.threadId)).toBeUndefined();
  }).pipe(Effect.provide(layer)),
);
