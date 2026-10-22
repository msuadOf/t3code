import { AuthOrchestrationOperateScope } from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import { HttpRouter, HttpServerRequest, HttpServerResponse } from "effect/http";

import * as EnvironmentAuth from "../auth/EnvironmentAuth.ts";
import {
  failEnvironmentAuthInvalid,
  failEnvironmentInternal,
  failEnvironmentScopeRequired,
} from "../auth/http.ts";
import * as CodexNativeProxy from "./CodexNativeProxy.ts";

const handler = Effect.gen(function* () {
  const request = yield* HttpServerRequest.HttpServerRequest;
  const auth = yield* EnvironmentAuth.EnvironmentAuth;
  const session = yield* auth.authenticateWebSocketUpgrade(request).pipe(
    Effect.catchIf(EnvironmentAuth.isServerAuthCredentialError, (error) =>
      failEnvironmentAuthInvalid(
        EnvironmentAuth.serverAuthCredentialReason(error),
        EnvironmentAuth.serverAuthDpopFailureReason(error),
      ),
    ),
    Effect.catchIf(EnvironmentAuth.isServerAuthInternalError, (error) =>
      failEnvironmentInternal("internal_error", error),
    ),
  );
  if (!session.scopes.includes(AuthOrchestrationOperateScope)) {
    return yield* failEnvironmentScopeRequired(AuthOrchestrationOperateScope);
  }
  const proxy = yield* CodexNativeProxy.CodexNativeProxy;
  return yield* Effect.scoped(
    Effect.gen(function* () {
      const upstream = yield* proxy.connect.pipe(
        Effect.map((connection) => ({ connection })),
        Effect.orElseSucceed(() => ({ connection: undefined })),
      );
      if (!upstream.connection) {
        return HttpServerResponse.text(
          "Cannot connect to the native Codex server. Start a Codex app-server daemon and configure T3CODE_CODEX_NATIVE_ENDPOINT if needed.",
          { status: 502 },
        );
      }
      const client = yield* request.upgrade;
      yield* proxy.bridge(client, upstream.connection).pipe(Effect.ignore);
      return HttpServerResponse.empty();
    }),
  );
});

export const layer = HttpRouter.add("GET", "/api/codex-native/ws", handler);
