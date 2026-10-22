import { expect, it } from "vite-plus/test";
import {
  AuthOrchestrationOperateScope,
  AuthOrchestrationReadScope,
  AuthSessionId,
} from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import { HttpRouter } from "effect/http";

import * as EnvironmentAuth from "../auth/EnvironmentAuth.ts";
import * as CodexNativeProxy from "./CodexNativeProxy.ts";
import * as NativeHttp from "./http.ts";

it.each([
  { scope: AuthOrchestrationReadScope, missing: false, status: 403, connects: 0 },
  { scope: AuthOrchestrationOperateScope, missing: true, status: 401, connects: 0 },
  { scope: AuthOrchestrationOperateScope, missing: false, status: 502, connects: 1 },
])(
  "authenticates before connecting and maps unavailable upstream to $status",
  async ({ scope, missing, status, connects }) => {
    let connections = 0;
    const { handler, dispose } = HttpRouter.toWebHandler(
      NativeHttp.layer.pipe(
        Layer.provideMerge(
          Layer.succeed(EnvironmentAuth.EnvironmentAuth, {
            authenticateWebSocketUpgrade: () =>
              missing
                ? Effect.fail(new EnvironmentAuth.ServerAuthMissingCredentialError({}))
                : Effect.succeed({
                    sessionId: AuthSessionId.make("test"),
                    subject: "test",
                    method: "bearer-access-token",
                    scopes: [scope],
                  }),
          } as unknown as EnvironmentAuth.EnvironmentAuth["Service"]),
        ),
        Layer.provideMerge(
          Layer.succeed(CodexNativeProxy.CodexNativeProxy, {
            connect: Effect.suspend(() => {
              connections++;
              return Effect.fail(new CodexNativeProxy.CodexNativeEndpointError({}));
            }),
            bridge: () => Effect.void,
          }),
        ),
      ),
      { disableLogger: true },
    );
    try {
      const response = await handler(
        new Request("http://t3.test/api/codex-native/ws", {
          headers: { upgrade: "websocket" },
        }),
      );
      expect(response.status).toBe(status);
      expect(connections).toBe(connects);
    } finally {
      await dispose();
    }
  },
);
