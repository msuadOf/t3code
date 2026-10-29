import type { ProviderInstanceId, ThreadId } from "@t3tools/contracts";
import type { McpProviderSessionConfig } from "@t3tools/provider-core/server/mcpSession";
import * as Effect from "effect/Effect";
import { NativeClaudeError } from "./NativeClaudeClient.ts";
import {
  NativeClaudeMcpBindings,
  CLAUDE_MCP_SESSION_METADATA_KEY,
} from "./NativeClaudeMcpBindings.ts";
import type { NativeClaudeSessions } from "./NativeClaudeSessions.ts";

/** UI history conversion installs only while there is no native daemon owner to replace. */
export const openNativeClaudeMcpHistory = Effect.fn("openNativeClaudeMcpHistory")(
  function* (input: {
    readonly sessionId: string;
    readonly instanceId: ProviderInstanceId;
    readonly options?: { readonly threadId?: ThreadId; readonly observeModelActivity?: boolean };
    readonly sessions: Pick<NativeClaudeSessions, "available" | "open">;
    readonly bindings: NativeClaudeMcpBindings;
    readonly readConfig: (
      threadId: ThreadId,
    ) => Effect.Effect<McpProviderSessionConfig | undefined>;
    readonly launcher: Parameters<NativeClaudeMcpBindings["prepare"]>[0]["launcher"];
  }) {
    const attempt = <A>(run: () => Promise<A>) =>
      Effect.tryPromise({
        try: run,
        catch: (cause) =>
          cause instanceof NativeClaudeError
            ? cause
            : new NativeClaudeError(
                "EMCPSHARING",
                "Claude native MCP history conversion failed; no prompt was sent.",
              ),
      });
    const candidate = (yield* attempt(() => input.sessions.available())).find(
      (session) => session.id === input.sessionId,
    );
    let launch:
      | {
          readonly config: McpProviderSessionConfig;
          readonly prepared: Awaited<ReturnType<NativeClaudeMcpBindings["prepare"]>>;
        }
      | undefined;
    if (candidate?.sharingState === "history" && input.options?.threadId !== undefined) {
      const config = yield* input.readConfig(input.options.threadId);
      if (!config)
        return yield* Effect.fail(
          new NativeClaudeError(
            "EMCPOWNER",
            "Claude history MCP credential is unavailable; the native worker was not started.",
          ),
        );
      if (
        config.threadId !== input.options.threadId ||
        config.providerInstanceId !== input.instanceId ||
        !config.bindNativeSession
      )
        return yield* Effect.fail(
          new NativeClaudeError(
            "EMCPOWNER",
            "Claude history MCP credential does not match its T3 owner.",
          ),
        );
      const owner = yield* attempt(() => input.bindings.ownership(input.sessionId));
      if (owner) {
        if (
          owner.revoked ||
          owner.threadId !== config.threadId ||
          owner.providerInstanceId !== config.providerInstanceId
        )
          return yield* Effect.fail(
            new NativeClaudeError(
              "EMCPOWNER",
              "Claude history MCP binding belongs to another owner or was revoked.",
            ),
          );
        if (
          !(yield* config.bindNativeSession({
            metadataKey: CLAUDE_MCP_SESSION_METADATA_KEY,
            sessionId: input.sessionId,
          }))
        )
          return yield* Effect.fail(
            new NativeClaudeError(
              "EMCPOWNER",
              "Claude history MCP credential could not bind its original session.",
            ),
          );
        yield* attempt(() => input.bindings.recover({ nativeSessionId: input.sessionId, config }));
        launch = { config, prepared: owner.prepared };
      } else {
        launch = {
          config,
          prepared: yield* attempt(() =>
            input.bindings.prepare({ config, launcher: input.launcher }),
          ),
        };
      }
    }
    const shared = yield* attempt(() =>
      input.sessions.open(input.sessionId, {
        ...(input.options?.observeModelActivity === undefined
          ? {}
          : { observeModelActivity: input.options.observeModelActivity }),
        ...(launch ? { mcpConfigPath: launch.prepared.mcpConfigPath } : {}),
      }),
    );
    if (shared.id !== input.sessionId)
      return yield* Effect.fail(
        new NativeClaudeError(
          "ESESSIONCOPY",
          "Claude reopened another native session; MCP was not bound.",
        ),
      );
    if (launch) {
      const { config, prepared } = launch;
      if (
        !config.bindNativeSession ||
        !(yield* config.bindNativeSession({
          metadataKey: CLAUDE_MCP_SESSION_METADATA_KEY,
          sessionId: shared.id,
        }))
      )
        return yield* Effect.fail(
          new NativeClaudeError(
            "EMCPOWNER",
            "Claude history MCP credential could not bind its actual native session.",
          ),
        );
      yield* attempt(() => input.bindings.bind({ prepared, nativeSessionId: shared.id, config }));
    }
    return shared;
  },
);
