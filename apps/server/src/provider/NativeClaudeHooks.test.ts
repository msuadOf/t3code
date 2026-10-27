// @effect-diagnostics globalFetch:off -- simulate the native CLI HTTP connection and its abort signal.
import { expect, it } from "@effect/vitest";
import * as NodeServices from "@effect/platform-node/NodeServices";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Scope from "effect/Scope";
import * as PubSub from "effect/PubSub";
import { makeNativeClaudeHooks, type NativeClaudeHooks } from "./NativeClaudeHooks.ts";

const payload = {
  session_id: "11111111-2222-4333-8444-555555555555",
  hook_event_name: "PermissionRequest",
  tool_name: "Bash",
  tool_input: { command: "pwd" },
};
const waitPending = Effect.fn("test.waitPending")(function* (
  hooks: NativeClaudeHooks["Service"],
  count: number,
  changes: PubSub.Subscription<void>,
) {
  while (true) {
    const pending = yield* hooks.pending(payload.session_id);
    if (pending.length === count) return pending;
    yield* PubSub.take(changes);
  }
});
const post = (settings: string, signal?: AbortSignal, headers?: Record<string, string>) => {
  const hook = (
    JSON.parse(settings) as {
      hooks: { PermissionRequest: { hooks: { url: string; headers: Record<string, string> }[] }[] };
    }
  ).hooks.PermissionRequest[0]!.hooks[0]!;
  return fetch(hook.url, {
    method: "POST",
    headers: { "Content-Type": "application/json", ...(headers ?? hook.headers) },
    body: JSON.stringify(payload),
    ...(signal ? { signal } : {}),
  });
};

it.live(
  "keeps CLI-only requests local and accepts exactly one response for each unique callback",
  () =>
    Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem;
      const directory = yield* fs.makeTempDirectoryScoped();
      const hooks = yield* makeNativeClaudeHooks(directory);
      const changes = yield* hooks.subscribeChanges;
      const local = yield* Effect.promise(async () => (await post(hooks.settings)).json());
      expect(local).toEqual({});
      const scope = yield* Scope.Scope;
      yield* hooks.observe(payload.session_id, scope);
      const first = post(hooks.settings);
      const second = post(hooks.settings);
      const pending = yield* waitPending(hooks, 2, changes);
      expect(pending[0]!.id).not.toBe(pending[1]!.id);
      const response = {
        hookSpecificOutput: { hookEventName: "PermissionRequest", decision: { behavior: "allow" } },
      };
      expect(yield* hooks.answer(pending[0]!.id, response)).toBe(true);
      expect(yield* hooks.answer(pending[0]!.id, response)).toBe(false);
      expect(yield* hooks.answer(pending[1]!.id, {})).toBe(true);
      const results = yield* Effect.promise(() =>
        Promise.all([first, second].map(async (r) => (await r).json())),
      );
      expect(results).toEqual(expect.arrayContaining([response, {}]));
    }).pipe(Effect.scoped, Effect.provide(NodeServices.layer)),
);

it.live(
  "expires aborted native requests and preserves endpoint settings across broker restarts",
  () =>
    Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem;
      const directory = yield* fs.makeTempDirectoryScoped();
      const settings = yield* Effect.scoped(
        Effect.gen(function* () {
          const hooks = yield* makeNativeClaudeHooks(directory);
          const changes = yield* hooks.subscribeChanges;
          yield* hooks.observe(payload.session_id, yield* Scope.Scope);
          const controller = new AbortController();
          const request = post(hooks.settings, controller.signal).catch(() => undefined);
          const pending = yield* waitPending(hooks, 1, changes);
          controller.abort();
          yield* Effect.promise(() => request);
          yield* waitPending(hooks, 0, changes);
          expect(yield* hooks.answer(pending[0]!.id, {})).toBe(false);
          return hooks.settings;
        }),
      );
      const restored = yield* makeNativeClaudeHooks(directory);
      expect(restored.settings).toBe(settings);
      const rejected = yield* Effect.promise(() =>
        post(settings, undefined, { "X-T3-Native-Hook": "wrong" }),
      );
      expect(rejected.ok).toBe(false);
      expect(yield* restored.pending(payload.session_id)).toEqual([]);
    }).pipe(Effect.scoped, Effect.provide(NodeServices.layer)),
);
