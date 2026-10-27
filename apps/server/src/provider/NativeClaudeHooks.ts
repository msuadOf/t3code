// @effect-diagnostics nodeBuiltinImport:off -- the native CLI calls a private loopback HTTP transport.
import * as NodeHttp from "node:http";
import * as NodeCrypto from "node:crypto";
import * as Context from "effect/Context";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Path from "effect/Path";
import * as Schema from "effect/Schema";
import * as Scope from "effect/Scope";
import * as PubSub from "effect/PubSub";

const decodeJson = Schema.decodeUnknownSync(Schema.fromJsonString(Schema.Unknown));
const encodeJson = Schema.encodeSync(Schema.fromJsonString(Schema.Unknown));

type JsonObject = Record<string, unknown>;
const object = (value: unknown): value is JsonObject =>
  typeof value === "object" && value !== null && !Array.isArray(value);

export interface NativeClaudeHookRequest {
  readonly id: string;
  readonly sessionId: string;
  readonly event: "PreToolUse" | "PermissionRequest";
  readonly toolName: string;
  readonly toolUseId?: string;
  readonly input: JsonObject;
}

export class NativeClaudeHookError extends Schema.TaggedError<NativeClaudeHookError>()(
  "NativeClaudeHookError",
  { detail: Schema.String },
) {}

export class NativeClaudeHooks extends Context.Service<
  NativeClaudeHooks,
  {
    readonly settings: string;
    readonly settingsPath: string;
    readonly subscribeChanges: Effect.Effect<PubSub.Subscription<void>, never, Scope.Scope>;
    readonly observe: (sessionId: string, scope: Scope.Scope) => Effect.Effect<void>;
    readonly pending: (sessionId: string) => Effect.Effect<readonly NativeClaudeHookRequest[]>;
    readonly answer: (id: string, response: JsonObject) => Effect.Effect<boolean>;
  }
>()("t3/provider/NativeClaudeHooks") {}

/** Each provider keeps a stable private endpoint so a server restart needs no worker restart. */
export const makeNativeClaudeHooks = Effect.fn("NativeClaudeHooks.make")(function* (
  directory: string,
) {
  const fs = yield* FileSystem.FileSystem;
  const path = yield* Path.Path;
  yield* fs.makeDirectory(directory, { recursive: true, mode: 0o700 });
  const endpointPath = path.join(directory, "endpoint.json");
  const settingsPath = path.join(directory, "settings.json");
  let port = 0;
  let key = NodeCrypto.randomBytes(32).toString("hex");
  if (yield* fs.exists(endpointPath)) {
    const raw = yield* fs.readFileString(endpointPath);
    const saved = yield* Effect.try({
      try: () => decodeJson(raw) as unknown,
      catch: () => new NativeClaudeHookError({ detail: "Invalid native hook endpoint metadata." }),
    });
    if (
      !object(saved) ||
      typeof saved.key !== "string" ||
      !/^[a-f0-9]{64}$/.test(saved.key) ||
      typeof saved.port !== "number" ||
      !Number.isInteger(saved.port) ||
      saved.port < 1 ||
      saved.port > 65535
    )
      return yield* new NativeClaudeHookError({ detail: "Invalid native hook endpoint metadata." });
    port = saved.port;
    key = saved.key;
  }
  const changes = yield* PubSub.unbounded<void>();
  yield* Effect.addFinalizer(() => PubSub.shutdown(changes));
  const observers = new Map<string, number>();
  const requests = new Map<
    string,
    {
      request: NativeClaudeHookRequest;
      reply: Deferred.Deferred<JsonObject>;
    }
  >();
  const receive = Effect.fn("NativeClaudeHooks.receive")(function* (
    authorization: string,
    payload: unknown,
  ) {
    const presented = Buffer.from(authorization);
    const expected = Buffer.from(key);
    if (presented.length !== expected.length || !NodeCrypto.timingSafeEqual(presented, expected))
      return yield* new NativeClaudeHookError({ detail: "Unauthorized native hook." });
    if (
      !object(payload) ||
      typeof payload.session_id !== "string" ||
      typeof payload.tool_name !== "string" ||
      !object(payload.tool_input) ||
      (payload.hook_event_name !== "PermissionRequest" &&
        !(payload.hook_event_name === "PreToolUse" && payload.tool_name === "AskUserQuestion"))
    )
      return yield* new NativeClaudeHookError({ detail: "Unsupported native hook input." });
    // An installed hook must leave CLI-only sessions usable when no T3 runtime observes them.
    if (!observers.has(payload.session_id)) return {};
    const request: NativeClaudeHookRequest = {
      id: NodeCrypto.randomUUID(),
      sessionId: payload.session_id,
      event: payload.hook_event_name,
      toolName: payload.tool_name,
      input: payload.tool_input,
      ...(typeof payload.tool_use_id === "string" ? { toolUseId: payload.tool_use_id } : {}),
    };
    const reply = yield* Deferred.make<JsonObject>();
    requests.set(request.id, { request, reply });
    yield* PubSub.publish(changes, undefined);
    return yield* Deferred.await(reply).pipe(
      Effect.ensuring(
        Effect.sync(() => requests.delete(request.id)).pipe(
          Effect.andThen(PubSub.publish(changes, undefined)),
        ),
      ),
    );
  });
  const context = yield* Effect.context<never>();
  const run = Effect.runPromiseWith(context);
  const server = NodeHttp.createServer((request, response) => {
    const controller = new AbortController();
    response.once("close", () => {
      if (!response.writableEnded) controller.abort();
    });
    const handle = async () => {
      if (request.method !== "POST" || request.url !== "/hook") {
        response.writeHead(404).end();
        return;
      }
      let size = 0;
      const chunks: Buffer[] = [];
      for await (const chunk of request) {
        const bytes = Buffer.from(chunk);
        size += bytes.length;
        if (size > 1024 * 1024) throw new Error("Hook input exceeds the supported size.");
        chunks.push(bytes);
      }
      const payload: unknown = decodeJson(Buffer.concat(chunks).toString("utf8"));
      const result = await run(
        receive(String(request.headers["x-t3-native-hook"] ?? ""), payload),
        { signal: controller.signal },
      );
      response.writeHead(200, { "Content-Type": "application/json" }).end(encodeJson(result));
    };
    void handle().catch(() => {
      if (!response.destroyed && !response.writableEnded)
        response.writeHead(400, { "Content-Type": "application/json" }).end("{}");
    });
  });
  yield* Effect.acquireRelease(
    Effect.tryPromise({
      try: () =>
        new Promise<void>((resolve, reject) => {
          server.once("error", reject);
          server.listen(port, "127.0.0.1", () => {
            server.off("error", reject);
            resolve();
          });
        }),
      catch: () =>
        new NativeClaudeHookError({
          detail:
            "Could not bind the private Claude hook endpoint. Another T3 instance may own it.",
        }),
    }),
    () =>
      Effect.gen(function* () {
        for (const pending of requests.values()) yield* Deferred.succeed(pending.reply, {});
        yield* Effect.promise(
          () =>
            new Promise<void>((resolve) => {
              server.close(() => resolve());
              server.closeAllConnections();
            }),
        );
      }),
  );
  const address = server.address();
  if (!address || typeof address === "string")
    return yield* new NativeClaudeHookError({ detail: "Claude hook endpoint has no TCP address." });
  const handler = {
    type: "http",
    url: `http://127.0.0.1:${address.port}/hook`,
    headers: { "X-T3-Native-Hook": key },
    timeout: 600,
    onFailure: "continue",
  };
  const settings = encodeJson({
    hooks: {
      PreToolUse: [{ matcher: "AskUserQuestion", hooks: [handler] }],
      PermissionRequest: [{ hooks: [handler] }],
    },
  });
  yield* fs.writeFileString(endpointPath, encodeJson({ port: address.port, key }), {
    mode: 0o600,
  });
  yield* fs.writeFileString(settingsPath, settings, { mode: 0o600 });
  return NativeClaudeHooks.of({
    settings,
    settingsPath,
    subscribeChanges: PubSub.subscribe(changes),
    observe: Effect.fn("NativeClaudeHooks.observe")(function* (sessionId, scope) {
      yield* Effect.acquireRelease(
        Effect.sync(() => observers.set(sessionId, (observers.get(sessionId) ?? 0) + 1)),
        () =>
          Effect.gen(function* () {
            const count = (observers.get(sessionId) ?? 1) - 1;
            if (count > 0) {
              observers.set(sessionId, count);
              return;
            }
            observers.delete(sessionId);
            for (const pending of requests.values()) {
              if (pending.request.sessionId === sessionId)
                yield* Deferred.succeed(pending.reply, {});
            }
          }),
      ).pipe(Effect.provideService(Scope.Scope, scope));
    }),
    pending: Effect.fn("NativeClaudeHooks.pending")((sessionId) =>
      Effect.sync(() =>
        [...requests.values()]
          .filter((value) => value.request.sessionId === sessionId)
          .map((value) => value.request),
      ),
    ),
    answer: Effect.fn("NativeClaudeHooks.answer")(function* (id, response) {
      const pending = requests.get(id);
      if (!pending) return false;
      // Remove synchronously before waking the callback: two frontends cannot both win.
      requests.delete(id);
      return yield* Deferred.succeed(pending.reply, response);
    }),
  });
});
