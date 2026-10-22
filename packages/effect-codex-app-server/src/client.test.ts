import * as Deferred from "effect/Deferred";
import * as Queue from "effect/Queue";
import { makeInMemoryStdio } from "./_internal/stdio.ts";
import * as Exit from "effect/Exit";
import * as Fiber from "effect/Fiber";
import * as Layer from "effect/Layer";
import * as Path from "effect/Path";
import * as Effect from "effect/Effect";
import * as Ref from "effect/Ref";
import * as Scope from "effect/Scope";
import { ChildProcess, ChildProcessSpawner } from "effect/process";

import * as NodeServices from "@effect/platform-node/NodeServices";
import { assert, it } from "@effect/vitest";

import * as CodexClient from "./client.ts";

const mockPeerPath = Effect.map(Effect.service(Path.Path), (path) =>
  path.join(import.meta.dirname, "../test/fixtures/codex-app-server-mock-peer.ts"),
);
const mockPeerArgs = (path: string) => [path];

it.layer(NodeServices.layer)("effect-codex-app-server client", (it) => {
  const makeHandle = (env?: Record<string, string>) =>
    Effect.gen(function* () {
      const spawner = yield* ChildProcessSpawner.ChildProcessSpawner;
      const path = yield* Path.Path;
      const peerCwd = path.join(import.meta.dirname, "..");
      const command = ChildProcess.make(process.execPath, mockPeerArgs(yield* mockPeerPath), {
        cwd: peerCwd,
        ...(env ? { env: { ...process.env, ...env } } : {}),
      });
      return yield* spawner.spawn(command);
    });

  it.effect("passes native request IDs and leaves requests owned by other clients unanswered", () =>
    Effect.gen(function* () {
      const { stdio, input, output } = yield* makeInMemoryStdio();
      const client = yield* CodexClient.make(stdio);
      const observed = yield* Deferred.make<string | number>();
      const barrier = yield* Deferred.make<void>();
      yield* client.handleServerRequest("item/tool/requestUserInput", (_payload, requestId) =>
        Deferred.succeed(observed, requestId).pipe(Effect.as({ answers: {} })),
      );
      yield* client.handleServerNotification("serverRequest/resolved", () =>
        Deferred.succeed(barrier, undefined).pipe(Effect.asVoid),
      );
      const send = (value: unknown) =>
        Queue.offer(input, new TextEncoder().encode(`${JSON.stringify(value)}\n`));
      yield* send({
        id: "native-approval",
        method: "item/tool/requestUserInput",
        params: {
          isBlocking: true,
          itemId: "item-1",
          threadId: "thread-1",
          turnId: "turn-1",
          questions: [],
        },
      });
      assert.equal(yield* Deferred.await(observed), "native-approval");
      assert.deepEqual(JSON.parse(yield* Queue.take(output)), {
        id: "native-approval",
        result: { answers: {} },
      });
      yield* send({ id: "foreign-request", method: "other/client/request", params: {} });
      yield* send({
        id: "unregistered-known-request",
        method: "item/commandExecution/requestApproval",
        params: {},
      });
      yield* send({
        method: "serverRequest/resolved",
        params: { requestId: "foreign-request", threadId: "thread-1" },
      });
      yield* Deferred.await(barrier);
      yield* client.raw.notify("barrier");
      assert.deepEqual(JSON.parse(yield* Queue.take(output)), { method: "barrier" });
    }),
  );

  it.effect(
    "decodes resume replies while notification handlers wait, preserving notification order",
    () =>
      Effect.gen(function* () {
        const { stdio, input, output } = yield* makeInMemoryStdio();
        const client = yield* CodexClient.make(stdio);
        const reply = yield* Deferred.make<unknown>();
        const started = yield* Deferred.make<void>();
        const delivered = yield* Deferred.make<void>();
        const received: Array<string> = [];
        yield* client.handleUnknownServerNotification((_method, params) =>
          Effect.gen(function* () {
            const label = (params as { label: string }).label;
            if (label === "before-resume-reply") {
              yield* Deferred.succeed(started, undefined);
              yield* Deferred.await(reply);
            }
            received.push(label);
            if (label === "after-resume-reply") yield* Deferred.succeed(delivered, undefined);
          }),
        );
        const resume = yield* client.raw
          .request("thread/resume", { threadId: "native-thread" })
          .pipe(
            Effect.tap((result) => Deferred.succeed(reply, result)),
            Effect.forkScoped,
          );
        const request = JSON.parse(yield* Queue.take(output));
        const send = (value: unknown) =>
          Queue.offer(input, new TextEncoder().encode(`${JSON.stringify(value)}\n`));
        yield* send({ method: "test/notification", params: { label: "before-resume-reply" } });
        yield* Deferred.await(started);
        yield* send({ method: "test/notification", params: { label: "after-resume-reply" } });
        yield* send({ id: request.id, result: { thread: { id: "native-thread" } } });
        yield* Queue.end(input);
        assert.deepEqual(yield* Fiber.join(resume), { thread: { id: "native-thread" } });
        if (client.closed === undefined) return yield* Effect.die("Expected closure signal");
        yield* client.closed;
        assert.equal(yield* Deferred.isDone(delivered), true);
        assert.deepEqual(received, ["before-resume-reply", "after-resume-reply"]);
      }),
  );

  it.effect("fails a notification-owned pending RPC before draining EOF notifications", () =>
    Effect.gen(function* () {
      const { stdio, input, output } = yield* makeInMemoryStdio();
      const client = yield* CodexClient.make(stdio);
      const processed = yield* Deferred.make<void>();
      yield* client.handleUnknownServerNotification(() =>
        client.raw.request("thread/read", { threadId: "native-thread" }).pipe(
          Effect.exit,
          Effect.flatMap((exit) => {
            assert.equal(Exit.isFailure(exit), true);
            return Deferred.succeed(processed, undefined).pipe(Effect.asVoid);
          }),
        ),
      );
      yield* Queue.offer(input, new TextEncoder().encode('{"method":"test/notification"}\n'));
      const request = JSON.parse(yield* Queue.take(output));
      assert.equal(request.method, "thread/read");
      yield* Queue.end(input);
      if (client.closed === undefined) return yield* Effect.die("Expected closure signal");
      yield* client.closed;
      assert.equal(yield* Deferred.isDone(processed), true);
    }),
  );

  it.effect("exposes termination even when no request is pending", () =>
    Effect.gen(function* () {
      const first = yield* makeInMemoryStdio();
      const client = yield* CodexClient.make(first.stdio);
      if (client.closed === undefined)
        return yield* Effect.die("Production clients expose transport closure");
      yield* Queue.end(first.input);
      const error = yield* client.closed;
      assert.equal(error._tag, "CodexAppServerInputStreamEndedError");
      const next = yield* makeInMemoryStdio();
      const replacement = yield* CodexClient.make(next.stdio);
      assert.notStrictEqual(replacement, client);
      const request = replacement.raw.request("probe").pipe(Effect.forkScoped);
      const pending = yield* request;
      const outbound = JSON.parse(yield* Queue.take(next.output));
      yield* Queue.offer(
        next.input,
        new TextEncoder().encode(`${JSON.stringify({ id: outbound.id, result: "reconnected" })}\n`),
      );
      assert.equal(yield* Fiber.join(pending), "reconnected");
    }),
  );

  it.effect("initializes, handles typed server requests, and reads account and skills data", () =>
    Effect.gen(function* () {
      const userInputRequests = yield* Ref.make<Array<unknown>>([]);
      const messageDeltas = yield* Ref.make<Array<unknown>>([]);
      const handle = yield* makeHandle();
      const scope = yield* Scope.make();
      const layerClient = CodexClient.layerChildProcess(handle);
      const context = yield* Layer.buildWithScope(layerClient, scope);

      const result = yield* Effect.gen(function* () {
        const client = yield* CodexClient.CodexAppServerClient;

        yield* client.handleServerRequest("item/tool/requestUserInput", (payload) =>
          Ref.update(userInputRequests, (current) => [...current, payload]).pipe(
            Effect.as({
              answers: {
                approved: {
                  answers: ["yes"],
                },
              },
            }),
          ),
        );

        yield* client.handleServerNotification("item/agentMessage/delta", (payload) =>
          Ref.update(messageDeltas, (current) => [...current, payload]),
        );

        const initialized = yield* client.request("initialize", {
          clientInfo: {
            name: "effect-codex-app-server-test",
            title: "Effect Codex App Server Test",
            version: "0.0.0",
          },
          capabilities: {
            experimentalApi: true,
            optOutNotificationMethods: null,
          },
        });
        assert.equal(initialized.userAgent, "mock-codex-app-server");

        yield* client.notify("initialized", undefined);

        const account = yield* client.request("account/read", {});
        assert.equal(account.requiresOpenaiAuth, false);
        assert.deepEqual(account.account, {
          type: "chatgpt",
          email: "mock@example.com",
          planType: "plus",
        });

        const path = yield* Path.Path;
        const peerCwd = path.join(import.meta.dirname, "..");
        const skills = yield* client.request("skills/list", { cwds: [peerCwd] });
        assert.equal(skills.data.length, 1);
        assert.equal(skills.data[0]?.cwd, peerCwd);

        return {
          account,
          skills,
        };
      }).pipe(Effect.provide(context), Effect.ensuring(Scope.close(scope, Exit.void)));

      assert.equal(result.skills.data[0]?.skills.length, 0);
      assert.deepEqual(yield* Ref.get(userInputRequests), [
        {
          isBlocking: true,
          itemId: "item-approval-1",
          threadId: "thread-1",
          turnId: "turn-1",
          questions: [
            {
              id: "approved",
              header: "Approve",
              question: "Continue with the mock skills request?",
              options: [
                {
                  label: "yes",
                  description: "Approve the request",
                },
              ],
            },
          ],
        },
      ]);
      assert.deepEqual(yield* Ref.get(messageDeltas), [
        {
          delta: "Mock server is ready.",
          itemId: "item-1",
          threadId: "thread-1",
          turnId: "turn-1",
        },
      ]);
    }),
  );
  it.effect("drains child stderr so large diagnostics cannot block protocol responses", () =>
    Effect.gen(function* () {
      const handle = yield* makeHandle({
        CODEX_APP_SERVER_TEST_STDERR_BYTES: String(512 * 1024),
      });
      const scope = yield* Scope.make();
      const layerClient = CodexClient.layerChildProcess(handle);
      const context = yield* Layer.buildWithScope(layerClient, scope);

      const initialized = yield* Effect.gen(function* () {
        const client = yield* CodexClient.CodexAppServerClient;
        return yield* client.request("initialize", {
          clientInfo: {
            name: "effect-codex-app-server-test",
            title: "Effect Codex App Server Test",
            version: "0.0.0",
          },
          capabilities: {
            experimentalApi: true,
            optOutNotificationMethods: null,
          },
        });
      }).pipe(
        Effect.timeout("5 seconds"),
        Effect.provide(context),
        Effect.ensuring(Scope.close(scope, Exit.void)),
      );

      assert.equal(initialized.userAgent, "mock-codex-app-server");
    }),
  );
});
