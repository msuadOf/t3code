import * as NodeServices from "@effect/platform-node/NodeServices";
import { NodeWS } from "@effect/platform-node/NodeSocket";
import * as Effect from "effect/Effect";
import * as ChildProcessSpawner from "effect/process/ChildProcessSpawner";
import { withCodexAppServerClient, probeCodexSkillsForCwd } from "./CodexProvider.ts";
import { assert, it } from "@effect/vitest";

import { applyPreferredCodexDefaultModel, mapCodexModelCapabilities } from "./CodexProvider.ts";

it("maps current Codex model capability fields", () => {
  const capabilities = mapCodexModelCapabilities({
    additionalSpeedTiers: [],
    defaultReasoningEffort: "super-high",
    description: "Test model",
    displayName: "GPT Test",
    hidden: false,
    id: "gpt-test",
    isDefault: true,
    model: "gpt-test",
    defaultServiceTier: "flex",
    serviceTiers: [
      {
        id: "priority",
        name: "Fast",
        description: "Lower latency responses.",
      },
      {
        id: "flex",
        name: "Flex",
        description: "Lower-cost asynchronous routing.",
      },
    ],
    supportedReasoningEfforts: [
      {
        description: "Maximum reasoning",
        reasoningEffort: "super-high",
      },
    ],
  });

  assert.deepStrictEqual(capabilities.optionDescriptors, [
    {
      id: "reasoningEffort",
      label: "Reasoning",
      type: "select",
      options: [{ id: "super-high", label: "super-high", isDefault: true }],
      currentValue: "super-high",
    },
    {
      id: "serviceTier",
      label: "Service Tier",
      type: "select",
      options: [
        { id: "default", label: "Standard" },
        {
          id: "priority",
          label: "Fast",
          description: "Lower latency responses.",
        },
        {
          id: "flex",
          label: "Flex",
          description: "Lower-cost asynchronous routing.",
          isDefault: true,
        },
      ],
      currentValue: "flex",
    },
  ]);
});

it("uses standard routing when the catalog has no default service tier", () => {
  const capabilities = mapCodexModelCapabilities({
    additionalSpeedTiers: ["fast"],
    defaultReasoningEffort: "medium",
    defaultServiceTier: null,
    description: "Test model",
    displayName: "GPT Test",
    hidden: false,
    id: "gpt-test",
    isDefault: true,
    model: "gpt-test",
    serviceTiers: [
      {
        id: "priority",
        name: "Fast",
        description: "1.5x speed, increased usage",
      },
      {
        id: "ultrafast",
        name: "Ultrafast",
        description: "The fastest available responses for latency-sensitive work.",
      },
    ],
    supportedReasoningEfforts: [],
  });

  assert.deepStrictEqual(capabilities.optionDescriptors, [
    {
      id: "serviceTier",
      label: "Service Tier",
      type: "select",
      options: [
        { id: "default", label: "Standard", isDefault: true },
        {
          id: "priority",
          label: "Fast",
          description: "1.5x speed, increased usage",
        },
        {
          id: "ultrafast",
          label: "Ultrafast",
          description: "Even faster, more expensive",
        },
      ],
      currentValue: "default",
    },
  ]);
});

it("marks the most preferred available model as default", () => {
  const models = applyPreferredCodexDefaultModel([
    { slug: "gpt-5.6-terra", name: "GPT-5.6-Terra", isCustom: false, capabilities: null },
    { slug: "gpt-5.4", name: "GPT-5.4", isCustom: false, isDefault: true, capabilities: null },
  ]);

  assert.deepStrictEqual(
    models.map((model) => ({ slug: model.slug, isDefault: model.isDefault })),
    [
      { slug: "gpt-5.6-terra", isDefault: true },
      { slug: "gpt-5.4", isDefault: undefined },
    ],
  );
});

it("ranks qualified Codex models while preserving their wire ids", () => {
  const models = applyPreferredCodexDefaultModel([
    {
      slug: "openai.gpt-5.6-luna",
      name: "Luna",
      isCustom: false,
      isDefault: true,
      capabilities: null,
    },
    { slug: "openai.gpt-5.6-sol", name: "Sol", isCustom: false, capabilities: null },
  ]);
  assert.deepStrictEqual(
    models.filter((model) => model.isDefault).map((model) => model.slug),
    ["openai.gpt-5.6-sol"],
  );
});

it("ignores custom models that shadow a preferred slug", () => {
  const models = applyPreferredCodexDefaultModel([
    { slug: "gpt-5.6-sol", name: "gpt-5.6-sol", isCustom: true, capabilities: null },
    { slug: "gpt-5.4", name: "GPT-5.4", isCustom: false, isDefault: true, capabilities: null },
  ]);

  assert.deepStrictEqual(models.find((model) => model.isDefault)?.slug, "gpt-5.4");
});

it.live(
  "reads account, skills and reset credits through the configured shared daemon without a private CLI",
  () =>
    Effect.gen(function* () {
      const methods: string[] = [];
      let connections = 0;
      const server = yield* Effect.acquireRelease(
        Effect.sync(() => new NodeWS.WebSocketServer({ port: 0, host: "127.0.0.1" })),
        (server) =>
          Effect.promise(
            () =>
              new Promise<void>((resolve) => {
                for (const peer of server.clients) peer.terminate();
                server.close(() => resolve());
              }),
          ),
      );
      if (server.address() === null)
        yield* Effect.callback<void>((resume) => {
          server.once("listening", () => resume(Effect.void));
        });
      const address = server.address();
      if (address === null || typeof address === "string")
        return yield* Effect.die("Fixture port unavailable");
      server.on("connection", (peer) => {
        connections += 1;
        peer.on("message", (raw) => {
          const request = JSON.parse(raw.toString()) as { id?: number; method: string };
          methods.push(request.method);
          if (request.id === undefined) return;
          const result =
            request.method === "initialize"
              ? {
                  userAgent: "codex/0.164.0",
                  codexHome: "/same-cli-home",
                  platformFamily: "unix",
                  platformOs: "linux",
                }
              : request.method === "account/read"
                ? {
                    account: { type: "chatgpt", email: "cli@example.test", planType: "plus" },
                    requiresOpenaiAuth: false,
                  }
                : request.method === "account/rateLimitResetCredit/consume"
                  ? { outcome: "reset" }
                  : {
                      data: [
                        {
                          cwd: "/repo",
                          errors: [],
                          skills: [
                            {
                              name: "shared-skill",
                              path: "/same-cli-home/skills/shared/SKILL.md",
                              description: "Shared CLI skill",
                              enabled: true,
                              scope: "user",
                            },
                          ],
                        },
                      ],
                    };
          peer.send(JSON.stringify({ id: request.id, result }));
        });
      });
      const input = {
        binaryPath: "/must-not-spawn-private-codex",
        daemonEndpoint: `ws://127.0.0.1:${address.port}`,
        cwd: "/repo",
        environment: { CODEX_HOME: "/same-cli-home" },
      };
      yield* Effect.scoped(
        Effect.gen(function* () {
          const { client, initialize } = yield* withCodexAppServerClient(input);
          assert.equal(initialize.codexHome, "/same-cli-home");
          const account = yield* client.request("account/read", { refreshToken: false });
          assert.equal(account.account?.type, "chatgpt");
          const reset = yield* client.request("account/rateLimitResetCredit/consume", {
            idempotencyKey: "fixture-reset",
          });
          assert.equal(reset.outcome, "reset");
        }),
      );
      const skills = yield* Effect.scoped(probeCodexSkillsForCwd(input));
      assert.equal(skills[0]?.name, "shared-skill");
      assert.equal(connections, 2);
      assert.equal(methods.includes("thread/start") || methods.includes("turn/start"), false);
    }).pipe(
      Effect.scoped,
      Effect.provide(NodeServices.layer),
      Effect.provideService(
        ChildProcessSpawner.ChildProcessSpawner,
        ChildProcessSpawner.make(() => Effect.die("Metadata must not spawn a private app-server")),
      ),
    ),
);
