import { assert, it } from "@effect/vitest";
import {
  ProviderDriverKind,
  ProviderInstanceId,
  ProviderSessionId,
  ProviderThreadId,
  ThreadId,
  ProjectId,
  RunId,
  type OrchestrationV2DomainEvent,
  type OrchestrationV2ThreadProjection,
} from "@t3tools/contracts";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as IdAllocator from "@t3tools/provider-core/server/IdAllocator";
import type * as ProviderAdapter from "@t3tools/provider-core/server/ProviderAdapter";
import * as ExternalNativeRuns from "./ExternalNativeRuns.ts";
import * as ProjectionStore from "./ProjectionStore.ts";
import * as EventSink from "./EventSink.ts";
import * as RunExecutionService from "./RunExecutionService.ts";

it.effect.each(["codex", "claudeAgent", "opencode"] as const)(
  "%s: persists an external run before observing it and reuses its identity on duplicate notifications",
  (provider) =>
    Effect.gen(function* () {
      const now = yield* DateTime.now;
      const threadId = ThreadId.make("thread:external-adoption");
      const instanceId = ProviderInstanceId.make(`${provider}-primary`);
      const providerThreadId = ProviderThreadId.make("provider-thread:external-adoption");
      const driver = ProviderDriverKind.make(provider);
      let projection = {
        thread: {
          id: threadId,
          projectId: ProjectId.make("project:external-adoption"),
          modelSelection: { instanceId, model: "gpt-5.4" },
          runtimeMode: "approval-required",
          interactionMode: "default",
        },
        providerThreads: [
          {
            id: providerThreadId,
            driver,
            providerInstanceId: instanceId,
            nativeThreadRef: { nativeId: "native-thread" },
          },
        ],
        runs: [],
        attempts: [],
        nodes: [],
        providerTurns: [],
        checkpointScopes: [],
      } as unknown as OrchestrationV2ThreadProjection;
      const writes: Array<OrchestrationV2DomainEvent> = [];
      const observed: Array<RunExecutionService.RunExecutionServiceV2StartRootRunInput> = [];
      const session = {
        driver,
        instanceId,
        providerSessionId: ProviderSessionId.make("session:external-adoption"),
        providerSession: { cwd: "/workspace" },
      } as ProviderAdapter.ProviderAdapterV2SessionRuntime;
      const layer = ExternalNativeRuns.layer.pipe(
        Layer.provide(
          Layer.mergeAll(
            IdAllocator.layer,
            Layer.mock(ProjectionStore.ProjectionStoreV2)({
              getThreadProjection: () => Effect.sync(() => projection),
            }),
            Layer.mock(EventSink.EventSinkV2)({
              writeIfRunCurrent: (input) =>
                Effect.sync(() => {
                  assert.equal(input.expectedStatus, "starting");
                  writes.push(...input.events);
                  return { committed: true, storedEvents: [] };
                }),
              write: ({ events }) =>
                Effect.sync(() => {
                  writes.push(...events);
                  for (const event of events) {
                    if (event.type === "run.created")
                      projection = { ...projection, runs: projection.runs.concat(event.payload) };
                    if (event.type === "run-attempt.created")
                      projection = {
                        ...projection,
                        attempts: projection.attempts.concat(event.payload),
                      };
                    if (event.type === "node.updated")
                      projection = {
                        ...projection,
                        nodes: projection.nodes.concat(event.payload),
                      };
                    if (event.type === "checkpoint-scope.created")
                      projection = {
                        ...projection,
                        checkpointScopes: projection.checkpointScopes.concat(event.payload),
                      };
                  }
                  return [];
                }),
            }),
            Layer.mock(RunExecutionService.RunExecutionServiceV2)({
              observeExternalRun: (input) =>
                Effect.sync(() => {
                  assert.isTrue(
                    writes.some(
                      (event) => event.type === "run.created" && event.payload.id === input.run.id,
                    ),
                  );
                  observed.push(input);
                }),
            }),
          ),
        ),
      );
      yield* Effect.gen(function* () {
        const runs = yield* ExternalNativeRuns.ExternalNativeRuns;
        const input = {
          threadId,
          nativeThreadId: "native-thread",
          nativeTurnId: "cli-turn",
          startedAt: now,
          text: "From the CLI",
          session,
        };
        const first = yield* runs.adopt(input);
        const duplicate = yield* runs.adopt(input);
        assert.isDefined(first);
        assert.equal(first?.runId, duplicate?.runId);
        assert.equal(
          first?.runId,
          `run:${provider}-external:${encodeURIComponent(providerThreadId)}:cli-turn`,
        );
        assert.equal(
          first?.message.messageId,
          `message:${provider}-external:${encodeURIComponent(String(first?.runId))}`,
        );
        assert.equal(first?.providerThread.id, providerThreadId);
        assert.equal(first?.message.creationSource, "provider");
        assert.equal(observed.length, 1);
        assert.equal(writes.filter((event) => event.type === "run.created").length, 1);
        assert.isUndefined(yield* runs.adopt({ ...input, nativeThreadId: "unrelated-thread" }));
        assert.isUndefined(
          yield* runs.adopt({
            ...input,
            session: { ...session, instanceId: ProviderInstanceId.make(`${provider}-other`) },
          }),
        );
        assert.isUndefined(
          yield* runs.adopt({
            ...input,
            session: {
              ...session,
              driver: ProviderDriverKind.make(provider === "codex" ? "opencode" : "codex"),
            },
          }),
        );
        projection = {
          ...projection,
          runs: projection.runs.map((run) => ({ ...run, status: "completed" })),
        };
        assert.isUndefined(yield* runs.adopt(input));
        assert.equal(observed.length, 1);
        yield* observed[0]?.onObservationClosed ?? Effect.void;
        const revived = yield* runs.adopt({
          ...input,
          snapshot: true,
          nativeStatus: "inProgress",
        });
        assert.equal(revived?.runId, first?.runId);
        assert.equal(observed.length, 2);
        assert.isTrue(
          writes.some(
            (event) =>
              event.type === "run.updated" &&
              event.payload.id === first?.runId &&
              event.payload.status === "running",
          ),
        );
        assert.equal(writes.filter((event) => event.type === "run.created").length, 1);
        const recoveredRun = projection.runs[0];
        if (recoveredRun === undefined) return yield* Effect.die("Expected the adopted run");
        projection = {
          ...projection,
          runs: [
            {
              ...recoveredRun,
              status: "starting",
              requestedAt: DateTime.subtract(now, { seconds: 1 }),
            },
            {
              ...recoveredRun,
              id: RunId.make("run:current-process-input"),
              status: "starting",
              requestedAt: now,
            },
          ],
          attempts: projection.attempts.map((attempt) => ({ ...attempt, providerTurnId: null })),
        };
        const beforeRecovery = writes.length;
        yield* runs.reconcileSnapshot({
          threadId,
          nativeThreadId: "native-thread",
          driver,
          providerInstanceId: instanceId,
          idle: false,
          turns: [],
        });
        yield* runs.reconcileSnapshot({
          threadId,
          nativeThreadId: "native-thread",
          driver,
          providerInstanceId: instanceId,
          idle: true,
          turns: [{ id: "still-running", status: "inProgress" }],
        });
        assert.equal(writes.length, beforeRecovery);
        yield* runs.reconcileSnapshot({
          threadId,
          nativeThreadId: "native-thread",
          driver,
          providerInstanceId: instanceId,
          idle: true,
          turns: [{ id: "still-running", status: "running" }],
        });
        for (const mismatch of [
          {
            driver: ProviderDriverKind.make(provider === "codex" ? "opencode" : "codex"),
            providerInstanceId: instanceId,
          },
          { driver, providerInstanceId: ProviderInstanceId.make(`${provider}-other`) },
        ]) {
          yield* runs.reconcileSnapshot({
            threadId,
            nativeThreadId: "native-thread",
            idle: true,
            turns: [],
            ...mismatch,
          });
        }
        assert.equal(writes.length, beforeRecovery);
        yield* runs.reconcileSnapshot({
          threadId,
          nativeThreadId: "native-thread",
          driver,
          providerInstanceId: instanceId,
          idle: true,
          turns: [],
        });
        const recovered = writes.slice(beforeRecovery);
        assert.isTrue(
          recovered.some(
            (event) =>
              event.type === "run.updated" &&
              event.payload.id === recoveredRun.id &&
              event.payload.status === "interrupted",
          ),
        );
        assert.isFalse(recovered.some((event) => event.runId === "run:current-process-input"));
        assert.isTrue(
          recovered.some(
            (event) =>
              event.type === "turn-item.updated" &&
              event.payload.type === "error" &&
              event.payload.failure.code === "manual_retry_required",
          ),
        );
        assert.equal(observed.length, 2);
      }).pipe(Effect.provide(layer));
    }),
);
