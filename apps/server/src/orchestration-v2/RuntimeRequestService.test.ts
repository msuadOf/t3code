import { assert, it, vi } from "@effect/vitest";
import {
  NodeId,
  TurnItemId,
  ProviderSessionId,
  RuntimeRequestId,
  ThreadId,
  type OrchestrationV2ThreadProjection,
  type OrchestrationV2DomainEvent,
} from "@t3tools/contracts";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";

import * as EventSink from "./EventSink.ts";
import * as IdAllocator from "@t3tools/provider-core/server/IdAllocator";
import type * as ProviderAdapter from "@t3tools/provider-core/server/ProviderAdapter";
import * as ProjectionStore from "./ProjectionStore.ts";
import * as ProviderSessionManager from "./ProviderSessionManager.ts";
import * as RuntimeRequestService from "./RuntimeRequestService.ts";

it.effect("rejects expired runtime requests before invoking the live adapter", () => {
  const threadId = ThreadId.make("thread-runtime-request-expired");
  const providerSessionId = ProviderSessionId.make("provider-session-runtime-request-expired");
  const requestId = RuntimeRequestId.make("request-expired");
  const respondToRuntimeRequest = vi.fn(
    (_input: ProviderAdapter.ProviderAdapterV2RuntimeRequestResponseInput) => Effect.void,
  );
  const getSession = vi.fn(() =>
    Effect.succeed(
      Option.some({
        respondToRuntimeRequest,
      } as never),
    ),
  );
  const projection = {
    runtimeRequests: [
      {
        id: requestId,
        nodeId: NodeId.make("node-runtime-request-expired"),
        providerTurnId: null,
        nativeRequestRef: null,
        kind: "command",
        status: "expired",
        responseCapability: {
          type: "live",
          providerSessionId,
        },
        createdAt: DateTime.makeUnsafe("2026-07-29T00:00:00.000Z"),
        resolvedAt: DateTime.makeUnsafe("2026-07-29T00:00:01.000Z"),
      },
    ],
  } as unknown as OrchestrationV2ThreadProjection;
  const layerTest = RuntimeRequestService.layer.pipe(
    Layer.provide(
      Layer.mergeAll(
        IdAllocator.layer,
        Layer.mock(EventSink.EventSinkV2)({}),
        Layer.mock(ProjectionStore.ProjectionStoreV2)({
          getRuntimeRequest: (_threadId, requestId) =>
            Effect.succeed(projection.runtimeRequests.find((request) => request.id === requestId)),
        }),
        Layer.mock(ProviderSessionManager.ProviderSessionManagerV2)({
          get: getSession,
        }),
      ),
    ),
  );

  return Effect.gen(function* () {
    const service = yield* RuntimeRequestService.RuntimeRequestServiceV2;
    const error = yield* service
      .respond({
        threadId,
        providerSessionId,
        requestId,
        decision: "accept",
      })
      .pipe(Effect.flip);

    assert.equal(error.reason, "request-not-ready");
    assert.equal(
      error.message,
      `Runtime request ${requestId} on thread ${threadId} is not ready for response execution.`,
    );
    assert.isUndefined(error.cause);
    assert.equal(getSession.mock.calls.length, 0);
    assert.equal(respondToRuntimeRequest.mock.calls.length, 0);
  }).pipe(Effect.provide(layerTest));
});

it.effect.each(["ack", "failure", "missing-session"] as const)(
  "awaits native confirmation and safely closes unconfirmed delivery: %s",
  (mode) =>
    Effect.gen(function* () {
      const at = DateTime.makeUnsafe(10000);
      const threadId = ThreadId.make("provider-confirmation-thread");
      const providerSessionId = ProviderSessionId.make("provider-confirmation-session");
      const requestId = RuntimeRequestId.make("provider-confirmation-request");
      const request = {
        id: requestId,
        nodeId: NodeId.make("provider-confirmation-node"),
        providerTurnId: null,
        nativeRequestRef: null,
        kind: "user_input" as const,
        status: "pending" as const,
        responseCapability: {
          type: "live" as const,
          providerSessionId,
          confirmation: "provider" as const,
        },
        createdAt: at,
        resolvedAt: null,
        submittedAt: at,
        answers: { choice: "yes" },
      };
      const writes: OrchestrationV2DomainEvent[] = [];
      const respond = vi.fn(() =>
        mode === "failure" ? Effect.fail({ _tag: "DeliveryUnconfirmed" as const }) : Effect.void,
      );
      const context = {
        request,
        session: undefined,
        node: {
          id: request.nodeId,
          threadId,
          runId: null,
          parentNodeId: null,
          rootNodeId: request.nodeId,
          kind: "user_input_request" as const,
          status: "waiting" as const,
          countsForRun: false,
          providerThreadId: null,
          providerTurnId: null,
          nativeItemRef: null,
          runtimeRequestId: requestId,
          checkpointScopeId: null,
          startedAt: at,
          completedAt: null,
        },
        item: {
          id: TurnItemId.make("confirmation-item"),
          threadId,
          runId: null,
          nodeId: request.nodeId,
          providerThreadId: null,
          providerTurnId: null,
          nativeItemRef: null,
          parentItemId: null,
          ordinal: 1,
          status: "waiting" as const,
          title: null,
          startedAt: at,
          completedAt: null,
          updatedAt: at,
          type: "user_input_request" as const,
          requestId,
          questions: [],
        },
      };
      const testLayer = RuntimeRequestService.layer.pipe(
        Layer.provide(
          Layer.mergeAll(
            IdAllocator.layer,
            Layer.mock(ProjectionStore.ProjectionStoreV2)({
              getRuntimeRequest: () => Effect.succeed(request),
              getRuntimeResponseContext: () => Effect.succeed(context),
            }),
            Layer.mock(EventSink.EventSinkV2)({
              writeIfRuntimeRequestSubmitted: (input) =>
                Effect.sync(() => {
                  writes.push(...input.events);
                  return { committed: true, storedEvents: [] };
                }),
            }),
            Layer.mock(ProviderSessionManager.ProviderSessionManagerV2)({
              get: () =>
                Effect.succeed(
                  mode === "missing-session"
                    ? Option.none()
                    : Option.some({ respondToRuntimeRequest: respond } as never),
                ),
            }),
          ),
        ),
      );
      yield* Effect.gen(function* () {
        const service = yield* RuntimeRequestService.RuntimeRequestServiceV2;
        const result = yield* service
          .respond({ threadId, providerSessionId, requestId, answers: request.answers })
          .pipe(Effect.result);
        if (mode === "ack") {
          assert.equal(result._tag, "Success");
          assert.lengthOf(writes, 0);
        } else {
          assert.equal(result._tag, "Failure");
          assert.lengthOf(writes, 3);
          assert.deepEqual(
            writes
              .filter(
                (event) => event.type === "node.updated" || event.type === "turn-item.updated",
              )
              .map((event) => event.payload.status),
            ["cancelled", "cancelled"],
          );
          assert.equal(writes[0]?.type, "runtime-request.updated");
          if (writes[0]?.type === "runtime-request.updated") {
            assert.equal(writes[0].payload.status, "cancelled");
            assert.equal(writes[0].payload.responseCapability.type, "not_resumable");
          }
        }
      }).pipe(Effect.provide(testLayer));
    }),
);
