import {
  type OrchestrationV2DomainEvent,
  ProviderApprovalDecision,
  ProviderSessionId,
  ProviderUserInputAnswers,
  RuntimeRequestId,
  ThreadId,
} from "@t3tools/contracts";
import * as DateTime from "effect/DateTime";
import * as IdAllocator from "@t3tools/provider-core/server/IdAllocator";
import * as EventSink from "./EventSink.ts";
import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";

import * as ProjectionStore from "./ProjectionStore.ts";
import * as ProviderSessionManager from "./ProviderSessionManager.ts";

export class RuntimeRequestResponseExecutionError extends Schema.TaggedError<RuntimeRequestResponseExecutionError>()(
  "RuntimeRequestResponseExecutionError",
  {
    reason: Schema.Literals([
      "request-missing",
      "request-not-ready",
      "request-not-resumable",
      "provider-session-not-active",
      "unexpected-failure",
    ]),
    threadId: ThreadId,
    providerSessionId: ProviderSessionId,
    requestId: RuntimeRequestId,
    cause: Schema.optional(Schema.Defect()),
  },
) {
  override get message(): string {
    switch (this.reason) {
      case "request-missing":
        return `Runtime request ${this.requestId} no longer exists on thread ${this.threadId}.`;
      case "request-not-ready":
        return `Runtime request ${this.requestId} on thread ${this.threadId} is not ready for response execution.`;
      case "request-not-resumable":
        return `Runtime request ${this.requestId} on thread ${this.threadId} is not resumable on provider session ${this.providerSessionId}.`;
      case "provider-session-not-active":
        return `Provider session ${this.providerSessionId} is not active for runtime request ${this.requestId} on thread ${this.threadId}.`;
      case "unexpected-failure":
        return `Failed to respond to runtime request ${this.requestId} on thread ${this.threadId} via provider session ${this.providerSessionId}.`;
    }
  }
}

const isRuntimeRequestResponseExecutionError = Schema.is(RuntimeRequestResponseExecutionError);

export interface RuntimeRequestServiceV2Shape {
  readonly respond: (input: {
    readonly threadId: ThreadId;
    readonly providerSessionId: ProviderSessionId;
    readonly requestId: RuntimeRequestId;
    readonly decision?: ProviderApprovalDecision;
    readonly feedback?: string;
    readonly answers?: ProviderUserInputAnswers;
  }) => Effect.Effect<void, RuntimeRequestResponseExecutionError>;
}

export class RuntimeRequestServiceV2 extends Context.Service<
  RuntimeRequestServiceV2,
  RuntimeRequestServiceV2Shape
>()("t3/orchestration-v2/RuntimeRequestService/RuntimeRequestServiceV2") {}

export const layer: Layer.Layer<
  RuntimeRequestServiceV2,
  never,
  | ProjectionStore.ProjectionStoreV2
  | ProviderSessionManager.ProviderSessionManagerV2
  | EventSink.EventSinkV2
  | IdAllocator.IdAllocatorV2
> = Layer.effect(
  RuntimeRequestServiceV2,
  Effect.gen(function* () {
    const projections = yield* ProjectionStore.ProjectionStoreV2;
    const sessions = yield* ProviderSessionManager.ProviderSessionManagerV2;
    const sink = yield* EventSink.EventSinkV2;
    const ids = yield* IdAllocator.IdAllocatorV2;
    const cancelUnconfirmed = Effect.fn("RuntimeRequestService.cancelUnconfirmed")(function* (
      input: Parameters<RuntimeRequestServiceV2Shape["respond"]>[0],
    ) {
      const request = yield* projections.getRuntimeRequest(input.threadId, input.requestId);
      if (
        !request ||
        request.status !== "pending" ||
        request.submittedAt === undefined ||
        request.responseCapability.type !== "live" ||
        request.responseCapability.confirmation !== "provider"
      )
        return;
      const context = yield* projections.getRuntimeResponseContext(input.threadId, input.requestId);
      const now = yield* DateTime.now;
      const events: OrchestrationV2DomainEvent[] = [
        {
          id: yield* ids.allocate.event({ threadId: input.threadId }),
          type: "runtime-request.updated",
          threadId: input.threadId,
          nodeId: request.nodeId,
          occurredAt: now,
          payload: {
            ...request,
            status: "cancelled",
            resolvedAt: now,
            responseCapability: {
              type: "not_resumable",
              reason:
                "Response delivery was not confirmed. Check the original CLI before answering again.",
            },
          },
        },
      ];
      if (context.node)
        events.push({
          id: yield* ids.allocate.event({ threadId: input.threadId }),
          type: "node.updated",
          threadId: input.threadId,
          nodeId: context.node.id,
          occurredAt: now,
          payload: { ...context.node, status: "cancelled", completedAt: now },
        });
      if (context.item)
        events.push({
          id: yield* ids.allocate.event({ threadId: input.threadId }),
          type: "turn-item.updated",
          threadId: input.threadId,
          occurredAt: now,
          payload: { ...context.item, status: "cancelled", completedAt: now, updatedAt: now },
        });
      yield* sink.writeIfRuntimeRequestSubmitted({
        threadId: input.threadId,
        requestId: input.requestId,
        submittedAt: request.submittedAt,
        events,
      });
    });

    return RuntimeRequestServiceV2.of({
      respond: Effect.fn("RuntimeRequestService.respond")(
        function* (input) {
          const request = yield* projections.getRuntimeRequest(input.threadId, input.requestId);
          if (request === undefined) {
            return yield* new RuntimeRequestResponseExecutionError({
              reason: "request-missing",
              threadId: input.threadId,
              providerSessionId: input.providerSessionId,
              requestId: input.requestId,
            });
          }
          // Most providers resolve on dispatch; opt-in providers confirm the
          // persisted submission asynchronously from their native result.
          const awaitsProvider =
            request.responseCapability.type === "live" &&
            request.responseCapability.confirmation === "provider" &&
            request.status === "pending" &&
            request.submittedAt !== undefined;
          const needsConfirmation =
            request.responseCapability.type === "live" &&
            request.responseCapability.confirmation === "provider";
          if (needsConfirmation ? !awaitsProvider : request.status !== "resolved") {
            return yield* new RuntimeRequestResponseExecutionError({
              reason: "request-not-ready",
              threadId: input.threadId,
              providerSessionId: input.providerSessionId,
              requestId: input.requestId,
            });
          }
          if (
            request.responseCapability.type !== "live" ||
            request.responseCapability.providerSessionId !== input.providerSessionId
          ) {
            return yield* new RuntimeRequestResponseExecutionError({
              reason: "request-not-resumable",
              threadId: input.threadId,
              providerSessionId: input.providerSessionId,
              requestId: input.requestId,
            });
          }
          const session = yield* sessions.get(input.providerSessionId);
          if (Option.isNone(session)) {
            return yield* new RuntimeRequestResponseExecutionError({
              reason: "provider-session-not-active",
              threadId: input.threadId,
              providerSessionId: input.providerSessionId,
              requestId: input.requestId,
            });
          }
          yield* session.value.respondToRuntimeRequest({
            requestId: input.requestId,
            ...(input.decision === undefined ? {} : { decision: input.decision }),
            ...(input.feedback === undefined ? {} : { feedback: input.feedback }),
            ...(input.answers === undefined ? {} : { answers: input.answers }),
          });
        },
        (effect, input) =>
          effect.pipe(
            Effect.tapError(() => cancelUnconfirmed(input)),
            Effect.mapError((cause) =>
              isRuntimeRequestResponseExecutionError(cause)
                ? cause
                : new RuntimeRequestResponseExecutionError({
                    reason: "unexpected-failure",
                    threadId: input.threadId,
                    providerSessionId: input.providerSessionId,
                    requestId: input.requestId,
                    cause,
                  }),
            ),
          ),
      ),
    });
  }),
);
