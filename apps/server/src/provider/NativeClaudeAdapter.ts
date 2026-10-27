import { makeProviderFailure } from "@t3tools/provider-core/server/failure";
import {
  ProviderDriverKind,
  type ProviderInstanceId,
  type OrchestrationV2ProviderThread,
  type OrchestrationV2ProviderSession,
  type OrchestrationV2ProviderTurn,
  type OrchestrationV2TurnItem,
  type OrchestrationV2ConversationMessage,
} from "@t3tools/contracts";
import * as ProviderAdapter from "@t3tools/provider-core/server/ProviderAdapter";
import * as HostProcess from "@t3tools/shared/HostProcess";
import * as IdAllocator from "@t3tools/provider-core/server/IdAllocator";
import { ClaudeProviderCapabilitiesV2 } from "../orchestration-v2/Adapters/ClaudeAdapterV2.ts";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Queue from "effect/Queue";
import * as Schedule from "effect/Schedule";
import * as Stream from "effect/Stream";
import * as Cause from "effect/Cause";
import {
  NativeClaudeClient,
  NativeClaudeError,
  requireNativeClaudeBackgroundId,
  type NativeClaudeSession,
} from "./NativeClaudeClient.ts";
import { NativeClaudeSessions } from "./NativeClaudeSessions.ts";

/** A reply ACK has no native turn ID. Correlate only a new, matching user message. */
export const nativeClaudeMatchesPendingTurn = (
  turn: {
    readonly id: string;
    readonly items: readonly { readonly type: string; readonly text: string }[];
  },
  pending: { readonly baseline?: ReadonlySet<string>; readonly expectedText?: string },
): boolean =>
  !pending.baseline?.has(turn.id) &&
  turn.items.find((item) => item.type === "user")?.text === pending.expectedText;

export const nativeClaudeObservedTurnIsTerminal = (
  turn: { readonly status: string; readonly completedAt: number | null },
  job: NativeClaudeSession | undefined,
  interrupted = false,
): boolean =>
  turn.status === "failed" ||
  turn.status === "interrupted" ||
  job?.state === "failed" ||
  job?.state === "stopped" ||
  (interrupted && job?.status === "idle") ||
  (turn.completedAt !== null &&
    (job?.kind !== "background" || job.status === "idle" || job.state === "done"));

const driver = ProviderDriverKind.make("claudeAgent");
export const nativeClaudeCapabilities = {
  ...ClaudeProviderCapabilitiesV2,
  sessions: {
    ...ClaudeProviderCapabilitiesV2.sessions,
    supportsMultipleProviderThreadsPerSession: true,
    supportsModelSwitchInSession: false,
  },
  threads: {
    ...ClaudeProviderCapabilitiesV2.threads,
    canReadThreadSnapshot: false,
    canRollbackThread: false,
    canForkThread: false,
    canForkFromTurn: false,
  },
  turns: {
    ...ClaudeProviderCapabilitiesV2.turns,
    supportsInterrupt: true,
    supportsActiveSteering: false,
    activeSteeringInterruptsTools: false,
    terminalStatusQuality: "weak" as const,
  },
  streaming: {
    ...ClaudeProviderCapabilitiesV2.streaming,
    streamsAssistantText: false,
    streamsReasoning: false,
  },
  approvals: {
    ...ClaudeProviderCapabilitiesV2.approvals,
    supportsCommandApproval: false,
    supportsFileReadApproval: false,
    supportsFileChangeApproval: false,
    approvalsHaveNativeRequestIds: false,
  },
  planning: {
    ...ClaudeProviderCapabilitiesV2.planning,
    emitsPlanUpdated: false,
    emitsTodoList: false,
    emitsProposedPlan: false,
    supportsStructuredQuestions: false,
  },
  subagents: {
    ...ClaudeProviderCapabilitiesV2.subagents,
    supportsSubagents: false,
    emitsSubagentLifecycle: false,
  },
  tools: {
    ...ClaudeProviderCapabilitiesV2.tools,
    supportsMcpTools: false,
    supportsDynamicToolCallbacks: false,
  },
  checkpointing: {
    ...ClaudeProviderCapabilitiesV2.checkpointing,
    providerCanRollbackConversation: false,
    providerRollbackReturnsSnapshot: false,
    providerCanReadConversationSnapshot: true,
  },
};

/** The native worker owns execution; JSONL provides structured observation of that same worker. */
export const makeNativeClaudeAdapter = Effect.fn("NativeClaudeAdapter.make")(function* (options: {
  readonly instanceId: ProviderInstanceId;
  readonly configDirectory: string;
  readonly binaryPath: string;
  readonly environment: NodeJS.ProcessEnv;
  readonly client?: NativeClaudeClient;
}) {
  const allocator = yield* IdAllocator.IdAllocatorV2;
  const platform = yield* HostProcess.Platform;
  const client =
    options.client ??
    new NativeClaudeClient({
      platform,
      configDirectory: options.configDirectory,
      binaryPath: options.binaryPath || "claude",
      environment: options.environment,
    });
  const source = new NativeClaudeSessions(options.configDirectory, client);
  const error = (cause: unknown) =>
    new ProviderAdapter.ProviderAdapterProtocolError({
      driver,
      detail:
        cause instanceof NativeClaudeError
          ? cause.message
          : "Claude native operation failed. Check the CLI worker and native session availability.",
      cause,
    });
  const attempt = <A>(run: () => Promise<A>) => Effect.tryPromise({ try: run, catch: error });
  const unsupported = (detail: string) =>
    Effect.fail(new ProviderAdapter.ProviderAdapterProtocolError({ driver, detail }));
  return {
    instanceId: options.instanceId,
    driver,
    getCapabilities: () => Effect.succeed(nativeClaudeCapabilities),
    planSelectionTransition: () => Effect.succeed({ type: "restart_session" as const }),
    openSession: Effect.fn("NativeClaudeAdapter.openSession")(function* (input) {
      const now = yield* DateTime.now;
      const providerSession: OrchestrationV2ProviderSession = {
        id: input.providerSessionId,
        driver,
        providerInstanceId: options.instanceId,
        status: "ready",
        cwd: input.runtimePolicy.cwd ?? process.cwd(),
        model: input.modelSelection.model,
        capabilities: nativeClaudeCapabilities,
        createdAt: now,
        updatedAt: now,
        lastError: null,
      };
      const events = yield* Queue.unbounded<ProviderAdapter.ProviderAdapterV2Event, Cause.Done>();
      const threads = new Map<string, OrchestrationV2ProviderThread>();
      const newlyCreated = new Set<string>();
      const observed = new Map<string, Set<string>>();
      const active = new Map<
        string,
        {
          input: ProviderAdapter.ProviderAdapterV2TurnInput;
          nativeTurnId?: string;
          interrupted: boolean;
          expectedText?: string;
          baseline?: ReadonlySet<string>;
        }
      >();
      const eventSignatures = new Map<string, string>();
      const emit = (event: ProviderAdapter.ProviderAdapterV2Event) => {
        if (
          event.type === "turn_item.updated" ||
          event.type === "message.updated" ||
          event.type === "provider_turn.updated"
        ) {
          const record =
            event.type === "turn_item.updated"
              ? event.turnItem
              : event.type === "message.updated"
                ? event.message
                : event.providerTurn;
          const key = `${event.type}:${record.id}`;
          const signature = JSON.stringify({
            ...record,
            updatedAt: undefined,
            completedAt: "completedAt" in record && record.completedAt ? "completed" : null,
          });
          if (eventSignatures.get(key) === signature) return Effect.void;
          eventSignatures.set(key, signature);
        }
        return Queue.offer(events, event).pipe(Effect.asVoid);
      };
      const nativeId = (thread: OrchestrationV2ProviderThread) =>
        thread.nativeThreadRef?.nativeId ?? "";
      const jobFor = (thread: OrchestrationV2ProviderThread) =>
        attempt(async () => {
          // Observed sessions bypass ensureThread; sending must also revive their native worker.
          const job = await client.startBackground({
            sessionId: nativeId(thread),
            cwd: input.runtimePolicy.cwd ?? process.cwd(),
            resume: true,
          });
          requireNativeClaudeBackgroundId(job);
          return job;
        });
      const hydrate = Effect.fn("NativeClaudeAdapter.observe")(function* () {
        const inventory = yield* attempt(() => client.listSessions());
        for (const [id, thread] of threads) {
          const history = yield* attempt(() => source.history(id, inventory)).pipe(
            Effect.orElseSucceed(() => []),
          );
          const pending = active.get(id);
          const terminalJob = inventory.find((session) => session.sessionId === id);
          if (
            pending &&
            !pending.nativeTurnId &&
            (terminalJob?.state === "failed" || terminalJob?.state === "stopped")
          ) {
            yield* emit({
              type: "turn.terminal",
              driver,
              providerThreadId: thread.id,
              providerTurnId: allocator.derive.providerTurn({
                driver,
                nativeTurnId: `unconfirmed:${pending.input.attemptId}`,
              }),
              runOrdinal: pending.input.runOrdinal,
              status: "failed",
              failureItemOrdinal: 1,
              threadDisposition: "broken",
              failure: makeProviderFailure({
                message:
                  "Claude worker stopped before confirming the submitted native user turn. Reopen the session in the CLI.",
                class: "provider_error",
                code: "claude_native_unconfirmed",
              }),
            });
            active.delete(id);
          }
          const seen = observed.get(id) ?? new Set<string>();
          observed.set(id, seen);
          for (const turn of history) {
            let ownership = active.get(id);
            if (seen.has(turn.id) && ownership?.nativeTurnId !== turn.id) continue;
            if (!ownership && !seen.has(turn.id) && input.onExternalTurn && thread.appThreadId) {
              const adopted = yield* input.onExternalTurn({
                threadId: thread.appThreadId,
                nativeThreadId: id,
                nativeTurnId: turn.id,
                startedAt: DateTime.makeUnsafe(turn.startedAt),
                text: turn.items.find((item) => item.type === "user")?.text ?? "",
                ...(input.runtimePolicy.cwd ? { cwd: input.runtimePolicy.cwd } : {}),
                nativeStatus: turn.completedAt ? "completed" : "inProgress",
              });
              if (adopted) {
                ownership = { input: adopted, nativeTurnId: turn.id, interrupted: false };
                active.set(id, ownership);
              }
            }
            if (!ownership) {
              seen.add(turn.id);
              continue;
            }
            if (!ownership.nativeTurnId) {
              // Daemon reply ACK does not identify a turn: never bind another CLI user's input.
              if (!nativeClaudeMatchesPendingTurn(turn, ownership)) continue;
              ownership.nativeTurnId = turn.id;
            }
            if (ownership.nativeTurnId !== turn.id) continue;
            seen.add(turn.id);
            const owner = ownership.input;
            const providerTurnId = allocator.derive.providerTurn({ driver, nativeTurnId: turn.id });
            const startedAt = DateTime.makeUnsafe(turn.startedAt);
            const job = inventory.find((session) => session.sessionId === id);
            const finished = nativeClaudeObservedTurnIsTerminal(turn, job, ownership.interrupted);
            const nativeFailure = turn.status === "failed" || job?.state === "failed";
            const wasInterrupted =
              ownership.interrupted || turn.status === "interrupted" || job?.state === "stopped";
            const status = finished
              ? nativeFailure
                ? "failed"
                : wasInterrupted
                  ? "interrupted"
                  : "completed"
              : "running";
            const completedAt = finished ? yield* DateTime.now : null;
            const providerTurn: OrchestrationV2ProviderTurn = {
              id: providerTurnId,
              providerThreadId: thread.id,
              nodeId: owner.rootNodeId,
              runAttemptId: owner.attemptId,
              nativeTurnRef: { driver, nativeId: turn.id, strength: "strong" },
              ordinal: owner.providerTurnOrdinal,
              status,
              startedAt,
              completedAt,
            };
            yield* emit({
              type: "provider_turn.updated",
              driver,
              threadId: owner.threadId,
              providerTurn,
            });
            let ordinal = 0;
            for (const item of turn.items) {
              if (item.type === "user") continue;
              const updatedAt = yield* DateTime.now;
              const common = {
                id: allocator.derive.turnItemFromProviderItem({ driver, nativeItemId: item.id }),
                threadId: owner.threadId,
                runId: owner.runId,
                nodeId: owner.rootNodeId,
                providerThreadId: thread.id,
                providerTurnId,
                nativeItemRef: { driver, nativeId: item.id, strength: "strong" as const },
                parentItemId: null,
                ordinal: ++ordinal,
                status: item.status,
                title: item.title ?? null,
                startedAt,
                completedAt,
                updatedAt,
              };
              let projected: OrchestrationV2TurnItem;
              if (item.type === "assistant") {
                const messageId = allocator.derive.messageFromProviderItem({
                  driver,
                  nativeItemId: item.id,
                });
                const message: OrchestrationV2ConversationMessage = {
                  id: messageId,
                  threadId: owner.threadId,
                  runId: owner.runId,
                  nodeId: owner.rootNodeId,
                  role: "assistant",
                  text: item.text,
                  attachments: [],
                  createdBy: "agent",
                  creationSource: "provider",
                  streaming: false,
                  createdAt: startedAt,
                  updatedAt,
                };
                yield* emit({ type: "message.updated", driver, message });
                projected = {
                  ...common,
                  type: "assistant_message",
                  messageId,
                  text: item.text,
                  streaming: false,
                };
              } else if (item.type === "reasoning")
                projected = { ...common, type: "reasoning", text: item.text, streaming: false };
              else
                projected = {
                  ...common,
                  type: "dynamic_tool",
                  toolName: item.title ?? "Tool",
                  input: {},
                  output: item.text,
                };
              yield* emit({ type: "turn_item.updated", driver, turnItem: projected });
            }
            if (finished) {
              const common = {
                type: "turn.terminal" as const,
                driver,
                providerThreadId: thread.id,
                providerTurnId,
                runOrdinal: owner.runOrdinal,
                threadDisposition: "reusable" as const,
              };
              yield* emit(
                nativeFailure
                  ? {
                      ...common,
                      status: "failed",
                      failureItemOrdinal: ordinal + 1,
                      failure: makeProviderFailure({
                        message:
                          "Claude native background worker failed. Open claude attach for its terminal diagnostics.",
                        class: "provider_error",
                        code: "claude_native_failed",
                      }),
                    }
                  : {
                      ...common,
                      status: wasInterrupted ? "interrupted" : "completed",
                      failure: null,
                    },
              );
              active.delete(id);
            }
          }
        }
      });
      yield* hydrate().pipe(
        Effect.catch(() =>
          Effect.logWarning("Claude native observer failed; check native worker availability"),
        ),
        Effect.repeat(Schedule.spaced("1 second")),
        Effect.forkScoped,
      );
      const register = Effect.fn("NativeClaudeAdapter.register")(function* (
        thread: OrchestrationV2ProviderThread,
      ) {
        const id = nativeId(thread);
        threads.set(id, thread);
        const history = yield* attempt(() => source.history(id)).pipe(
          Effect.orElseSucceed(() => []),
        );
        const stillRunning = history.at(-1)?.status === "running";
        observed.set(
          id,
          new Set((stillRunning ? history.slice(0, -1) : history).map((turn) => turn.id)),
        );
        yield* emit({ type: "provider_thread.updated", driver, providerThread: thread });
        return thread;
      });
      const runtime: ProviderAdapter.ProviderAdapterV2SessionRuntime = {
        instanceId: options.instanceId,
        driver,
        providerSessionId: input.providerSessionId,
        providerSession,
        events: Stream.fromQueue(events),
        ensureThread: Effect.fn("NativeClaudeAdapter.ensureThread")(function* (request) {
          const existing = request.existingProviderThread;
          const job = yield* attempt(() =>
            client.startBackground({
              ...(existing?.nativeThreadRef?.nativeId
                ? { sessionId: existing.nativeThreadRef.nativeId }
                : {}),
              cwd: request.runtimePolicy.cwd ?? process.cwd(),
              model: request.modelSelection.model,
              resume: existing !== undefined,
            }),
          );
          const id = job.sessionId;
          if (!id)
            return yield* unsupported(
              "Claude worker did not expose a canonical session identifier.",
            );
          if (!existing) newlyCreated.add(id);
          const createdAt = yield* DateTime.now;
          return yield* register({
            ...(existing ?? {
              id: allocator.derive.providerThread({ driver, nativeThreadId: id }),
              driver,
              providerInstanceId: options.instanceId,
              appThreadId: request.threadId,
              ownerNodeId: null,
              firstRunOrdinal: null,
              lastRunOrdinal: null,
              handoffIds: [],
              forkedFrom: null,
              createdAt,
            }),
            providerSessionId: input.providerSessionId,
            nativeThreadRef: { driver, nativeId: id, strength: "strong" },
            nativeConversationHeadRef: null,
            status: "idle",
            updatedAt: createdAt,
          });
        }),
        resumeThread: ({ providerThread }) => register(providerThread),
        startTurn: Effect.fn("NativeClaudeAdapter.startTurn")(function* (request) {
          const id = nativeId(request.providerThread);
          if (request.message.attachments.length)
            return yield* unsupported(
              "Claude native daemon text input does not expose a structured attachment API. Send attachments through the attached CLI.",
            );
          if (active.has(id))
            return yield* unsupported(
              "Claude already has an observed or pending turn in this session. Wait for it to finish before sending.",
            );
          const job = yield* jobFor(request.providerThread);
          if (job.status !== "idle")
            return yield* unsupported(
              "Claude native input is idle-only. Finish the current CLI turn or permission prompt before sending from T3.",
            );
          const baselineHistory = yield* attempt(() => source.history(id)).pipe(
            Effect.catch((cause) =>
              newlyCreated.has(id) ? Effect.succeed([]) : Effect.fail(cause),
            ),
          );
          if (baselineHistory.length > 0) newlyCreated.delete(id);
          const baseline = new Set(baselineHistory.map((turn) => turn.id));
          active.set(id, {
            input: request,
            interrupted: false,
            expectedText: request.message.text,
            baseline,
          });
          yield* attempt(() => client.reply(job, request.message.text)).pipe(
            Effect.tapError(() => Effect.sync(() => active.delete(id))),
          );
        }),
        steerTurn: () =>
          unsupported(
            "Claude native daemon cannot atomically target a turn for steering. Use claude attach to send input to the current worker.",
          ),
        interruptTurn: Effect.fn("NativeClaudeAdapter.interruptTurn")(function* (request) {
          const id = nativeId(request.providerThread);
          const ownership = active.get(id);
          if (
            ownership?.nativeTurnId === undefined ||
            allocator.derive.providerTurn({ driver, nativeTurnId: ownership.nativeTurnId }) !==
              request.providerTurnId
          ) {
            return yield* unsupported(
              "This Claude turn is no longer active; no interrupt was sent.",
            );
          }
          const inventory = yield* attempt(() => client.listSessions());
          const history = yield* attempt(() => source.history(id, inventory));
          const job = inventory.find((session) => session.sessionId === id);
          if (job === undefined || history.at(-1)?.id !== ownership.nativeTurnId) {
            return yield* unsupported("Claude's active turn changed; no interrupt was sent.");
          }
          yield* attempt(() => client.interrupt(job));
          ownership.interrupted = true;
          yield* hydrate();
        }),
        respondToRuntimeRequest: () =>
          unsupported(
            "Claude's native daemon does not expose structured approval replies. Answer this permission or question in claude attach; no approval was sent.",
          ),
        readThreadSnapshot: ({ providerThread }) =>
          attempt(() => source.history(nativeId(providerThread))).pipe(
            Effect.map((history) => ({
              providerThread,
              providerTurns: [],
              messages: [],
              runtimeRequests: [],
              providerPayload: { nativeHistory: history },
            })),
          ),
        rollbackThread: () =>
          unsupported(
            "Claude native daemon does not expose conversation rollback. Use the attached CLI.",
          ),
        forkThread: () =>
          unsupported(
            "Claude native daemon does not expose structured conversation fork. Use the attached CLI.",
          ),
      };
      return runtime;
    }),
  } satisfies ProviderAdapter.ProviderAdapterV2["Service"];
});
