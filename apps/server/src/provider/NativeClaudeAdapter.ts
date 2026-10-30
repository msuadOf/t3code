import { makeNativeClaudeTerminalView } from "./NativeClaudeTerminalView.ts";
import { nativeSessionProjectionId } from "@t3tools/provider-core/server/nativeSessions";
import { makeProviderFailure } from "@t3tools/provider-core/server/failure";
import {
  ProviderDriverKind,
  PlanId,
  type ProviderInstanceId,
  type OrchestrationV2ProviderThread,
  type OrchestrationV2ProviderSession,
  type OrchestrationV2ProviderTurn,
  type OrchestrationV2TurnItem,
  type OrchestrationV2ConversationMessage,
  type OrchestrationV2RuntimeRequest,
  type OrchestrationV2ExecutionNode,
  type OrchestrationV2PlanArtifact,
  type ProviderUserInputAnswers,
  type ChatAttachment,
  type ThreadId,
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
import * as Semaphore from "effect/Semaphore";
import * as Clock from "effect/Clock";
import * as Scope from "effect/Scope";
import * as Schema from "effect/Schema";
import {
  nativeClaudeQuestion,
  nativeClaudeQuestionAnswer,
  nativeClaudeQuestionResult,
} from "./NativeClaudeQuestions.ts";
import {
  NativeClaudeClient,
  NativeClaudeError,
  requireNativeClaudeBackgroundId,
  type NativeClaudeSession,
  type NativeClaudeModelActivity,
} from "./NativeClaudeClient.ts";
import type { NativeClaudeSubagentObservation } from "./NativeClaudeSubagents.ts";
import { nativeClaudePlan } from "./NativeClaudePlanning.ts";
import { nativeClaudeAttachmentPrompt } from "./NativeClaudeAttachments.ts";
import type { McpProviderSessionConfig } from "@t3tools/provider-core/server/mcpSession";
import {
  NativeClaudeMcpBindings,
  CLAUDE_MCP_SESSION_METADATA_KEY,
  type PreparedClaudeMcp,
} from "./NativeClaudeMcpBindings.ts";
import type { NativeClaudeBackgroundCommand } from "./NativeClaudeBackgroundCommands.ts";
import { makeNativeClaudeSessionStreams } from "./NativeClaudeSessionStreams.ts";
import { makeNativeClaudeBackgroundOutput } from "./NativeClaudeBackgroundOutput.ts";
import { NativeClaudeSessions } from "./NativeClaudeSessions.ts";
import {
  nativeClaudeWaitingNotice,
  type NativeClaudeWaitingNotice,
} from "./NativeClaudeWaiting.ts";

const encodeNativeInput = Schema.encodeSync(Schema.fromJsonString(Schema.Unknown));

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
    runtimeSurvivesHostRestart: true,
  },
  threads: {
    ...ClaudeProviderCapabilitiesV2.threads,
    canReadThreadSnapshot: false,
    canRollbackThread: false,
    canForkThread: true,
    canForkFromTurn: false,
  },
  turns: {
    ...ClaudeProviderCapabilitiesV2.turns,
    exposesNativeTurnId: true,
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
    emitsPlanUpdated: true,
    emitsTodoList: true,
    emitsProposedPlan: true,
    supportsStructuredQuestions: true,
  },
  subagents: {
    ...ClaudeProviderCapabilitiesV2.subagents,
    supportsSubagents: true,
    emitsSubagentLifecycle: true,
  },
  tools: {
    ...ClaudeProviderCapabilitiesV2.tools,
    supportsMcpTools: false,
    supportsDynamicToolCallbacks: false,
  },
  identity: {
    ...ClaudeProviderCapabilitiesV2.identity,
    nativeTurnIds: "strong" as const,
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
  readonly modelActivity?: NativeClaudeModelActivity;
  readonly observeNewSessions?: boolean;
  readonly resolveAttachmentPath?: (attachment: ChatAttachment) => string | null;
  readonly mcp?: {
    readonly bindings: NativeClaudeMcpBindings;
    readonly readConfig: (
      threadId: ThreadId,
    ) => Effect.Effect<McpProviderSessionConfig | undefined>;
    readonly launcher: Parameters<NativeClaudeMcpBindings["prepare"]>[0]["launcher"];
  };
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
      ...(options.modelActivity ? { modelActivity: options.modelActivity } : {}),
    });
  const source = new NativeClaudeSessions(options.configDirectory, client);
  const capabilities = options.mcp
    ? {
        ...nativeClaudeCapabilities,
        tools: { ...nativeClaudeCapabilities.tools, supportsMcpTools: true },
      }
    : nativeClaudeCapabilities;
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
    getCapabilities: () => Effect.succeed(capabilities),
    planSelectionTransition: () => Effect.succeed({ type: "restart_session" as const }),
    openSession: Effect.fn("NativeClaudeAdapter.openSession")(function* (input) {
      const sessionScope = yield* Scope.Scope;
      const now = yield* DateTime.now;
      const providerSession: OrchestrationV2ProviderSession = {
        id: input.providerSessionId,
        driver,
        providerInstanceId: options.instanceId,
        status: "ready",
        cwd: input.runtimePolicy.cwd ?? process.cwd(),
        model: input.modelSelection.model,
        capabilities,
        createdAt: now,
        updatedAt: now,
        lastError: null,
      };
      const itemOrdinals = new Map<string, Map<string, number>>();
      const itemOrdinal = (turnId: string, itemId: string) => {
        let items = itemOrdinals.get(turnId);
        if (!items) {
          items = new Map();
          itemOrdinals.set(turnId, items);
        }
        const existing = items.get(itemId);
        if (existing !== undefined) return existing;
        const ordinal = items.size + 1;
        items.set(itemId, ordinal);
        return ordinal;
      };
      const events = yield* Queue.unbounded<ProviderAdapter.ProviderAdapterV2Event, Cause.Done>();
      const threads = new Map<string, OrchestrationV2ProviderThread>();
      const newlyCreated = new Set<string>();
      const mcpGenerations = new Map<string, string>();
      const prepareMcp = Effect.fn("NativeClaudeAdapter.prepareMcp")(function* (
        threadId: ThreadId,
      ) {
        if (!options.mcp) return;
        const config = yield* options.mcp.readConfig(threadId);
        if (!config) return;
        if (
          config.threadId !== threadId ||
          config.providerInstanceId !== options.instanceId ||
          !config.bindNativeSession
        )
          return yield* unsupported(
            "Claude MCP requires a matching, native-session-bound T3 credential.",
          );
        const prepared = yield* attempt(() =>
          options.mcp!.bindings.prepare({ config, launcher: options.mcp!.launcher }),
        );
        return { config, prepared };
      });
      const bindMcp = Effect.fn("NativeClaudeAdapter.bindMcp")(function* (
        id: string,
        config: McpProviderSessionConfig,
        prepared?: PreparedClaudeMcp,
      ) {
        if (
          !options.mcp ||
          !config.bindNativeSession ||
          !(yield* config.bindNativeSession({
            metadataKey: CLAUDE_MCP_SESSION_METADATA_KEY,
            sessionId: id,
          }))
        )
          return yield* unsupported(
            "Claude MCP credential could not be bound to this native session.",
          );
        yield* attempt(() =>
          prepared
            ? options.mcp!.bindings.bind({ prepared, nativeSessionId: id, config })
            : options.mcp!.bindings.recover({ nativeSessionId: id, config }),
        );
      });
      const refreshMcp = Effect.fn("NativeClaudeAdapter.refreshMcp")(function* (
        thread: OrchestrationV2ProviderThread,
        job: NativeClaudeSession,
      ) {
        if (!options.mcp || !job.sessionId || !thread.appThreadId) return;
        const config = yield* options.mcp.readConfig(thread.appThreadId);
        if (!config) return;
        if (
          config.threadId !== thread.appThreadId ||
          config.providerInstanceId !== options.instanceId
        )
          return yield* unsupported(
            "Claude MCP credential belongs to another T3 thread or provider instance.",
          );
        const generation = `${config.providerSessionId}:${config.authorizationHeader}:${job.pid ?? ""}:${job.startedAt}`;
        if (mcpGenerations.get(job.sessionId) === generation) return;
        const owner = yield* attempt(() => options.mcp!.bindings.ownership(job.sessionId!));
        // Existing workers without this private launch remain usable, but never claim a hot installation.
        if (
          !owner ||
          owner.revoked ||
          !(yield* attempt(() => client.hasMcpConfig(job, owner.prepared.mcpConfigPath)))
        )
          return;
        if (
          owner.threadId !== thread.appThreadId ||
          owner.providerInstanceId !== options.instanceId ||
          config.threadId !== thread.appThreadId ||
          config.providerInstanceId !== options.instanceId
        )
          return yield* unsupported(
            "Claude MCP launch belongs to another T3 thread or provider instance.",
          );
        yield* bindMcp(job.sessionId, config);
        mcpGenerations.set(job.sessionId, generation);
      });
      const observed = new Map<string, Set<string>>();
      const recoverBackgroundOnRegister = new Set<string>();
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
      const waitingNotices = new Map<
        string,
        { notice: NativeClaudeWaitingNotice; startedAt: DateTime.Utc }
      >();
      const questions = new Map<
        string,
        {
          nativeSessionId: string;
          nativeTurnId: string;
          nativeToolId: string;
          inputSignature: string;
          answering: boolean;
          request: OrchestrationV2RuntimeRequest;
          node: OrchestrationV2ExecutionNode;
          item: Extract<OrchestrationV2TurnItem, { type: "user_input_request" }>;
        }
      >();
      const eventSignatures = new Map<string, string>();
      const plans = new Map<string, { signature: string; plan: OrchestrationV2PlanArtifact }>();
      const latestPlans = new Map<string, OrchestrationV2PlanArtifact>();
      const progressStates = new Map<string, "running" | "waiting">();
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
      const backgroundOutput = yield* makeNativeClaudeBackgroundOutput({
        ids: allocator,
        emit,
        itemOrdinal,
      });
      const terminalView = makeNativeClaudeTerminalView({ ids: allocator, emit, itemOrdinal });
      const sessionStreams = makeNativeClaudeSessionStreams({
        ids: allocator,
        emit,
        itemOrdinal,
        ...(options.environment.CLAUDE_CODE_TMPDIR || options.environment.TMPDIR
          ? {
              temporaryDirectory:
                options.environment.CLAUDE_CODE_TMPDIR || options.environment.TMPDIR!,
            }
          : {}),
      });
      // Native inventory may block on a subprocess. Once ownership is known,
      // stdout and terminal pixels must not wait for another inventory refresh.
      const captureLock = yield* Semaphore.make(1);
      type LiveCapture = {
        owner: ProviderAdapter.ProviderAdapterV2TurnInput;
        providerTurnId: OrchestrationV2ProviderTurn["id"];
        turnId: string;
        startedAt: number;
        excludedPaths: readonly string[];
      };
      const liveCaptures = new Map<string, LiveCapture>();
      const sampleCapture = Effect.fn("NativeClaudeAdapter.sampleCapture")(function* (
        id: string,
        capture: LiveCapture,
        finished: boolean,
      ) {
        yield* sessionStreams.sample(
          id,
          capture.owner,
          capture.providerTurnId,
          capture.turnId,
          capture.startedAt,
          finished,
          capture.excludedPaths,
        );
        yield* terminalView.sample(
          id,
          capture.owner,
          capture.providerTurnId,
          capture.turnId,
          capture.startedAt,
          finished,
        );
      });
      yield* Effect.gen(function* () {
        for (const [id, capture] of liveCaptures) {
          yield* sampleCapture(id, capture, false);
        }
      }).pipe(
        captureLock.withPermit,
        Effect.catch((cause) => Effect.logWarning("Claude live output capture failed", { cause })),
        Effect.repeat(Schedule.spaced("250 millis")),
        Effect.forkScoped,
      );
      const settleQuestion = Effect.fn("NativeClaudeAdapter.settleQuestion")(function* (
        key: string,
        status: "resolved" | "cancelled",
        uncertain = false,
        actualAnswers?: ProviderUserInputAnswers,
      ) {
        const entry = questions.get(key);
        if (!entry) return;
        if (entry.request.status === "resolved") {
          questions.delete(key);
          return;
        }
        if (entry.request.status === status) return;
        const at = yield* DateTime.now;
        const { answers: _submittedAnswer, ...withoutAnswer } = entry.request;
        entry.request = {
          ...(status === "resolved" ? withoutAnswer : entry.request),
          ...(actualAnswers === undefined ? {} : { answers: actualAnswers }),
          status,
          resolvedAt: at,
          ...(uncertain
            ? {
                responseCapability: {
                  type: "not_resumable" as const,
                  reason:
                    "Answer delivery was not confirmed. Check the native Claude terminal before continuing.",
                },
              }
            : {}),
        };
        yield* emit({
          type: "runtime_request.updated",
          driver,
          threadId: entry.item.threadId,
          runtimeRequest: entry.request,
        });
        yield* emit({
          type: "node.updated",
          driver,
          node: {
            ...entry.node,
            status: status === "resolved" ? "completed" : "cancelled",
            completedAt: at,
          },
        });
        yield* emit({
          type: "turn_item.updated",
          driver,
          turnItem: {
            ...entry.item,
            ...(status === "resolved" && actualAnswers !== undefined
              ? {
                  questionAnswer: {
                    requestId: entry.request.id,
                    answers: actualAnswers,
                    attachmentsByQuestionId: {},
                    questionTextById: Object.fromEntries(
                      entry.item.questions.map((question) => [question.id, question.question]),
                    ),
                  },
                }
              : {}),
            status: status === "resolved" ? "completed" : "cancelled",
            completedAt: at,
            updatedAt: at,
          },
        });
        if (!uncertain) questions.delete(key);
      });
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
      const observation = yield* Semaphore.make(1);
      // State refresh must never block intake of subsequent native terminal bytes.
      const refreshRequests = yield* Queue.sliding<void>(1);
      const watchers = new Map<
        string,
        { readonly generation: string; readonly controller: AbortController }
      >();
      const reconnects = new Map<string, { readonly attempts: number; readonly after: number }>();
      const watch = Effect.fn("NativeClaudeAdapter.watchProgress")(function* (
        job: NativeClaudeSession,
      ): Effect.fn.Return<void, never, never> {
        if (job.kind !== "background" || !job.sessionId || !job.id) return;
        const id = job.sessionId;
        const generation = `${job.id}:${job.pid ?? ""}:${job.startedAt}`;
        const previous = watchers.get(id);
        if (previous?.generation === generation) return;
        previous?.controller.abort();
        const retry = reconnects.get(id);
        if (!previous && retry && retry.after > (yield* Clock.currentTimeMillis)) return;
        const controller = new AbortController();
        watchers.set(id, { generation, controller });
        yield* Effect.gen(function* () {
          yield* Effect.addFinalizer(() => Effect.sync(() => controller.abort()));
          let signature = "";
          yield* Stream.fromAsyncIterable(
            client.subscribe(job, controller.signal),
            (cause) => cause,
          ).pipe(
            Stream.runForEach((frame) => {
              terminalView.accept(id, frame);
              if (frame.type === "stream") return Effect.void;
              // A snapshot is the subscription baseline; replayed snapshots must not spin hydrate.
              if (frame.type === "snapshot") return Effect.void;
              const next = JSON.stringify(frame);
              if (next === signature) return Effect.void;
              signature = next;
              return Queue.offer(refreshRequests, undefined).pipe(Effect.asVoid);
            }),
            Effect.catch(() => Effect.void),
          );
        }).pipe(
          Effect.scoped,
          Effect.ensuring(
            Effect.gen(function* () {
              if (watchers.get(id)?.controller !== controller) return;
              watchers.delete(id);
              const attempts = Math.min((reconnects.get(id)?.attempts ?? 0) + 1, 5);
              reconnects.set(id, {
                attempts,
                after:
                  (yield* Clock.currentTimeMillis) + Math.min(30000, 1000 * 2 ** (attempts - 1)),
              });
            }),
          ),
          Effect.forkIn(sessionScope),
        );
      });
      const subagentOwners = new Map<
        string,
        {
          owner: ProviderAdapter.ProviderAdapterV2TurnInput;
          providerTurnId: OrchestrationV2ProviderTurn["id"];
        }
      >();
      const subagentSignatures = new Map<string, string>();
      const pendingSubagents = new Set<string>();
      const publishSubagent = Effect.fnUntraced(function* (
        sessionId: string,
        observed: NativeClaudeSubagentObservation,
      ) {
        const key = `${sessionId}:${observed.toolId}`;
        const binding = subagentOwners.get(key);
        if (binding === undefined) return;
        if (observed.status === "running" || observed.status === "unknown")
          pendingSubagents.add(key);
        else pendingSubagents.delete(key);
        const signature = encodeNativeInput(observed);
        if (subagentSignatures.get(key) === signature) return;
        const { owner, providerTurnId } = binding;
        const nativeItemId = `subagent:${sessionId}:${observed.toolId}`;
        const nodeId = allocator.derive.nodeFromProviderItem({ driver, nativeItemId });
        const at = yield* DateTime.now;
        const startedAt = DateTime.makeUnsafe(observed.startedAt);
        const completedAt =
          observed.completedAt === undefined ? null : DateTime.makeUnsafe(observed.completedAt);
        const status = observed.status === "unknown" ? "waiting" : observed.status;
        const nativeItemRef = { driver, nativeId: observed.agentId, strength: "strong" as const };
        const progress =
          observed.status === "unknown"
            ? "Native agent ended; its outcome is not yet confirmed."
            : observed.progress;
        yield* emit({
          type: "node.updated",
          driver,
          node: {
            id: nodeId,
            threadId: owner.threadId,
            runId: owner.runId,
            parentNodeId: owner.rootNodeId,
            rootNodeId: owner.rootNodeId,
            kind: "subagent",
            status,
            countsForRun: false,
            providerThreadId: owner.providerThread.id,
            providerTurnId,
            nativeItemRef,
            runtimeRequestId: null,
            checkpointScopeId: null,
            startedAt,
            completedAt,
          },
        });
        yield* emit({
          type: "subagent.updated",
          driver,
          subagent: {
            id: nodeId,
            threadId: owner.threadId,
            runId: owner.runId,
            parentNodeId: owner.rootNodeId,
            origin: "provider_native",
            createdBy: "agent",
            driver,
            providerInstanceId: options.instanceId,
            providerThreadId: owner.providerThread.id,
            childThreadId: null,
            nativeTaskRef: nativeItemRef,
            prompt: observed.prompt,
            title: observed.title,
            model: observed.model ?? null,
            status: observed.status === "unknown" ? "idle" : observed.status,
            ...(progress === undefined ? {} : { progress }),
            result: observed.result ?? null,
            startedAt,
            completedAt,
            updatedAt: at,
          },
        });
        yield* emit({
          type: "turn_item.updated",
          driver,
          turnItem: {
            id: allocator.derive.turnItemFromProviderItem({ driver, nativeItemId }),
            threadId: owner.threadId,
            runId: owner.runId,
            nodeId,
            providerThreadId: owner.providerThread.id,
            providerTurnId,
            nativeItemRef,
            parentItemId: allocator.derive.turnItemFromProviderItem({
              driver,
              nativeItemId: nativeSessionProjectionId(owner.providerThread, observed.toolId),
            }),
            ordinal: itemOrdinal(
              nativeSessionProjectionId(owner.providerThread, observed.turnId),
              nativeItemId,
            ),
            status,
            title: observed.title,
            startedAt,
            completedAt,
            updatedAt: at,
            type: "subagent",
            subagentId: nodeId,
            origin: "provider_native",
            driver,
            providerInstanceId: options.instanceId,
            childThreadId: null,
            prompt: observed.prompt,
            ...(progress === undefined ? {} : { progress }),
            result: observed.result ?? null,
          },
        });
        subagentSignatures.set(key, signature);
      });
      const hydrate = Effect.fn("NativeClaudeAdapter.observe")(function* () {
        const inventory = yield* attempt(() => client.listSessions());
        for (const [id, watcher] of watchers) {
          if (
            !threads.has(id) ||
            !inventory.some((job) => job.sessionId === id && job.kind === "background")
          )
            watcher.controller.abort();
        }
        for (const [id, thread] of threads) {
          const job = inventory.find((job) => job.sessionId === id);
          if (job) {
            yield* refreshMcp(thread, job);
            yield* watch(job);
          }
          let historyAvailable = true;
          const history = yield* attempt(() => source.history(id, inventory)).pipe(
            Effect.catch(() =>
              Effect.sync(() => {
                historyAvailable = false;
                return [];
              }),
            ),
          );
          const subagents = historyAvailable
            ? yield* attempt(() => source.subagents(id)).pipe(
                Effect.catch(() => Effect.succeed([])),
              )
            : [];
          let commandsAvailable = historyAvailable;
          const commands = historyAvailable
            ? yield* attempt(() => source.backgroundCommands(id)).pipe(
                Effect.catch(() =>
                  Effect.sync(() => {
                    commandsAvailable = false;
                    return [];
                  }),
                ),
              )
            : [];
          backgroundOutput.update(id, commands);
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
                nativeStatus: nativeClaudeObservedTurnIsTerminal(turn, terminalJob)
                  ? turn.status === "failed" || terminalJob?.state === "failed"
                    ? "failed"
                    : turn.status === "interrupted" || terminalJob?.state === "stopped"
                      ? "interrupted"
                      : "completed"
                  : "inProgress",
                snapshot: true,
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
            const providerTurnId = allocator.derive.providerTurn({
              driver,
              nativeTurnId: nativeSessionProjectionId(thread, turn.id),
            });
            const startedAt = DateTime.makeUnsafe(turn.startedAt);
            const job =
              turn.id === history.at(-1)?.id
                ? inventory.find((session) => session.sessionId === id)
                : undefined;
            const finished = nativeClaudeObservedTurnIsTerminal(turn, job, ownership.interrupted);
            yield* Effect.gen(function* () {
              const capture: LiveCapture = {
                owner,
                providerTurnId,
                turnId: turn.id,
                startedAt: turn.startedAt,
                excludedPaths: commands.map((command) => command.outputPath),
              };
              if (finished) {
                // Finalize only a capture that was observed live, never old history.
                if (liveCaptures.get(id)?.turnId === turn.id) {
                  liveCaptures.delete(id);
                  yield* sampleCapture(id, capture, true);
                }
              } else {
                liveCaptures.set(id, capture);
                yield* sampleCapture(id, capture, false);
              }
            }).pipe(captureLock.withPermit);
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
            if (!finished && input.onNativeTurnProgress) {
              const progress = job?.status === "waiting" ? "waiting" : "running";
              if (progressStates.get(owner.rootNodeId) !== progress) {
                const applied = yield* input.onNativeTurnProgress({
                  threadId: owner.threadId,
                  providerThreadId: thread.id,
                  runId: owner.runId,
                  rootNodeId: owner.rootNodeId,
                  status: progress,
                });
                if (applied) progressStates.set(owner.rootNodeId, progress);
              }
            } else if (finished) progressStates.delete(owner.rootNodeId);
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
            if (!finished) {
              yield* emit({
                type: "provider_turn.updated",
                driver,
                threadId: owner.threadId,
                providerTurn,
              });
            }
            const pendingQuestions = turn.items.filter(
              (item) =>
                item.type === "tool" &&
                item.title === "AskUserQuestion" &&
                item.status === "running",
            );
            const waitingKey = nativeSessionProjectionId(thread, `${turn.id}:waiting-status`);
            const previousWaiting = waitingNotices.get(waitingKey);
            const waitingNotice = nativeClaudeWaitingNotice(
              previousWaiting?.notice,
              job,
              pendingQuestions,
              finished,
            );
            if (waitingNotice) {
              const at = yield* DateTime.now;
              const noticeStartedAt = previousWaiting?.startedAt ?? at;
              yield* emit({
                type: "turn_item.updated",
                driver,
                turnItem: {
                  id: allocator.derive.turnItemFromProviderItem({
                    driver,
                    nativeItemId: waitingKey,
                  }),
                  threadId: owner.threadId,
                  runId: owner.runId,
                  nodeId: owner.rootNodeId,
                  providerThreadId: thread.id,
                  providerTurnId,
                  nativeItemRef: null,
                  parentItemId: null,
                  ordinal: itemOrdinal(nativeSessionProjectionId(thread, turn.id), waitingKey),
                  status: waitingNotice.status,
                  title: "Claude input status",
                  startedAt: noticeStartedAt,
                  completedAt: waitingNotice.status === "completed" ? at : null,
                  updatedAt: at,
                  type: "system_notice",
                  message: waitingNotice.message,
                },
              });
              waitingNotices.set(waitingKey, { notice: waitingNotice, startedAt: noticeStartedAt });
            }
            // Restore the original request before terminal projection can cancel it.
            for (const item of turn.items) {
              if (item.type !== "tool" || item.title !== "AskUserQuestion") continue;
              const key = `${id}:${turn.id}:${item.id}`;
              let entry = questions.get(key);
              const question = nativeClaudeQuestion(item.input);
              if (!entry && question) {
                const restored = yield* (
                  input.findNativeRuntimeRequest?.({
                    threadId: owner.threadId,
                    nativeThreadId: id,
                    nativeTurnId: turn.id,
                    nativeRequestId: item.id,
                  }) ?? Effect.succeed(undefined)
                );
                if (restored?.status === "resolved") continue;
                const canPublish =
                  !finished &&
                  item.status === "running" &&
                  job?.status === "waiting" &&
                  pendingQuestions.length === 1;
                if (restored || canPublish) {
                  const at = yield* DateTime.now;
                  const requestId =
                    restored?.id ??
                    (yield* allocator.allocate
                      .runtimeRequest({ driver, providerTurnId, nativeRequestId: item.id })
                      .pipe(Effect.mapError(error)));
                  const nodeId = restored?.nodeId ?? allocator.derive.approvalNode({ requestId });
                  const blocked =
                    restored !== undefined &&
                    (restored.submittedAt !== undefined ||
                      restored.status !== "pending" ||
                      restored.responseCapability.type === "not_resumable");
                  const responseCapability: OrchestrationV2RuntimeRequest["responseCapability"] =
                    blocked
                      ? restored!.responseCapability
                      : {
                          type: "live",
                          providerSessionId: input.providerSessionId,
                          confirmation: "provider",
                        };
                  const request: OrchestrationV2RuntimeRequest = restored
                    ? { ...restored, responseCapability }
                    : {
                        id: requestId,
                        nodeId,
                        providerTurnId,
                        nativeRequestRef: { driver, nativeId: item.id, strength: "strong" },
                        kind: "user_input",
                        status: "pending",
                        responseCapability,
                        createdAt: at,
                        resolvedAt: null,
                      };
                  const status =
                    request.status === "pending"
                      ? ("waiting" as const)
                      : request.status === "resolved"
                        ? ("completed" as const)
                        : ("cancelled" as const);
                  const node: OrchestrationV2ExecutionNode = {
                    id: nodeId,
                    threadId: owner.threadId,
                    runId: owner.runId,
                    parentNodeId: owner.rootNodeId,
                    rootNodeId: owner.rootNodeId,
                    kind: "user_input_request",
                    status,
                    countsForRun: false,
                    providerThreadId: thread.id,
                    providerTurnId,
                    nativeItemRef: { driver, nativeId: item.id, strength: "strong" },
                    runtimeRequestId: requestId,
                    checkpointScopeId: null,
                    startedAt: request.createdAt,
                    completedAt: request.resolvedAt,
                  };
                  const questionItem: Extract<
                    OrchestrationV2TurnItem,
                    { type: "user_input_request" }
                  > = {
                    id: allocator.derive.approvalTurnItem({ requestId }),
                    threadId: owner.threadId,
                    runId: owner.runId,
                    nodeId,
                    providerThreadId: thread.id,
                    providerTurnId,
                    nativeItemRef: { driver, nativeId: item.id, strength: "strong" },
                    parentItemId: null,
                    ordinal: itemOrdinal(turn.id, `question:${item.id}`),
                    status,
                    title: "User input",
                    startedAt: request.createdAt,
                    completedAt: request.resolvedAt,
                    updatedAt: at,
                    type: "user_input_request",
                    requestId,
                    questions: [question],
                  };
                  entry = {
                    nativeSessionId: id,
                    nativeTurnId: turn.id,
                    nativeToolId: item.id,
                    inputSignature: encodeNativeInput(item.input),
                    answering: blocked,
                    request,
                    node,
                    item: questionItem,
                  };
                  questions.set(key, entry);
                  yield* emit({ type: "node.updated", driver, node });
                  yield* emit({
                    type: "runtime_request.updated",
                    driver,
                    threadId: owner.threadId,
                    runtimeRequest: request,
                  });
                  yield* emit({ type: "turn_item.updated", driver, turnItem: questionItem });
                }
              }
              if (!entry) continue;
              if (item.status !== "running" || finished) {
                yield* settleQuestion(
                  key,
                  item.status === "completed" && item.text.length > 0 ? "resolved" : "cancelled",
                  false,
                  nativeClaudeQuestionResult(entry.item.questions[0]!, item.result),
                );
              } else if (pendingQuestions.length !== 1) {
                entry.answering = true;
                yield* settleQuestion(key, "cancelled", true);
              }
            }
            if (finished) {
              yield* emit({
                type: "provider_turn.updated",
                driver,
                threadId: owner.threadId,
                providerTurn,
              });
            }
            let ordinal = 0;
            for (const item of turn.items) {
              if (item.type === "user" && item.inputIntent !== "steer") continue;
              const updatedAt = yield* DateTime.now;
              const common = {
                id: allocator.derive.turnItemFromProviderItem({
                  driver,
                  nativeItemId: nativeSessionProjectionId(thread, item.id),
                }),
                threadId: owner.threadId,
                runId: owner.runId,
                nodeId: owner.rootNodeId,
                providerThreadId: thread.id,
                providerTurnId,
                nativeItemRef: { driver, nativeId: item.id, strength: "strong" as const },
                parentItemId: null,
                ordinal: (ordinal = itemOrdinal(
                  nativeSessionProjectionId(thread, turn.id),
                  nativeSessionProjectionId(thread, item.id),
                )),
                status:
                  finished && item.status === "running"
                    ? nativeFailure
                      ? "failed"
                      : "cancelled"
                    : item.status,
                title: item.title ?? null,
                startedAt,
                completedAt,
                updatedAt,
              };
              let projected: OrchestrationV2TurnItem;
              if (item.type === "assistant" || item.type === "user") {
                const messageId = allocator.derive.messageFromProviderItem({
                  driver,
                  nativeItemId: nativeSessionProjectionId(thread, item.id),
                });
                const message: OrchestrationV2ConversationMessage = {
                  id: messageId,
                  threadId: owner.threadId,
                  runId: owner.runId,
                  nodeId: owner.rootNodeId,
                  role: item.type === "user" ? "user" : "assistant",
                  text: item.text,
                  attachments: [],
                  createdBy: item.type === "user" ? "user" : "agent",
                  creationSource: "provider",
                  streaming: false,
                  createdAt: startedAt,
                  updatedAt,
                };
                yield* emit({ type: "message.updated", driver, message });
                projected =
                  item.type === "user"
                    ? {
                        ...common,
                        type: "user_message",
                        messageId,
                        text: item.text,
                        attachments: [],
                        inputIntent: "steer",
                        createdBy: "user",
                        creationSource: "provider",
                      }
                    : {
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
                  input: item.input ?? {},
                  output: item.text,
                };
              yield* emit({ type: "turn_item.updated", driver, turnItem: projected });
              const nativePlan = nativeClaudePlan(item);
              if (nativePlan !== undefined) {
                const nativeItemId = nativeSessionProjectionId(
                  thread,
                  `${nativePlan.kind}:${item.id}`,
                );
                const key = `${owner.threadId}:${nativeItemId}`;
                const signature = encodeNativeInput(nativePlan);
                const previous = plans.get(key);
                if (previous?.signature === signature) continue;
                // Native tool IDs survive observer reconnects and T3 restarts.
                const planId = PlanId.make(
                  `plan:claude-native:${encodeURIComponent(owner.threadId)}:${encodeURIComponent(nativeItemId)}`,
                );
                const nodeId = allocator.derive.nodeFromProviderItem({ driver, nativeItemId });
                const plan: OrchestrationV2PlanArtifact =
                  nativePlan.kind === "todo_list"
                    ? {
                        id: planId,
                        threadId: owner.threadId,
                        runId: owner.runId,
                        nodeId,
                        kind: "todo_list",
                        steps: [...nativePlan.steps],
                        status: nativePlan.steps.every((step) => step.status === "completed")
                          ? "completed"
                          : "active",
                      }
                    : {
                        id: planId,
                        threadId: owner.threadId,
                        runId: owner.runId,
                        nodeId,
                        kind: "proposed_plan",
                        markdown: nativePlan.markdown,
                        status: "active",
                      };
                const latestKey = `${owner.threadId}:${nativePlan.kind}`;
                const latest = latestPlans.get(latestKey);
                if (latest !== undefined && latest.id !== plan.id)
                  yield* emit({
                    type: "plan.updated",
                    driver,
                    plan: { ...latest, status: "superseded" },
                  });
                yield* emit({
                  type: "node.updated",
                  driver,
                  node: {
                    id: nodeId,
                    threadId: owner.threadId,
                    runId: owner.runId,
                    parentNodeId: owner.rootNodeId,
                    rootNodeId: owner.rootNodeId,
                    kind: nativePlan.kind === "todo_list" ? "todo_list" : "plan",
                    status: "completed",
                    countsForRun: false,
                    providerThreadId: thread.id,
                    providerTurnId,
                    nativeItemRef: common.nativeItemRef,
                    runtimeRequestId: null,
                    checkpointScopeId: null,
                    startedAt,
                    completedAt: updatedAt,
                  },
                });
                yield* emit({ type: "plan.updated", driver, plan });
                const planCommon = {
                  ...common,
                  id: allocator.derive.turnItemFromProviderItem({ driver, nativeItemId }),
                  nodeId,
                  ordinal: itemOrdinal(nativeSessionProjectionId(thread, turn.id), nativeItemId),
                  status: "completed" as const,
                  completedAt: updatedAt,
                  planId,
                };
                yield* emit({
                  type: "turn_item.updated",
                  driver,
                  turnItem:
                    nativePlan.kind === "todo_list"
                      ? { ...planCommon, type: "todo_list", steps: [...nativePlan.steps] }
                      : {
                          ...planCommon,
                          type: "proposed_plan",
                          markdown: nativePlan.markdown,
                          streaming: false,
                        },
                });
                plans.set(key, { signature, plan });
                latestPlans.set(latestKey, plan);
              }
            }
            for (const command of commands) {
              if (command.turnId === turn.id)
                yield* backgroundOutput.bind(id, command, owner, providerTurnId);
            }
            for (const child of subagents) {
              if (child.turnId !== turn.id) continue;
              subagentOwners.set(`${id}:${child.toolId}`, { owner, providerTurnId });
              yield* publishSubagent(id, child);
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
          if (input.recoverNativeBackgroundTurn && thread.appThreadId && commandsAvailable) {
            const recoverable = new Map<string, NativeClaudeBackgroundCommand[]>();
            for (const command of commands) {
              if (
                backgroundOutput.hasObserved(id, command) ||
                (command.status !== "running" && !recoverBackgroundOnRegister.has(id))
              )
                continue;
              const group = recoverable.get(command.turnId) ?? [];
              group.push(command);
              recoverable.set(command.turnId, group);
            }
            for (const [turnId, tasks] of recoverable) {
              const restored:
                | ProviderAdapter.ProviderAdapterV2NativeBackgroundTurnOwner
                | undefined = yield* input.recoverNativeBackgroundTurn({
                threadId: thread.appThreadId,
                nativeThreadId: id,
                nativeTurnId: turnId,
                tasks: tasks.map((command) => ({
                  nativeToolId: command.toolId,
                  nativeTaskId: command.taskId,
                  status: command.status,
                })),
              });
              if (
                !restored ||
                restored.turn.threadId !== thread.appThreadId ||
                restored.turn.providerThread.id !== thread.id ||
                restored.turn.providerThread.nativeThreadRef?.nativeId !== id
              )
                continue;
              const providerTurnId = restored.providerTurnId;
              for (const command of tasks) {
                if (
                  restored.tasks.some(
                    (task) =>
                      task.nativeToolId === command.toolId && task.nativeTaskId === command.taskId,
                  )
                )
                  yield* backgroundOutput.bind(id, command, restored.turn, providerTurnId);
              }
            }
            recoverBackgroundOnRegister.delete(id);
          }
          // Background children can finish after their parent model turn has ended.
          for (const child of subagents) yield* publishSubagent(id, child);
          for (const [key, entry] of questions) {
            if (entry.nativeSessionId !== id || !historyAvailable) continue;
            const current = history.at(-1);
            const tool = current?.items.find((item) => item.id === entry.nativeToolId);
            if (
              current?.id !== entry.nativeTurnId ||
              !tool ||
              nativeClaudeObservedTurnIsTerminal(current, terminalJob)
            )
              yield* settleQuestion(
                key,
                tool?.status === "completed" && tool.text.length > 0 ? "resolved" : "cancelled",
                false,
                nativeClaudeQuestionResult(entry.item.questions[0]!, tool?.result),
              );
          }
        }
      }, observation.withPermit);
      yield* Queue.take(refreshRequests).pipe(
        Effect.flatMap(() => hydrate()),
        Effect.catch((cause) => Effect.logWarning("Claude native state refresh failed", { cause })),
        Effect.forever,
        Effect.forkScoped,
      );
      yield* hydrate().pipe(
        Effect.catch((cause) =>
          Effect.logWarning("Claude native observer failed; check native worker availability", {
            cause,
          }),
        ),
        Effect.repeat(Schedule.spaced("1 second")),
        Effect.forkScoped,
      );
      const register = Effect.fn("NativeClaudeAdapter.register")(function* (
        thread: OrchestrationV2ProviderThread,
      ) {
        const id = nativeId(thread);
        if (thread.status === "active") recoverBackgroundOnRegister.add(id);
        if (!observed.has(id)) {
          const history = yield* attempt(() => source.history(id)).pipe(
            Effect.orElseSucceed(() => []),
          );
          // An active T3 turn can finish while this daemon is offline. Keep its last
          // native turn observable so adoption recovers the original run/attempt.
          const recoverLast = thread.status === "active" || history.at(-1)?.status === "running";
          observed.set(
            id,
            new Set((recoverLast ? history.slice(0, -1) : history).map((turn) => turn.id)),
          );
        }
        threads.set(id, thread);
        yield* emit({ type: "provider_thread.updated", driver, providerThread: thread });
        yield* hydrate();
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
          const nativeOwner = existing?.nativeThreadRef?.nativeId
            ? (yield* attempt(() => client.listSessions())).some(
                (job) => job.sessionId === existing.nativeThreadRef!.nativeId,
              )
            : false;
          const mcp = nativeOwner ? undefined : yield* prepareMcp(request.threadId);
          const job = yield* attempt(() =>
            client.startBackground({
              ...(existing?.nativeThreadRef?.nativeId
                ? { sessionId: existing.nativeThreadRef.nativeId }
                : {}),
              cwd: request.runtimePolicy.cwd ?? process.cwd(),
              model: request.modelSelection.model,
              resume: existing !== undefined,
              ...(mcp ? { mcpConfigPath: mcp.prepared.mcpConfigPath } : {}),
              ...(existing === undefined && options.observeNewSessions
                ? { observeModelActivity: true }
                : {}),
            }),
          );
          const id = job.sessionId;
          if (!id)
            return yield* unsupported(
              "Claude worker did not expose a canonical session identifier.",
            );
          if (mcp) yield* bindMcp(id, mcp.config, mcp.prepared);
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
        hasPendingBackgroundWorkForThread: (providerThread) =>
          Effect.sync(() => {
            const id = nativeId(providerThread);
            return (
              active.has(id) ||
              backgroundOutput.hasPending(id) ||
              [...pendingSubagents].some((key) => key.startsWith(`${id}:`))
            );
          }),
        unloadThread: Effect.fn("NativeClaudeAdapter.unloadThread")(function ({ providerThread }) {
          return Effect.sync(() => {
            const id = nativeId(providerThread);
            threads.delete(id);
            active.delete(id);
            observed.delete(id);
            recoverBackgroundOnRegister.delete(id);
            backgroundOutput.drop(id);
            liveCaptures.delete(id);
            sessionStreams.drop(id);
            terminalView.drop(id);
            watchers.get(id)?.controller.abort();
            watchers.delete(id);
            for (const key of subagentOwners.keys())
              if (key.startsWith(`${id}:`)) {
                subagentOwners.delete(key);
                subagentSignatures.delete(key);
                pendingSubagents.delete(key);
              }
            for (const [key, question] of questions)
              if (question.nativeSessionId === id) questions.delete(key);
          }).pipe(captureLock.withPermit);
        }, observation.withPermit),
        startTurn: Effect.fn("NativeClaudeAdapter.startTurn")(function* (request) {
          const id = nativeId(request.providerThread);
          const prompt = yield* attempt(() =>
            nativeClaudeAttachmentPrompt({
              text: request.message.text,
              attachments: request.message.attachments,
              resolveAttachmentPath: options.resolveAttachmentPath,
            }).catch((cause: unknown) => {
              throw new NativeClaudeError(
                "EATTACHMENT",
                cause instanceof Error ? cause.message : "Claude attachment is unavailable.",
              );
            }),
          );
          if (active.has(id))
            return yield* unsupported(
              "Claude already has an observed or pending turn in this session. Wait for it to finish before sending.",
            );
          const job = yield* jobFor(request.providerThread);
          yield* refreshMcp(request.providerThread, job);
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
            expectedText: prompt,
            baseline,
          });
          yield* attempt(() => client.reply(job, prompt)).pipe(
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
            allocator.derive.providerTurn({
              driver,
              nativeTurnId: nativeSessionProjectionId(
                request.providerThread,
                ownership.nativeTurnId,
              ),
            }) !== request.providerTurnId
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
        respondToRuntimeRequest: Effect.fn("NativeClaudeAdapter.answerQuestion")(
          function* (response) {
            const entry = [...questions.values()].find(
              (entry) => entry.request.id === response.requestId,
            );
            if (!entry || entry.answering || response.decision !== undefined)
              return yield* unsupported(
                "No answerable native Claude question; no approval was sent.",
              );
            const answer = nativeClaudeQuestionAnswer(entry.item.questions[0]!, response.answers);
            if (answer === undefined)
              return yield* unsupported(
                "Choose known options for a multi-select question, or provide one nonempty answer without command prefixes or control characters.",
              );
            entry.answering = true;
            entry.request = {
              ...entry.request,
              ...(response.answers === undefined ? {} : { answers: response.answers }),
            };
            const key = `${entry.nativeSessionId}:${entry.nativeTurnId}:${entry.nativeToolId}`;
            yield* Effect.gen(function* () {
              const inventory = yield* attempt(() => client.listSessions());
              const job = inventory.find((job) => job.sessionId === entry.nativeSessionId);
              const history = yield* attempt(() =>
                source.history(entry.nativeSessionId, inventory),
              );
              const turn = history.at(-1);
              const tool = turn?.items.find((item) => item.id === entry.nativeToolId);
              if (
                !job ||
                job.kind !== "background" ||
                job.status !== "waiting" ||
                turn?.id !== entry.nativeTurnId ||
                tool?.status !== "running" ||
                turn.items.filter(
                  (item) =>
                    item.type === "tool" &&
                    item.title === "AskUserQuestion" &&
                    item.status === "running",
                ).length !== 1 ||
                encodeNativeInput(tool.input) !== entry.inputSignature
              )
                return yield* unsupported(
                  "The native Claude question changed or was answered in the CLI; no answer was sent.",
                );
              // The native protocol cannot atomically address a request. Only a matching
              // persisted tool result confirms delivery; never resolve on the routing ACK.
              yield* attempt(() => client.reply(job, answer));
              yield* hydrate();
            }).pipe(Effect.tapError(() => settleQuestion(key, "cancelled", true)));
          },
        ),
        readThreadSnapshot: ({ providerThread }) =>
          attempt(() => source.history(nativeId(providerThread))).pipe(
            Effect.map((history) => ({
              providerThread,
              providerTurns: [],
              messages: [],
              runtimeRequests: [...questions.values()]
                .filter((entry) => entry.item.providerThreadId === providerThread.id)
                .map((entry) => entry.request),
              providerPayload: { nativeHistory: history },
            })),
          ),
        rollbackThread: () =>
          unsupported(
            "Claude native daemon does not expose conversation rollback. Use the attached CLI.",
          ),
        forkThread: Effect.fn("NativeClaudeAdapter.forkThread")(function* (request) {
          if (request.providerTurnId !== undefined)
            return yield* unsupported(
              "Native Claude supports complete-session forks; selecting a historical turn is unavailable.",
            );
          const original = request.sourceProviderThread;
          const sourceId = nativeId(original);
          const inventory = yield* attempt(() => client.listSessions());
          const sourceJob = inventory.find((job) => job.sessionId === sourceId);
          if (
            !sourceJob ||
            (request.runtimePolicy?.cwd != null && request.runtimePolicy.cwd !== sourceJob.cwd)
          )
            return yield* unsupported(
              "Native Claude forks currently retain the source working directory. Fork in the same directory.",
            );
          if (request.modelSelection !== undefined) {
            const candidate = (yield* attempt(() => source.available())).find(
              (candidate) => candidate.id === sourceId,
            );
            if (
              request.modelSelection.instanceId !== options.instanceId ||
              candidate?.model !== request.modelSelection.model
            )
              return yield* unsupported(
                "Native Claude forks retain their source model. Select the same native model before forking.",
              );
          }
          const history = yield* attempt(() => source.history(sourceId));
          if (history.at(-1)?.status === "running")
            return yield* unsupported(
              "Wait for the source Claude turn to finish before forking its complete history.",
            );
          const sourceMcp = options.mcp
            ? yield* attempt(() => options.mcp!.bindings.ownership(sourceId))
            : undefined;
          const targetMcp = yield* prepareMcp(request.targetThreadId);
          if (sourceMcp && (!targetMcp || sourceMcp.revoked))
            return yield* unsupported(
              "Forking a Claude MCP worker requires an independent target T3 credential.",
            );
          const job = yield* attempt(() =>
            client.forkBackground({
              sessionId: sourceId,
              targetThreadId: request.targetThreadId,
              cwd: sourceJob.cwd,
              ...(targetMcp ? { mcpConfigPath: targetMcp.prepared.mcpConfigPath } : {}),
              ...(sourceMcp
                ? { expectedSourceMcpConfigPath: sourceMcp.prepared.mcpConfigPath }
                : {}),
              ...(options.observeNewSessions ? { observeModelActivity: true } : {}),
            }),
          );
          const id = job.sessionId!;
          if (targetMcp) yield* bindMcp(id, targetMcp.config, targetMcp.prepared);
          const at = yield* DateTime.now;
          newlyCreated.add(id);
          // Copied native entry UUIDs are history, never newly arriving external turns.
          observed.set(id, new Set(history.map((turn) => turn.id)));
          return yield* register({
            id: allocator.derive.providerThread({ driver, nativeThreadId: id }),
            driver,
            providerInstanceId: options.instanceId,
            providerSessionId: input.providerSessionId,
            appThreadId: request.targetThreadId,
            ownerNodeId: request.ownerNodeId ?? null,
            nativeThreadRef: { driver, nativeId: id, strength: "strong" },
            nativeConversationHeadRef: null,
            nativeMetadata: { claudeForkIdentityNamespace: id },
            status: "idle",
            firstRunOrdinal: null,
            lastRunOrdinal: null,
            handoffIds: [],
            forkedFrom: { providerThreadId: original.id },
            createdAt: at,
            updatedAt: at,
          });
        }),
      };
      return runtime;
    }),
  } satisfies ProviderAdapter.ProviderAdapterV2["Service"];
});
