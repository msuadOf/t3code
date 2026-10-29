import { assert, it } from "@effect/vitest";
import * as NodeServices from "@effect/platform-node/NodeServices";
import {
  ProviderDriverKind,
  ProviderInstanceId,
  ProviderSessionId,
  ThreadId,
  ProjectId,
  type OrchestrationV2AppThread,
  type OrchestrationV2ProviderThread,
  type OrchestrationV2TurnItem,
} from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as DateTime from "effect/DateTime";
import * as Queue from "effect/Queue";
import * as Stream from "effect/Stream";
import * as Scope from "effect/Scope";
import * as Exit from "effect/Exit";
import * as IdAllocator from "@t3tools/provider-core/server/IdAllocator";
import type * as ProviderAdapter from "@t3tools/provider-core/server/ProviderAdapter";
import * as Sqlite from "../persistence/Sqlite.ts";
import * as ServerConfig from "../config.ts";
import * as GitVcsDriver from "../vcs/GitVcsDriver.ts";
import * as EventSink from "./EventSink.ts";
import * as EventStore from "./EventStore.ts";
import * as ProjectionStore from "./ProjectionStore.ts";
import * as ProviderEventIngestor from "./ProviderEventIngestor.ts";
import * as ThreadCommandExecutor from "./ThreadCommandExecutor.ts";
import { mapHistory } from "../provider/NativeSessionHistory.ts";
import { makeNativeBackgroundRecovery } from "./NativeBackgroundRecovery.ts";

const database = Sqlite.layerMemory;
const stores = Layer.merge(EventStore.layer, ProjectionStore.layer).pipe(Layer.provide(database));
const sinkLayer = EventSink.layer.pipe(Layer.provide(Layer.mergeAll(stores, database)));
const gitLayer = GitVcsDriver.layer.pipe(
  Layer.provide(ServerConfig.layerTest(process.cwd(), { prefix: "native-background-recovery-" })),
  Layer.provideMerge(NodeServices.layer),
);
const dependencies = Layer.mergeAll(
  stores,
  sinkLayer,
  IdAllocator.layer,
  ThreadCommandExecutor.layer,
  gitLayer,
);
const testLayer = Layer.mergeAll(
  dependencies,
  ProviderEventIngestor.layer.pipe(Layer.provide(dependencies)),
);

it.effect(
  "recovers a completed native task from SQLite after losing its observer while the completed parent stays unchanged",
  () =>
    Effect.gen(function* () {
      const ids = yield* IdAllocator.IdAllocatorV2,
        sink = yield* EventSink.EventSinkV2,
        store = yield* ProjectionStore.ProjectionStoreV2,
        ingestor = yield* ProviderEventIngestor.ProviderEventIngestorV2;
      const now = DateTime.makeUnsafe(1000),
        driver = ProviderDriverKind.make("claudeAgent"),
        instanceId = ProviderInstanceId.make("native-claude"),
        threadId = ThreadId.make("native-bg-recovery"),
        sessionId = ProviderSessionId.make("native-bg-session");
      const providerThreadId = ids.derive.providerThread({
        driver,
        nativeThreadId: "original-native-session",
      });
      const appThread: OrchestrationV2AppThread = {
        id: threadId,
        projectId: ProjectId.make("native-bg-project"),
        createdBy: "user",
        creationSource: "web",
        title: "Original shared conversation",
        providerInstanceId: instanceId,
        modelSelection: { instanceId, model: "claude-test" },
        runtimeMode: "full-access",
        interactionMode: "default",
        branch: null,
        worktreePath: null,
        branchPullRequest: null,
        activeOrderKey: null,
        activeProviderThreadId: providerThreadId,
        lineage: { rootThreadId: threadId, parentThreadId: null, relationshipToParent: null },
        forkedFrom: null,
        createdAt: now,
        updatedAt: now,
        archivedAt: null,
        settledOverride: null,
        settledAt: null,
        lastVisitedAt: null,
        deletedAt: null,
      };
      const providerThread: OrchestrationV2ProviderThread = {
        id: providerThreadId,
        driver,
        providerInstanceId: instanceId,
        providerSessionId: sessionId,
        appThreadId: threadId,
        ownerNodeId: null,
        nativeThreadRef: { driver, nativeId: "original-native-session", strength: "strong" },
        nativeConversationHeadRef: null,
        status: "idle",
        firstRunOrdinal: 1,
        lastRunOrdinal: 1,
        handoffIds: [],
        forkedFrom: null,
        createdAt: now,
        updatedAt: now,
      };
      yield* sink.write({
        events: [
          {
            id: yield* ids.allocate.event({ threadId }),
            type: "thread.created",
            threadId,
            occurredAt: now,
            payload: appThread,
          },
          {
            id: yield* ids.allocate.event({ threadId }),
            type: "provider-thread.updated",
            threadId,
            occurredAt: now,
            payload: providerThread,
          },
          ...mapHistory(
            {
              driver,
              thread: {
                id: "original-native-session",
                cwd: "/isolated/original-workspace",
                createdAt: 1000,
                updatedAt: 2000,
              },
              appThread,
              providerThread,
              turns: [
                {
                  id: "original-native-turn",
                  status: "completed",
                  startedAt: 1000,
                  completedAt: 2000,
                  items: [
                    {
                      id: "original-prompt",
                      type: "user",
                      text: "Start the original task",
                      status: "completed",
                    },
                    {
                      id: "original-answer",
                      type: "assistant",
                      text: "Parent reply is complete",
                      status: "completed",
                    },
                  ],
                },
              ],
            },
            ids,
          ),
        ],
      });
      const before = yield* store.getThreadProjection(threadId),
        run = before.runs[0]!,
        turn = before.providerTurns[0]!;
      assert.equal(run.status, "completed");
      assert.equal(turn.status, "completed");
      const events = yield* Queue.unbounded<ProviderAdapter.ProviderAdapterV2Event>(),
        commits = yield* Queue.unbounded<string>(),
        closed = yield* Queue.unbounded<void>();
      let subscriptions = 0;
      const session = {
        driver,
        instanceId,
        providerSessionId: sessionId,
        providerSession: { cwd: "/different-container-project" },
        startTurn: () => Effect.die("Recovery must never submit a prompt"),
        subscribeEvents: Effect.sync(() => {
          subscriptions++;
          return {
            events: Stream.fromQueue(events),
            close: Queue.offer(closed, undefined).pipe(Effect.asVoid),
          };
        }),
      } as unknown as ProviderAdapter.ProviderAdapterV2SessionRuntime;
      let observerScope = yield* Scope.make();
      yield* Effect.addFinalizer(() => Scope.close(observerScope, Exit.void));
      const buildRecovery = () =>
        makeNativeBackgroundRecovery({
          ids,
          store,
          isObserved: () => false,
          ingestor: {
            ingestNormalized: (input) =>
              ingestor
                .ingestNormalized(input)
                .pipe(
                  Effect.tap(() =>
                    input.event.type === "turn_item.updated"
                      ? Queue.offer(commits, input.event.turnItem.status)
                      : Effect.void,
                  ),
                ),
          },
        }).pipe(Effect.provideService(Scope.Scope, observerScope));
      let recover = yield* buildRecovery();
      const owner = yield* recover({
        session,
        threadId,
        nativeThreadId: "original-native-session",
        nativeTurnId: "original-native-turn",
        tasks: [{ nativeToolId: "original-bash", nativeTaskId: "native-task", status: "running" }],
      });
      assert.equal(owner?.turn.runId, run.id);
      assert.equal(owner?.turn.runtimePolicy.cwd, "/isolated/original-workspace");
      assert.equal(subscriptions, 1);
      const child: OrchestrationV2TurnItem = {
        id: ids.derive.turnItemFromProviderItem({
          driver,
          nativeItemId: "background-command:original-native-session:original-bash:native-task",
        }),
        threadId,
        runId: run.id,
        nodeId: run.rootNodeId,
        providerThreadId,
        providerTurnId: turn.id,
        nativeItemRef: { driver, nativeId: "native-task", strength: "strong" },
        parentItemId: ids.derive.turnItemFromProviderItem({
          driver,
          nativeItemId: "original-bash",
        }),
        ordinal: 9,
        status: "running",
        title: "Original background command",
        startedAt: now,
        completedAt: null,
        updatedAt: now,
        type: "command_execution",
        input: "echo original",
      };
      yield* Queue.offer(events, { type: "turn_item.updated", driver, turnItem: child });
      assert.equal(yield* Queue.take(commits), "running");
      const running = yield* store.getThreadProjection(threadId);
      assert.equal(running.turnItems.find((i) => i.id === child.id)?.status, "running");
      assert.deepEqual(running.runs, before.runs);
      assert.deepEqual(running.providerTurns, before.providerTurns);
      yield* Scope.close(observerScope, Exit.void);
      yield* Queue.take(closed);
      observerScope = yield* Scope.make();
      recover = yield* buildRecovery();
      const restored = yield* recover({
        session,
        threadId,
        nativeThreadId: "original-native-session",
        nativeTurnId: "original-native-turn",
        tasks: [
          { nativeToolId: "original-bash", nativeTaskId: "native-task", status: "completed" },
        ],
      });
      assert.equal(restored?.turn.runId, run.id);
      assert.equal(subscriptions, 2);
      yield* Queue.offer(events, {
        type: "turn_item.updated",
        driver,
        turnItem: {
          ...child,
          status: "completed",
          output: "original streamed output",
          completedAt: DateTime.makeUnsafe(3000),
          updatedAt: DateTime.makeUnsafe(3000),
        },
      });
      assert.equal(yield* Queue.take(commits), "completed");
      yield* Queue.take(closed);
      const after = yield* store.getThreadProjection(threadId);
      assert.deepEqual(after.runs, before.runs);
      assert.deepEqual(after.providerTurns, before.providerTurns);
      assert.deepEqual(after.attempts, before.attempts);
      assert.deepEqual(after.messages, before.messages);
      assert.deepEqual(after.checkpointScopes, before.checkpointScopes);
      assert.match(
        JSON.stringify(after.turnItems.find((i) => i.id === child.id)),
        /original streamed output/,
      );
      assert.equal(
        yield* recover({
          session,
          threadId,
          nativeThreadId: "original-native-session",
          nativeTurnId: "original-native-turn",
          tasks: [
            { nativeToolId: "original-bash", nativeTaskId: "native-task", status: "completed" },
          ],
        }),
        undefined,
      );
      assert.equal(subscriptions, 2);
    }).pipe(Effect.scoped, Effect.provide(testLayer)),
);
