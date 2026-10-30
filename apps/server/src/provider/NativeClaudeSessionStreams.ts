import { ProviderDriverKind, type OrchestrationV2ProviderTurn } from "@t3tools/contracts";
import type * as ProviderAdapter from "@t3tools/provider-core/server/ProviderAdapter";
import type * as IdAllocator from "@t3tools/provider-core/server/IdAllocator";
import { nativeSessionProjectionId } from "@t3tools/provider-core/server/nativeSessions";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import { NativeClaudeSessionOutput } from "./NativeClaudeSessionOutput.ts";

export function makeNativeClaudeSessionStreams(input: {
  ids: IdAllocator.IdAllocatorV2["Service"];
  emit: (event: ProviderAdapter.ProviderAdapterV2Event) => Effect.Effect<void>;
  itemOrdinal: (turnId: string, itemId: string) => number;
  temporaryDirectory?: string;
}) {
  const driver = ProviderDriverKind.make("claudeAgent");
  const captures = new Map<
    string,
    {
      reader: NativeClaudeSessionOutput;
      sources: Map<string, { output: string; truncated: boolean }>;
    }
  >();
  const completed = new Set<string>();
  return {
    drop(sessionId: string) {
      for (const key of captures.keys()) if (key.startsWith(`${sessionId}:`)) captures.delete(key);
      for (const key of completed) if (key.startsWith(`${sessionId}:`)) completed.delete(key);
    },
    sample: Effect.fn("NativeClaudeSessionStreams.sample")(function* (
      sessionId: string,
      owner: ProviderAdapter.ProviderAdapterV2TurnInput,
      providerTurnId: OrchestrationV2ProviderTurn["id"],
      turnId: string,
      startedAt: number,
      finished: boolean,
      excludedPaths: readonly string[] = [],
    ) {
      const key = `${sessionId}:${turnId}`;
      if (completed.has(key)) return;
      let capture = captures.get(key);
      if (finished && !capture) return;
      if (!capture) {
        capture = {
          reader: new NativeClaudeSessionOutput(input.temporaryDirectory),
          sources: new Map(),
        };
        captures.set(key, capture);
      }
      const pages = yield* Effect.tryPromise(() =>
        capture.reader.sample(sessionId, startedAt, excludedPaths),
      ).pipe(Effect.orElseSucceed(() => []));
      const at = yield* DateTime.now;
      for (const page of pages) {
        const nativeId = nativeSessionProjectionId(
          owner.providerThread,
          `session-output:${sessionId}:${turnId}:${page.source}`,
        );
        const itemId = input.ids.derive.turnItemFromProviderItem({
          driver,
          nativeItemId: nativeId,
        });
        const first = !capture.sources.has(page.source);
        capture.sources.set(page.source, { output: page.output, truncated: page.truncated });
        yield* input.emit({
          type: "turn_item.updated",
          driver,
          turnItem: {
            id: itemId,
            threadId: owner.threadId,
            runId: owner.runId,
            nodeId: owner.rootNodeId,
            providerThreadId: owner.providerThread.id,
            providerTurnId,
            nativeItemRef: { driver, nativeId, strength: "strong" },
            parentItemId: null,
            ordinal: input.itemOrdinal(
              nativeSessionProjectionId(owner.providerThread, turnId),
              nativeId,
            ),
            status: "running",
            title: "Session output (tool not identified)",
            type: "command_execution",
            input: `Native output source: ${page.source}. Session capture only; no command identity or exit status is inferred.`,
            startedAt: DateTime.makeUnsafe(startedAt),
            completedAt: null,
            updatedAt: at,
          },
        });
        yield* input.emit({
          type: "command_output.delta",
          driver,
          threadId: owner.threadId,
          itemId,
          chunk: first ? page.output : page.chunk,
          ...(first ? { replace: true } : {}),
          ...(page.truncated ? { truncated: true } : {}),
        });
      }
      if (finished) {
        for (const [source, page] of capture.sources) {
          const nativeId = nativeSessionProjectionId(
            owner.providerThread,
            `session-output:${sessionId}:${turnId}:${source}`,
          );
          yield* input.emit({
            type: "turn_item.updated",
            driver,
            turnItem: {
              id: input.ids.derive.turnItemFromProviderItem({ driver, nativeItemId: nativeId }),
              threadId: owner.threadId,
              runId: owner.runId,
              nodeId: owner.rootNodeId,
              providerThreadId: owner.providerThread.id,
              providerTurnId,
              nativeItemRef: { driver, nativeId, strength: "strong" },
              parentItemId: null,
              ordinal: input.itemOrdinal(
                nativeSessionProjectionId(owner.providerThread, turnId),
                nativeId,
              ),
              status: "completed",
              title: "Session output capture ended",
              type: "command_execution",
              output: page.output,
              outputTruncated: page.truncated,
              input: `Native output source: ${source}. Capture ended with the conversation turn; command exit status is unknown.`,
              startedAt: DateTime.makeUnsafe(startedAt),
              completedAt: at,
              updatedAt: at,
            },
          });
        }
        captures.delete(key);
        completed.add(key);
        if (completed.size > 1024) completed.delete(completed.values().next().value!);
      }
    }),
  };
}
