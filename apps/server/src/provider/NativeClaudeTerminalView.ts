import { ProviderDriverKind, type OrchestrationV2ProviderTurn } from "@t3tools/contracts";
import type * as ProviderAdapter from "@t3tools/provider-core/server/ProviderAdapter";
import type * as IdAllocator from "@t3tools/provider-core/server/IdAllocator";
import { nativeSessionProjectionId } from "@t3tools/provider-core/server/nativeSessions";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import { NativeClaudeTerminalProgress } from "./NativeClaudeTerminalProgress.ts";

/** Native terminal pixels are a separate view; never parse them into agent semantics. */
export function makeNativeClaudeTerminalView(input: {
  ids: IdAllocator.IdAllocatorV2["Service"];
  emit: (event: ProviderAdapter.ProviderAdapterV2Event) => Effect.Effect<void>;
  itemOrdinal: (turnId: string, itemId: string) => number;
}) {
  const driver = ProviderDriverKind.make("claudeAgent");
  const screens = new Map<string, NativeClaudeTerminalProgress>();
  const published = new Map<string, string>();
  const lastPages = new Map<string, { text: string; truncated: boolean }>();
  const completed = new Set<string>();
  return {
    accept(sessionId: string, frame: Record<string, unknown>) {
      let screen = screens.get(sessionId);
      if (!screen) {
        screen = new NativeClaudeTerminalProgress(sessionId);
        screens.set(sessionId, screen);
      }
      screen.accept(frame);
    },
    drop(sessionId: string) {
      screens.delete(sessionId);
      for (const key of lastPages.keys())
        if (key.startsWith(`${sessionId}:`)) lastPages.delete(key);
      for (const key of completed) if (key.startsWith(`${sessionId}:`)) completed.delete(key);
      for (const key of published.keys())
        if (key.startsWith(`${sessionId}:`)) published.delete(key);
    },
    sample: Effect.fn("NativeClaudeTerminalView.sample")(function* (
      sessionId: string,
      owner: ProviderAdapter.ProviderAdapterV2TurnInput,
      providerTurnId: OrchestrationV2ProviderTurn["id"],
      turnId: string,
      startedAt: number,
      finished: boolean,
    ) {
      const key = `${sessionId}:${turnId}`;
      const current = screens.get(sessionId)?.snapshot();
      const page = current?.text ? current : finished ? lastPages.get(key) : undefined;
      if (!page?.text) return;
      if (completed.has(key) || (finished && !published.has(key))) return;
      if (finished) {
        completed.add(key);
        if (completed.size > 1024) completed.delete(completed.values().next().value!);
      }
      const signature = `${finished}:${page.truncated}:${page.text}`;
      if (published.get(key) === signature) return;
      published.set(key, signature);
      lastPages.set(key, page);
      // Only loaded sessions are retained; per-session completed-turn metadata is bounded.
      if (published.size > 256) {
        const oldest = published.keys().next().value!;
        published.delete(oldest);
        lastPages.delete(oldest);
      }
      const nativeId = nativeSessionProjectionId(owner.providerThread, `cli-view:${key}`);
      const itemId = input.ids.derive.turnItemFromProviderItem({ driver, nativeItemId: nativeId });
      const at = yield* DateTime.now;
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
          status: finished ? "completed" : "running",
          title: finished ? "Native CLI view capture ended" : "Native CLI view (partial)",
          type: "command_execution",
          input:
            "Read-only native terminal view. May include previous conversation and partial redraws; capture status is not command exit status.",
          startedAt: DateTime.makeUnsafe(startedAt),
          completedAt: finished ? at : null,
          updatedAt: at,
          ...(finished ? { output: page.text, outputTruncated: page.truncated } : {}),
        },
      });
      yield* input.emit({
        type: "command_output.delta",
        driver,
        threadId: owner.threadId,
        itemId,
        chunk: page.text,
        replace: true,
        ...(page.truncated ? { truncated: true } : {}),
      });
    }),
  };
}
