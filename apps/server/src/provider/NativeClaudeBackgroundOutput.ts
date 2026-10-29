import {
  ProviderDriverKind,
  type OrchestrationV2ProviderTurn,
  type OrchestrationV2TurnItem,
} from "@t3tools/contracts";
import type * as ProviderAdapter from "@t3tools/provider-core/server/ProviderAdapter";
import type * as IdAllocator from "@t3tools/provider-core/server/IdAllocator";
import { nativeSessionProjectionId } from "@t3tools/provider-core/server/nativeSessions";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Semaphore from "effect/Semaphore";
import { NativeClaudeCommandOutput } from "./NativeClaudeCommandOutput.ts";
import type { NativeClaudeBackgroundCommand } from "./NativeClaudeBackgroundCommands.ts";

export const makeNativeClaudeBackgroundOutput = Effect.fn("NativeClaudeBackgroundOutput.make")(
  function* (input: {
    readonly ids: IdAllocator.IdAllocatorV2["Service"];
    readonly emit: (event: ProviderAdapter.ProviderAdapterV2Event) => Effect.Effect<void>;
    readonly itemOrdinal: (turnId: string, itemId: string) => number;
  }) {
    const ids = input.ids;
    const driver = ProviderDriverKind.make("claudeAgent");
    type Binding = {
      readonly key: string;
      readonly sessionId: string;
      latest: NativeClaudeBackgroundCommand;
      readonly owner: ProviderAdapter.ProviderAdapterV2TurnInput;
      readonly item: Extract<OrchestrationV2TurnItem, { type: "command_execution" }>;
      readonly reader: NativeClaudeCommandOutput;
      readonly lock: Semaphore.Semaphore;
      output: string | undefined;
      truncated: boolean;
      unavailable: boolean;
    };
    const bindings = new Map<string, Binding>();
    const finished = new Set<string>();
    const keyOf = (sessionId: string, command: NativeClaudeBackgroundCommand) =>
      `${sessionId}:${command.toolId}:${command.taskId}`;
    const emitItem = Effect.fn("NativeClaudeBackgroundOutput.publish")(function* (
      binding: Binding,
      terminal: boolean,
    ) {
      const at = yield* DateTime.now;
      if (bindings.get(binding.key) !== binding) return;
      yield* input.emit({
        type: "turn_item.updated",
        driver,
        turnItem: {
          ...binding.item,
          status: terminal ? binding.latest.status : "running",
          completedAt: terminal
            ? binding.latest.completedAt === undefined
              ? at
              : DateTime.makeUnsafe(binding.latest.completedAt)
            : null,
          updatedAt: at,
          ...(terminal && binding.output !== undefined ? { output: binding.output } : {}),
          ...(terminal && binding.truncated ? { outputTruncated: true } : {}),
        },
      });
    });
    const sample = Effect.fn("NativeClaudeBackgroundOutput.sample")(function* (
      key: string,
      binding: Binding,
    ) {
      if (bindings.get(key) !== binding) return false;
      const terminal = binding.latest.status !== "running";
      if (!binding.unavailable) {
        const read = yield* Effect.tryPromise(() =>
          binding.reader.sample({ final: terminal }),
        ).pipe(Effect.result);
        if (bindings.get(key) !== binding) return false;
        if (read._tag === "Success") {
          const page = read.success;
          const sourceGap = page.truncated && !binding.truncated;
          binding.output = page.output;
          binding.truncated ||= page.truncated;
          if (page.chunk.length > 0 || sourceGap)
            yield* input.emit({
              type: "command_output.delta",
              driver,
              threadId: binding.item.threadId,
              itemId: binding.item.id,
              chunk: page.chunk,
              ...(sourceGap ? { truncated: true } : {}),
            });
          if (page.hasMore) return true;
        } else {
          binding.unavailable = true;
          binding.truncated = true;
          const nativeId = `${binding.item.nativeItemRef!.nativeId}:output-unavailable`;
          const at = yield* DateTime.now;
          yield* input.emit({
            type: "turn_item.updated",
            driver,
            turnItem: {
              ...binding.item,
              id: ids.derive.turnItemFromProviderItem({ driver, nativeItemId: nativeId }),
              parentItemId: binding.item.id,
              ordinal: input.itemOrdinal(
                nativeSessionProjectionId(binding.owner.providerThread, binding.latest.turnId),
                nativeId,
              ),
              type: "system_notice",
              status: "completed",
              title: "Command output",
              completedAt: at,
              updatedAt: at,
              message:
                "The native background command output is unavailable. Claude's task status is still being observed.",
            },
          });
        }
      }
      if (terminal) {
        yield* emitItem(binding, true);
        bindings.delete(key);
        finished.add(key);
      }
      return false;
    });
    yield* Effect.gen(function* () {
      while (true) {
        for (const [key, binding] of bindings) {
          while (yield* sample(key, binding).pipe(binding.lock.withPermit)) {
            /* Drain available bytes before waiting. */
          }
        }
        yield* Effect.sleep("100 millis");
      }
    }).pipe(Effect.forkScoped);
    return {
      drop(sessionId: string) {
        for (const [key, binding] of bindings)
          if (binding.sessionId === sessionId) bindings.delete(key);
        for (const key of finished) if (key.startsWith(`${sessionId}:`)) finished.delete(key);
      },
      hasPending(sessionId: string) {
        return [...bindings.values()].some((binding) => binding.sessionId === sessionId);
      },
      bind: Effect.fn("NativeClaudeBackgroundOutput.bind")(function* (
        sessionId: string,
        command: NativeClaudeBackgroundCommand,
        owner: ProviderAdapter.ProviderAdapterV2TurnInput,
        providerTurnId: OrchestrationV2ProviderTurn["id"],
      ) {
        const key = keyOf(sessionId, command);
        if (finished.has(key) || bindings.has(key)) return;
        const nativeId = nativeSessionProjectionId(
          owner.providerThread,
          `background-command:${sessionId}:${command.toolId}:${command.taskId}`,
        );
        const at = yield* DateTime.now;
        const binding: Binding = {
          key,
          sessionId,
          latest: command,
          owner,
          output: undefined,
          truncated: false,
          unavailable: false,
          lock: yield* Semaphore.make(1),
          reader: new NativeClaudeCommandOutput({
            sessionId,
            taskId: command.taskId,
            outputPath: command.outputPath,
          }),
          item: {
            id: ids.derive.turnItemFromProviderItem({ driver, nativeItemId: nativeId }),
            threadId: owner.threadId,
            runId: owner.runId,
            nodeId: owner.rootNodeId,
            providerThreadId: owner.providerThread.id,
            providerTurnId,
            nativeItemRef: { driver, nativeId, strength: "strong" },
            parentItemId: ids.derive.turnItemFromProviderItem({
              driver,
              nativeItemId: nativeSessionProjectionId(owner.providerThread, command.toolId),
            }),
            ordinal: input.itemOrdinal(
              nativeSessionProjectionId(owner.providerThread, command.turnId),
              nativeId,
            ),
            status: "running",
            title: "Background command",
            type: "command_execution",
            input: command.command,
            startedAt: DateTime.makeUnsafe(command.startedAt),
            completedAt: null,
            updatedAt: at,
          },
        };
        bindings.set(key, binding);
        yield* emitItem(binding, false);
        while (yield* sample(key, binding).pipe(binding.lock.withPermit)) {
          /* Drain the initial native tail. */
        }
      }),
      update(sessionId: string, commands: readonly NativeClaudeBackgroundCommand[]) {
        for (const command of commands) {
          const binding = bindings.get(keyOf(sessionId, command));
          if (
            binding &&
            binding.latest.status === "running" &&
            binding.latest.outputPath === command.outputPath
          )
            binding.latest = command;
        }
      },
    };
  },
);
