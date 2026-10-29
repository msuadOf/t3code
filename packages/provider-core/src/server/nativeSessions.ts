import type * as Effect from "effect/Effect";
import type * as Stream from "effect/Stream";
import type { ProviderDriverError } from "./errors.ts";
import type { OrchestrationV2ProviderThread, ThreadId } from "@t3tools/contracts";

/** Native Claude forks preserve message/tool UUIDs; only explicit fork targets change T3 identity. */
export const nativeSessionProjectionId = (
  thread: Pick<OrchestrationV2ProviderThread, "driver" | "nativeMetadata">,
  nativeId: string,
): string => {
  const namespace =
    thread.driver === "claudeAgent"
      ? thread.nativeMetadata?.claudeForkIdentityNamespace
      : undefined;
  return namespace === undefined ? nativeId : `claude-fork:${namespace}:${nativeId}`;
};

/** Native timestamps are Unix milliseconds throughout this boundary. */
export interface NativeSession {
  readonly id: string;
  readonly cwd: string;
  readonly title: string;
  readonly model?: string;
  readonly createdAt: number;
  readonly updatedAt: number;
  readonly archived: boolean;
  readonly active: boolean;
  /** The adapter can observe active work without starting another native writer. */
  readonly canObserveActive?: boolean;
  /** A durable native fork launch belongs only to this preselected T3 thread. */
  readonly requiresExistingOwner?: { readonly threadId: string };
}

export interface NativeHistoryItem {
  readonly id: string;
  readonly type: "user" | "assistant" | "reasoning" | "tool";
  /** Human input the native runtime consumed inside an existing turn. */
  readonly inputIntent?: "steer";
  readonly text: string;
  readonly status: "running" | "completed" | "failed" | "cancelled";
  readonly title?: string;
  /** Structured tool arguments; text remains the result/output. */
  readonly input?: Readonly<Record<string, unknown>>;
  readonly result?: Readonly<Record<string, unknown>>;
}

export interface NativeHistoryTurn {
  readonly id: string;
  readonly status: "running" | "completed" | "failed" | "interrupted";
  readonly startedAt: number;
  readonly completedAt: number | null;
  readonly items: ReadonlyArray<NativeHistoryItem>;
}

/** List/history are read-only; sharing.open is the explicit native ownership transition. */
export interface NativeSessionCandidate extends NativeSession {
  readonly historyAvailable?: boolean;
  readonly modelActivityAvailable?: boolean;
  readonly modelActivityEnabled?: boolean;
  readonly sharingState: "history" | "foreground" | "shared";
}

export interface NativeSessionSource {
  /** Inventory/execution wakeups; content deltas must not trigger full session scans. */
  readonly changes?: Stream.Stream<void, ProviderDriverError>;
  readonly identityDirectory?: string;
  /** Native model IDs confirmed by persisted history, independent of native entry UUIDs. */
  readonly confirmedModelMessageIds?: (
    sessionId: string,
  ) => Effect.Effect<ReadonlyArray<string>, ProviderDriverError>;

  readonly sharing?: {
    readonly setModelActivityEnabled?: (
      sessionId: string,
      enabled: boolean,
    ) => Effect.Effect<NativeSession, ProviderDriverError>;
    readonly list: Effect.Effect<ReadonlyArray<NativeSessionCandidate>, ProviderDriverError>;
    readonly open: (
      sessionId: string,
      options?: { readonly observeModelActivity?: boolean; readonly threadId?: ThreadId },
    ) => Effect.Effect<NativeSession, ProviderDriverError>;
  };
  readonly list: Effect.Effect<ReadonlyArray<NativeSession>, ProviderDriverError>;
  readonly history: (
    sessionId: string,
  ) => Effect.Effect<ReadonlyArray<NativeHistoryTurn>, ProviderDriverError>;
}
