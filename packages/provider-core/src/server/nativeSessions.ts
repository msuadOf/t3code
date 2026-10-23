import type * as Effect from "effect/Effect";
import type { ProviderDriverError } from "./errors.ts";

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
}

export interface NativeHistoryItem {
  readonly id: string;
  readonly type: "user" | "assistant" | "reasoning" | "tool";
  readonly text: string;
  readonly status: "running" | "completed" | "failed" | "cancelled";
  readonly title?: string;
}

export interface NativeHistoryTurn {
  readonly id: string;
  readonly status: "running" | "completed" | "failed" | "interrupted";
  readonly startedAt: number;
  readonly completedAt: number | null;
  readonly items: ReadonlyArray<NativeHistoryItem>;
}

/** Observation must never resume a second writer or mutate the native session. */
export interface NativeSessionSource {
  readonly list: Effect.Effect<ReadonlyArray<NativeSession>, ProviderDriverError>;
  readonly history: (
    sessionId: string,
  ) => Effect.Effect<ReadonlyArray<NativeHistoryTurn>, ProviderDriverError>;
}
