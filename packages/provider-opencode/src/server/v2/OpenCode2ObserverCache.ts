// Bump when the worker changes so an older process retires after its active executions.
export const OBSERVER_REVISION = 1;

/** Cumulative values let subscribers join without replaying or double-appending deltas. */
export interface ObserverText {
  readonly type: "t3.observer.text";
  readonly data: {
    readonly sessionID: string;
    readonly assistantMessageID: string;
    readonly ordinal: number;
    readonly kind: "text" | "reasoning";
    readonly text: string;
  };
}
export interface ObserverGap {
  readonly type: "t3.observer.gap";
  readonly data: { readonly sessionID: string };
}
export interface ObserverReady {
  readonly type: "t3.observer.ready";
  readonly data: Record<string, never>;
}
export type ObserverEvent = ObserverText | ObserverGap | ObserverReady;
type NativeEvent = { readonly type: string; readonly data?: unknown };
const object = (value: unknown): Record<string, unknown> | undefined =>
  typeof value === "object" && value !== null ? (value as Record<string, unknown>) : undefined;

/** A missing delta cannot be repaired until OpenCode emits the completed block. */
export class OpenCode2ObserverCache {
  private readonly blocks = new Map<string, ObserverText>();
  private readonly active = new Set<string>();
  private readonly gaps = new Set<string>();
  private size = 0;
  private readonly limit: number;
  degraded = false;

  constructor(limit = 32 * 1024 * 1024) {
    this.limit = limit;
  }

  private key(session: string, message: string, kind: string, ordinal: number): string {
    return JSON.stringify([session, message, kind, ordinal]);
  }
  private clearSession(session: string): void {
    for (const [key, block] of this.blocks) {
      if (block.data.sessionID !== session) continue;
      this.size -= block.data.text.length * 2 + key.length * 2 + 512;
      this.blocks.delete(key);
    }
  }
  private addActive(session: string): void {
    if (this.active.has(session)) return;
    if (this.active.size >= 2048 || session.length > 1024) {
      this.degraded = true;
      return;
    }
    this.active.add(session);
  }
  private gap(session: string): ObserverGap {
    this.clearSession(session);
    this.gaps.add(session);
    return { type: "t3.observer.gap", data: { sessionID: session } };
  }
  markGap(sessions: ReadonlyArray<string>): void {
    const stillActive = new Set(sessions);
    for (const session of this.active) {
      if (!stillActive.has(session)) {
        this.active.delete(session);
        this.gaps.delete(session);
        this.clearSession(session);
      }
    }
    this.degraded = false;
    for (const session of sessions) {
      this.addActive(session);
      if (!this.degraded) this.gap(session);
    }
  }
  invalidate(): void {
    for (const session of this.active) this.gap(session);
  }
  get hasActive(): boolean {
    return this.degraded || this.active.size > 0;
  }
  snapshot(): ReadonlyArray<ObserverText | ObserverGap> {
    return [
      ...this.blocks.values(),
      ...[...this.gaps].map((sessionID) => ({
        type: "t3.observer.gap" as const,
        data: { sessionID },
      })),
    ];
  }
  accept(event: NativeEvent): NativeEvent | ObserverText | ObserverGap | undefined {
    const data = object(event.data);
    const session = data?.sessionID;
    if (data === undefined || typeof session !== "string") return event;
    if (event.type === "session.execution.started") {
      this.clearSession(session);
      this.gaps.delete(session);
      this.addActive(session);
      return event;
    }
    if (
      [
        "session.execution.succeeded",
        "session.execution.failed",
        "session.execution.interrupted",
      ].includes(event.type)
    ) {
      this.clearSession(session);
      this.active.delete(session);
      this.gaps.delete(session);
      return event;
    }
    const match = /^session\.(text|reasoning)\.(started|delta|ended)$/.exec(event.type);
    if (!match || typeof data.assistantMessageID !== "string" || typeof data.ordinal !== "number")
      return event;
    this.addActive(session);
    const kind = match[1] as "text" | "reasoning";
    const key = this.key(session, data.assistantMessageID, kind, data.ordinal);
    if (match[2] === "ended") {
      const previous = this.blocks.get(key);
      if (previous) this.size -= previous.data.text.length * 2 + key.length * 2 + 512;
      this.blocks.delete(key);
      return event;
    }
    if (this.gaps.has(session)) return undefined;
    if (match[2] !== "delta" || typeof data.delta !== "string") return event;
    const previous = this.blocks.get(key);
    const extra = data.delta.length * 2 + (previous === undefined ? key.length * 2 + 512 : 0);
    if (this.size + extra > this.limit || (this.blocks.size >= 4096 && previous === undefined))
      return this.gap(session);
    const next: ObserverText = {
      type: "t3.observer.text",
      data: {
        sessionID: session,
        assistantMessageID: data.assistantMessageID,
        ordinal: data.ordinal,
        kind,
        text: (previous?.data.text ?? "") + data.delta,
      },
    };
    this.blocks.set(key, next);
    this.size += extra;
    return event;
  }
}
