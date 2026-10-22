export type NativeRequestId = string | number;

export interface NativeNotification {
  readonly method: string;
  readonly params?: unknown;
}

export interface NativeServerRequest extends NativeNotification {
  readonly id: NativeRequestId;
}

export interface NativeItem {
  readonly id: string;
  readonly type: string;
  readonly text?: string;
  readonly summary?: ReadonlyArray<string>;
  readonly content?: ReadonlyArray<unknown>;
  readonly aggregatedOutput?: string | null;
  readonly [key: string]: unknown;
}

export interface NativeTurn {
  readonly id: string;
  readonly status: string;
  readonly items: ReadonlyArray<NativeItem>;
  readonly [key: string]: unknown;
}

export interface NativeThread {
  readonly id: string;
  readonly cwd?: string;
  readonly title?: string | null;
  readonly name?: string | null;
  readonly preview?: string;
  readonly status?: { readonly type: string; readonly [key: string]: unknown };
  readonly turns: ReadonlyArray<NativeTurn>;
  readonly [key: string]: unknown;
}

interface NativeClientOptions {
  readonly onNotification: (notification: NativeNotification) => void;
  readonly onServerRequest: (request: NativeServerRequest) => void;
  readonly onClose?: (error: Error) => void;
  readonly requestTimeoutMs?: number;
  readonly socketFactory?: (url: string) => WebSocket;
}

interface PendingRequest {
  readonly onResult?: (value: unknown) => void;
  readonly resolve: (value: unknown) => void;
  readonly reject: (error: Error) => void;
  readonly timer: ReturnType<typeof setTimeout>;
}

const isRecord = (value: unknown): value is Record<string, unknown> =>
  typeof value === "object" && value !== null;
const isId = (value: unknown): value is NativeRequestId =>
  typeof value === "string" || typeof value === "number";

export class NativeCodexClient {
  private socket: WebSocket | undefined;
  private connecting: Promise<void> | undefined;
  private connected = false;
  private nextId = 1;
  private readonly pending = new Map<NativeRequestId, PendingRequest>();

  constructor(
    private readonly url: string,
    private readonly options: NativeClientOptions,
  ) {}

  connect(): Promise<void> {
    if (this.connecting) return this.connecting;
    this.connecting = new Promise<void>((resolve, reject) => {
      const socket = this.options.socketFactory?.(this.url) ?? new WebSocket(this.url);
      this.socket = socket;
      socket.addEventListener("open", () => {
        this.connected = true;
        resolve();
      });
      socket.addEventListener("message", (event: MessageEvent<unknown>) => {
        let message: unknown;
        try {
          if (typeof event.data !== "string") throw new Error("Expected a Codex text frame.");
          message = JSON.parse(event.data);
          if (!isRecord(message)) throw new Error("Invalid Codex RPC message.");
        } catch (cause) {
          const error = cause instanceof Error ? cause : new Error("Invalid Codex RPC message.");
          reject(error);
          this.disconnect(error);
          socket.close();
          return;
        }
        this.receive(message);
      });
      socket.addEventListener("error", () => {
        const error = new Error("Codex connection failed.");
        reject(error);
        this.disconnect(error);
      });
      socket.addEventListener("close", () => {
        const error = new Error(
          "Codex disconnected. Refresh the session before retrying an operation.",
        );
        reject(error);
        this.disconnect(error);
      });
    });
    return this.connecting;
  }

  request<T = unknown>(
    method: string,
    params?: unknown,
    onResult?: (result: T) => void,
  ): Promise<T> {
    if (!this.connected) return Promise.reject(new Error("Codex is not connected."));
    const id = `t3-native-${this.nextId++}`;
    return new Promise<unknown>((resolve, reject) => {
      const timer = setTimeout(() => {
        this.pending.delete(id);
        reject(
          new Error(
            `Codex ${method} timed out; the operation's outcome is unknown. Refresh before retrying.`,
          ),
        );
      }, this.options.requestTimeoutMs ?? 30_000);
      this.pending.set(id, {
        resolve,
        reject,
        timer,
        ...(onResult ? { onResult: (value: unknown) => onResult(value as T) } : {}),
      });
      try {
        this.send({ id, method, ...(params === undefined ? {} : { params }) });
      } catch (cause) {
        clearTimeout(timer);
        this.pending.delete(id);
        reject(cause instanceof Error ? cause : new Error("Could not send Codex request."));
      }
    }) as Promise<T>;
  }

  notify(method: string, params?: unknown): void {
    this.send({ method, ...(params === undefined ? {} : { params }) });
  }

  respond(id: NativeRequestId, result: unknown): void {
    this.send({ id, result });
  }

  respondError(
    id: NativeRequestId,
    error: { readonly code: number; readonly message: string },
  ): void {
    this.send({ id, error });
  }

  close(): void {
    this.disconnect(new Error("Codex connection closed."));
    this.socket?.close();
  }

  private send(message: unknown): void {
    if (!this.connected || !this.socket) throw new Error("Codex is not connected.");
    this.socket.send(JSON.stringify(message));
  }

  private disconnect(error: Error): void {
    const wasConnected = this.connected;
    this.connected = false;
    for (const request of this.pending.values()) {
      clearTimeout(request.timer);
      request.reject(error);
    }
    this.pending.clear();
    if (wasConnected) this.options.onClose?.(error);
  }

  private receive(message: Record<string, unknown>): void {
    if (typeof message.method === "string") {
      if (isId(message.id)) {
        this.options.onServerRequest({
          id: message.id,
          method: message.method,
          params: message.params,
        });
      } else {
        this.options.onNotification({ method: message.method, params: message.params });
      }
      return;
    }
    if (!isId(message.id)) return;
    const pending = this.pending.get(message.id);
    if (!pending) return;
    this.pending.delete(message.id);
    clearTimeout(pending.timer);
    if (isRecord(message.error)) {
      pending.reject(
        new Error(
          typeof message.error.message === "string"
            ? message.error.message
            : "Codex request failed.",
        ),
      );
    } else {
      try {
        // A native resume response is the live snapshot boundary. Hydrate synchronously
        // before the socket can deliver subsequent deltas and pending approval requests.
        pending.onResult?.(message.result);
        pending.resolve(message.result);
      } catch (cause) {
        pending.reject(
          cause instanceof Error ? cause : new Error("Could not apply Codex response."),
        );
      }
    }
  }
}

const mergeById = <T extends { readonly id: string }>(
  current: ReadonlyArray<T>,
  incoming: ReadonlyArray<T>,
  merge: (current: T, incoming: T) => T,
): ReadonlyArray<T> => {
  const next = [...current];
  for (const item of incoming) {
    const index = next.findIndex((existing) => existing.id === item.id);
    if (index === -1) next.push(item);
    else next[index] = merge(next[index]!, item);
  }
  return next;
};

// Snapshot hydration belongs in request onResult, using resume initialTurnsPage as the live boundary.
export const mergeThreadSnapshot = (
  current: NativeThread | undefined,
  incoming: NativeThread,
): NativeThread => {
  if (!current || current.id !== incoming.id) return incoming;
  return {
    ...current,
    ...incoming,
    turns: mergeById(current.turns, incoming.turns, (a, b) => ({
      ...a,
      ...b,
      items: mergeById(a.items, b.items, (old, fresh) => ({ ...old, ...fresh })),
    })),
  };
};

const updateTurn = (
  thread: NativeThread,
  turnId: string,
  update: (turn: NativeTurn) => NativeTurn,
): NativeThread => ({
  ...thread,
  turns: thread.turns.some((turn) => turn.id === turnId)
    ? thread.turns.map((turn) => (turn.id === turnId ? update(turn) : turn))
    : [...thread.turns, update({ id: turnId, status: "inProgress", items: [] })],
});

export const applyThreadNotification = (
  thread: NativeThread,
  notification: NativeNotification,
): NativeThread => {
  const params = notification.params;
  if (!isRecord(params) || params.threadId !== thread.id) return thread;
  if (
    notification.method === "thread/status/changed" &&
    isRecord(params.status) &&
    typeof params.status.type === "string"
  ) {
    return {
      ...thread,
      status: params.status as { readonly type: string; readonly [key: string]: unknown },
    };
  }
  if (
    (notification.method === "turn/started" || notification.method === "turn/completed") &&
    isRecord(params.turn) &&
    typeof params.turn.id === "string"
  ) {
    const turn = params.turn;
    return updateTurn(thread, turn.id as string, (old) => ({
      ...old,
      ...turn,
      items: Array.isArray(turn.items)
        ? mergeById(old.items, turn.items as NativeItem[], (a, b) => ({ ...a, ...b }))
        : old.items,
    }));
  }
  if (typeof params.turnId !== "string") return thread;
  if (
    (notification.method === "item/started" || notification.method === "item/completed") &&
    isRecord(params.item) &&
    typeof params.item.id === "string" &&
    typeof params.item.type === "string"
  ) {
    return updateTurn(thread, params.turnId, (turn) => ({
      ...turn,
      items: mergeById(turn.items, [params.item as NativeItem], (a, b) => ({ ...a, ...b })),
    }));
  }
  if (typeof params.itemId !== "string" || typeof params.delta !== "string") return thread;
  const delta = params.delta;
  const type =
    notification.method === "item/agentMessage/delta"
      ? "agentMessage"
      : notification.method === "item/plan/delta"
        ? "plan"
        : notification.method === "item/commandExecution/outputDelta"
          ? "commandExecution"
          : notification.method.startsWith("item/reasoning/")
            ? "reasoning"
            : undefined;
  if (!type) return thread;
  return updateTurn(thread, params.turnId, (turn) => {
    const existing = turn.items.find((item) => item.id === params.itemId) ?? {
      id: params.itemId as string,
      type,
    };
    let item: NativeItem;
    if (type === "reasoning") {
      const summary = notification.method === "item/reasoning/summaryTextDelta";
      const index = summary ? params.summaryIndex : params.contentIndex;
      if (typeof index !== "number" || !Number.isInteger(index) || index < 0 || index > 10_000)
        return turn;
      const key = summary ? "summary" : "content";
      const parts = [...(existing[key] ?? [])];
      while (parts.length <= index) parts.push("");
      parts[index] = `${typeof parts[index] === "string" ? parts[index] : ""}${delta}`;
      item = { ...existing, [key]: parts };
    } else if (type === "commandExecution") {
      item = { ...existing, aggregatedOutput: `${existing.aggregatedOutput ?? ""}${delta}` };
    } else {
      item = { ...existing, text: `${existing.text ?? ""}${delta}` };
    }
    return { ...turn, items: mergeById(turn.items, [item], (_old, fresh) => fresh) };
  });
};
