import { describe, expect, it, vi } from "vite-plus/test";

import {
  NativeCodexClient,
  applyThreadNotification,
  mergeThreadSnapshot,
  type NativeNotification,
  type NativeServerRequest,
  type NativeThread,
} from "./protocol";

class FakeSocket extends EventTarget {
  readonly sent: Array<Record<string, unknown>> = [];
  send(message: string) {
    this.sent.push(JSON.parse(message));
  }
  close() {
    this.dispatchEvent(new Event("close"));
  }
  receive(message: unknown) {
    this.dispatchEvent(new MessageEvent("message", { data: JSON.stringify(message) }));
  }
}

const setup = async () => {
  const socket = new FakeSocket();
  const notifications: NativeNotification[] = [];
  const requests: NativeServerRequest[] = [];
  const client = new NativeCodexClient("ws://example/native", {
    socketFactory: () => socket as unknown as WebSocket,
    onNotification: (notification) => notifications.push(notification),
    onServerRequest: (request) => requests.push(request),
  });
  const connected = client.connect();
  socket.dispatchEvent(new Event("open"));
  await connected;
  return { client, socket, notifications, requests };
};

const emptyThread = (): NativeThread => ({ id: "native-thread", turns: [] });

describe("native Codex RPC client", () => {
  it("routes responses, forwards external requests and preserves real approval IDs", async () => {
    const { client, socket, requests, notifications } = await setup();
    try {
      expect(socket.sent).toEqual([]);
      const listed = client.request<{ data: string[] }>("thread/loaded/list", {});
      const id = socket.sent[0]?.id;
      socket.receive({ id, result: { data: ["native-thread"] } });
      await expect(listed).resolves.toEqual({ data: ["native-thread"] });
      socket.receive({
        id: 82,
        method: "item/tool/requestUserInput",
        params: { threadId: "native-thread" },
      });
      expect(requests[0]?.id).toBe(82);
      client.respond(82, { answers: {} });
      expect(socket.sent.at(-1)).toEqual({ id: 82, result: { answers: {} } });
      socket.receive({
        method: "serverRequest/resolved",
        params: { threadId: "native-thread", requestId: 82 },
      });
      expect(notifications.at(-1)?.method).toBe("serverRequest/resolved");
      socket.receive({ id: "unknown", method: "future/approval" });
      expect(requests.at(-1)?.id).toBe("unknown");
      expect(socket.sent).toHaveLength(2);
    } finally {
      client.close();
    }
  });

  it("rejects outstanding requests on disconnect without replaying them", async () => {
    const { client, socket } = await setup();
    const result = client.request("turn/start", { threadId: "native-thread" });
    const rejected = expect(result).rejects.toThrow("disconnected");
    socket.close();
    await rejected;
    expect(socket.sent).toHaveLength(1);
    await expect(client.request("thread/read", {})).rejects.toThrow("not connected");
  });

  it("reports an unknown outcome when a request times out", async () => {
    vi.useFakeTimers();
    try {
      const { client, socket } = await setup();
      try {
        const result = client.request("turn/start", { threadId: "native-thread" });
        const rejected = expect(result).rejects.toThrow("outcome is unknown");
        await vi.advanceTimersByTimeAsync(30_000);
        await rejected;
        expect(socket.sent).toHaveLength(1);
      } finally {
        client.close();
      }
    } finally {
      vi.useRealTimers();
    }
  });
});

describe("native Codex thread projection", () => {
  it("accepts externally started turns and merges streaming items with completed snapshots", () => {
    let thread = applyThreadNotification(emptyThread(), {
      method: "turn/started",
      params: {
        threadId: "native-thread",
        turn: { id: "external-turn", status: "inProgress", items: [] },
      },
    });
    for (const delta of ["Hello", " world"]) {
      thread = applyThreadNotification(thread, {
        method: "item/agentMessage/delta",
        params: {
          threadId: "native-thread",
          turnId: "external-turn",
          itemId: "reply",
          delta,
        },
      });
    }
    expect(thread.turns[0]?.items[0]?.text).toBe("Hello world");
    thread = applyThreadNotification(thread, {
      method: "item/completed",
      params: {
        threadId: "native-thread",
        turnId: "external-turn",
        item: { id: "reply", type: "agentMessage", text: "Hello world" },
      },
    });
    const snapshot: NativeThread = {
      id: "native-thread",
      turns: [
        {
          id: "external-turn",
          status: "completed",
          items: [{ id: "reply", type: "agentMessage", text: "Hello world" }],
        },
      ],
    };
    thread = mergeThreadSnapshot(thread, snapshot);
    thread = mergeThreadSnapshot(thread, snapshot);
    expect(thread.turns).toHaveLength(1);
    expect(thread.turns[0]?.items).toHaveLength(1);
    expect(thread.turns[0]?.items[0]?.text).toBe("Hello world");
    expect(thread.turns[0]?.status).toBe("completed");
  });

  it("keeps reasoning summary segments separate and appends command output", () => {
    let thread = emptyThread();
    for (const [summaryIndex, delta] of [
      [0, "Thinking"],
      [1, "Checking"],
      [0, "…"],
    ] as const) {
      thread = applyThreadNotification(thread, {
        method: "item/reasoning/summaryTextDelta",
        params: {
          threadId: thread.id,
          turnId: "turn",
          itemId: "reasoning",
          summaryIndex,
          delta,
        },
      });
    }
    for (const delta of ["first\n", "second\n"]) {
      thread = applyThreadNotification(thread, {
        method: "item/commandExecution/outputDelta",
        params: {
          threadId: thread.id,
          turnId: "turn",
          itemId: "command",
          delta,
        },
      });
    }
    expect(thread.turns[0]?.items[0]?.summary).toEqual(["Thinking…", "Checking"]);
    expect(thread.turns[0]?.items[1]?.aggregatedOutput).toBe("first\nsecond\n");
    const unrelated = applyThreadNotification(thread, {
      method: "turn/started",
      params: { threadId: "other" },
    });
    expect(unrelated).toBe(thread);
  });

  it("hydrates the atomic resume page before a synchronously following live delta", async () => {
    const socket = new FakeSocket();
    let thread = emptyThread();
    const order: string[] = [];
    const client = new NativeCodexClient("ws://example/native", {
      socketFactory: () => socket as unknown as WebSocket,
      onNotification: (notification) => {
        order.push("delta");
        thread = applyThreadNotification(thread, notification);
      },
      onServerRequest: () => {},
    });
    const connected = client.connect();
    socket.dispatchEvent(new Event("open"));
    await connected;
    try {
      const resumed = client.request<{
        thread: NativeThread;
        initialTurnsPage: { data: NativeThread["turns"] };
      }>(
        "thread/resume",
        {
          threadId: "native-thread",
          excludeTurns: true,
          initialTurnsPage: { limit: 30, sortDirection: "desc", itemsView: "full" },
        },
        (result) => {
          order.push("snapshot");
          thread = { ...result.thread, turns: result.initialTurnsPage.data.toReversed() };
        },
      );
      socket.receive({
        id: socket.sent[0]?.id,
        result: {
          thread: emptyThread(),
          initialTurnsPage: {
            data: [
              {
                id: "turn",
                status: "inProgress",
                items: [{ id: "item", type: "agentMessage", text: "Hello" }],
              },
            ],
          },
        },
      });
      socket.receive({
        method: "item/agentMessage/delta",
        params: {
          threadId: "native-thread",
          turnId: "turn",
          itemId: "item",
          delta: " world",
        },
      });
      expect(order).toEqual(["snapshot", "delta"]);
      expect(thread.turns[0]?.items[0]?.text).toBe("Hello world");
      await resumed;
      expect(thread.turns[0]?.items[0]?.text).toBe("Hello world");
    } finally {
      client.close();
    }
  });
});
