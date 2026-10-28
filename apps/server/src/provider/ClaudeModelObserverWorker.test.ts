// @effect-diagnostics nodeBuiltinImport:off globalFetch:off - Exercises actual loopback HTTP and SSE forwarding.
import * as NodeHttp from "node:http";
import { expect, it } from "vite-plus/test";
import { startClaudeModelObserver } from "./ClaudeModelObserverWorker.ts";

it("forwards native bytes and credentials unchanged while replaying identified provisional deltas", async () => {
  const sessionId = "11111111-2222-4333-8444-555555555555";
  const body = JSON.stringify({
    metadata: { user_id: JSON.stringify({ session_id: sessionId }) },
    messages: [],
  });
  let seenBody = "";
  let seenAuth: string | undefined;
  let seenPath: string | undefined;
  const payload = [
    { type: "message_start", message: { id: "msg_native" } },
    {
      type: "content_block_delta",
      index: 0,
      delta: { type: "thinking_delta", thinking: "thought" },
    },
    { type: "content_block_delta", index: 1, delta: { type: "text_delta", text: "回复" } },
    {
      type: "content_block_delta",
      index: 2,
      delta: { type: "input_json_delta", partial_json: '{"path":' },
    },
    { type: "message_stop" },
  ]
    .map((value) => `data: ${JSON.stringify(value)}\r\n\r\n`)
    .join("");
  const upstream = NodeHttp.createServer(async (request, response) => {
    for await (const chunk of request) seenBody += String(chunk);
    seenAuth = request.headers.authorization;
    seenPath = request.url;
    response.writeHead(200, { "content-type": "text/event-stream", "x-fixture": "original" });
    const bytes = Buffer.from(payload);
    const split = bytes.indexOf(Buffer.from("回复")) + 1;
    response.write(bytes.subarray(0, split));
    response.end(bytes.subarray(split));
  });
  await new Promise<void>((resolve) => upstream.listen(0, "127.0.0.1", resolve));
  const address = upstream.address();
  if (!address || typeof address === "string") throw new Error("No upstream address");
  const observer = await startClaudeModelObserver({
    upstreamUrl: `http://127.0.0.1:${address.port}/gateway`,
    token: "local-route",
  });
  const url = `http://127.0.0.1:${observer.port}`;
  try {
    expect((await fetch(`${url}/events?sessionId=${sessionId}`)).status).toBe(401);
    const result = await fetch(`${url}/proxy/local-route/v1/messages?beta=true`, {
      method: "POST",
      headers: { authorization: "Bearer fixture-model-secret", "anthropic-beta": "fixture" },
      body,
    });
    expect(await result.text()).toBe(payload);
    expect(result.headers.get("x-fixture")).toBe("original");
    expect(seenBody).toBe(body);
    expect(seenAuth).toBe("Bearer fixture-model-secret");
    expect(seenPath).toBe("/gateway/v1/messages?beta=true");
    const replay = await fetch(`${url}/events?sessionId=${sessionId}&after=1`, {
      headers: { authorization: "Bearer local-route" },
    });
    const reader = replay.body!.getReader();
    const first = await reader.read();
    await reader.cancel();
    const snapshot = JSON.parse(new TextDecoder().decode(first.value).split("\n\n")[0]!.slice(6));
    expect(snapshot.type).toBe("observer.snapshot");
    expect(snapshot.events).toHaveLength(5);
    expect(snapshot.events.map((event: { seq: number }) => event.seq)).toEqual([2, 3, 4, 5, 6]);
    expect(snapshot.events[0]).toMatchObject({
      sessionId,
      messageId: "msg_native",
      data: { delta: { thinking: "thought" } },
    });
    expect(snapshot.events[1].data.delta.text).toBe("回复");
    expect(snapshot.events[2].data.delta.partial_json).toBe('{"path":');
    expect(JSON.stringify(snapshot)).not.toContain("fixture-model-secret");
    expect(JSON.stringify(snapshot)).not.toContain("messages");
    // Disconnecting this reader cannot stop subsequent native model traffic.
    expect(
      (await fetch(`${url}/proxy/local-route/v1/messages/count_tokens`, { method: "POST", body }))
        .status,
    ).toBe(200);
  } finally {
    await observer.close();
    upstream.closeAllConnections();
    await new Promise<void>((resolve) => upstream.close(() => resolve()));
  }
});

it("retains a stable port while a managed worker is replaced and rejects a second owner", async () => {
  const NodeFSP = await import("node:fs/promises");
  const NodeFS = await import("node:fs");
  const NodeOS = await import("node:os");
  const NodePath = await import("node:path");
  const NodeChildProcess = await import("node:child_process");
  const NodeURL = await import("node:url");
  const directory = await NodeFSP.mkdtemp(NodePath.join(NodeOS.tmpdir(), "t3-claude-proxy-"));
  const entry = NodeURL.fileURLToPath(new URL("../bin.ts", import.meta.url));
  const registration = NodePath.join(directory, "registration.json");
  const ready = () =>
    new Promise<{ port: number; token: string; epoch: string }>((resolve, reject) => {
      const watcher = NodeFS.watch(directory, () => {
        void read();
      });
      watcher.once("error", reject);
      const read = async () => {
        const raw = await NodeFSP.readFile(registration, "utf8").catch(() => undefined);
        if (raw) {
          try {
            const value = JSON.parse(raw);
            watcher.close();
            resolve(value);
          } catch {
            /* Wait for atomic registration completion. */
          }
        }
      };
      void read();
    });
  const launch = (port?: number) => {
    const child = NodeChildProcess.spawn(process.execPath, [entry, "__claude-model-observer"], {
      stdio: ["pipe", "ignore", "ignore"],
    });
    const exited = new Promise<void>((resolve) => child.once("exit", () => resolve()));
    child.stdin.end(
      JSON.stringify({
        directory,
        upstreamUrl: "http://127.0.0.1:1",
        token: "a".repeat(64),
        ...(port ? { port } : {}),
      }),
    );
    return { child, exited };
  };
  const firstReady = ready();
  const first = launch();
  let second: ReturnType<typeof launch> | undefined;
  try {
    const a = await firstReady;
    const rival = launch();
    await rival.exited;
    expect(first.child.exitCode).toBeNull();
    first.child.kill("SIGTERM");
    await first.exited;
    await NodeFSP.unlink(registration);
    const secondReady = ready();
    second = launch(a.port);
    const b = await secondReady;
    expect(b.port).toBe(a.port);
    expect(b.token).toBe(a.token);
    expect(b.epoch).not.toBe(a.epoch);
    expect(
      (
        await fetch(`http://127.0.0.1:${b.port}/health`, {
          headers: { authorization: `Bearer ${b.token}` },
        })
      ).status,
    ).toBe(200);
  } finally {
    if (first.child.exitCode === null) first.child.kill("SIGTERM");
    await first.exited;
    if (second) {
      second.child.kill("SIGTERM");
      await second.exited;
    }
    await NodeFSP.rm(directory, { recursive: true, force: true });
  }
});

it("reports a replay gap when its bounded event window has evicted the prefix", async () => {
  const sessionId = "11111111-2222-4333-8444-555555555555";
  const body = JSON.stringify({ metadata: { user_id: JSON.stringify({ session_id: sessionId }) } });
  const nativeBytes = Array.from(
    { length: 20002 },
    () =>
      'data: {"type":"content_block_delta","index":0,"delta":{"type":"text_delta","text":"字"}}\n\n',
  ).join("");
  const upstream = NodeHttp.createServer((_request, response) => {
    response.writeHead(200, { "content-type": "text/event-stream" });
    response.end(nativeBytes);
  });
  await new Promise<void>((resolve) => upstream.listen(0, "127.0.0.1", resolve));
  const address = upstream.address();
  if (!address || typeof address === "string") throw new Error("No upstream");
  const observer = await startClaudeModelObserver({
    upstreamUrl: `http://127.0.0.1:${address.port}`,
    token: "fixture",
  });
  try {
    const base = `http://127.0.0.1:${observer.port}`;
    expect(
      await (await fetch(`${base}/proxy/fixture/v1/messages`, { method: "POST", body })).text(),
    ).toBe(nativeBytes);
    const { readClaudeModelActivitySnapshot } = await import("./ClaudeModelObserver.ts");
    const snapshot = await readClaudeModelActivitySnapshot({
      url: base,
      token: "fixture",
      sessionId,
    });
    expect(snapshot.gap).toBe(true);
    expect(snapshot.oldestSeq).toBe(4);
    expect(snapshot.events).toHaveLength(20000);
    expect(snapshot.events.at(-2)?.data).toMatchObject({ delta: { text: "字" } });
    expect(snapshot.events.at(-1)?.data).toMatchObject({ reason: "completed", complete: false });
  } finally {
    await observer.close();
    upstream.closeAllConnections();
    await new Promise<void>((resolve) => upstream.close(() => resolve()));
  }
});

it("keeps an in-flight native model response running after its activity reader disconnects", async () => {
  const sessionId = "11111111-2222-4333-8444-555555555555";
  const prefix = 'data: {"type":"message_start","message":{"id":"msg_live"}}\n\n';
  const suffix = 'data: {"type":"message_stop"}\n\n';
  let release!: () => void;
  const released = new Promise<void>((resolve) => {
    release = resolve;
  });
  const upstream = NodeHttp.createServer(async (request, response) => {
    for await (const _chunk of request) {
      /* Consume the native request. */
    }
    response.writeHead(200, { "content-type": "text/event-stream" });
    response.write(prefix);
    await released;
    response.end(suffix);
  });
  await new Promise<void>((resolve) => upstream.listen(0, "127.0.0.1", resolve));
  const address = upstream.address();
  if (!address || typeof address === "string") throw new Error("No upstream");
  const observer = await startClaudeModelObserver({
    upstreamUrl: `http://127.0.0.1:${address.port}`,
    token: "fixture",
  });
  try {
    const base = `http://127.0.0.1:${observer.port}`;
    const live = await fetch(`${base}/events?sessionId=${sessionId}`, {
      headers: { authorization: "Bearer fixture" },
    });
    const reader = live.body!.getReader();
    await reader.read(); // snapshot
    const native = await fetch(`${base}/proxy/fixture/v1/messages`, {
      method: "POST",
      body: JSON.stringify({ metadata: { user_id: JSON.stringify({ session_id: sessionId }) } }),
    });
    expect(new TextDecoder().decode((await reader.read()).value)).toContain("msg_live");
    await reader.cancel();
    release();
    expect(await native.text()).toBe(prefix + suffix);
  } finally {
    release();
    await observer.close();
    upstream.closeAllConnections();
    await new Promise<void>((resolve) => upstream.close(() => resolve()));
  }
});

it("cancels the upstream model request when native CLI disconnects before response headers", async () => {
  let accepted!: () => void;
  const incoming = new Promise<void>((resolve) => {
    accepted = resolve;
  });
  let disconnected!: () => void;
  const closed = new Promise<void>((resolve) => {
    disconnected = resolve;
  });
  const upstream = NodeHttp.createServer(async (request, response) => {
    response.on("close", disconnected);
    for await (const _chunk of request) {
      /* Consume the native request before waiting on the model. */
    }
    accepted();
  });
  await new Promise<void>((resolve) => upstream.listen(0, "127.0.0.1", resolve));
  const address = upstream.address();
  if (!address || typeof address === "string") throw new Error("No upstream");
  const observer = await startClaudeModelObserver({
    upstreamUrl: `http://127.0.0.1:${address.port}`,
    token: "fixture",
  });
  const controller = new AbortController();
  try {
    const result = fetch(`http://127.0.0.1:${observer.port}/proxy/fixture/v1/messages`, {
      method: "POST",
      body: "{}",
      signal: controller.signal,
    }).then(
      () => "unexpected-response",
      () => "cancelled",
    );
    await incoming;
    controller.abort();
    expect(await result).toBe("cancelled");
    await closed;
  } finally {
    controller.abort();
    await observer.close();
    upstream.closeAllConnections();
    await new Promise<void>((resolve) => upstream.close(() => resolve()));
  }
});
