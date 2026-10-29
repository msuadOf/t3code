// @effect-diagnostics nodeBuiltinImport:off globalFetch:off - This exercises the detached process and loopback protocol.
import * as NodeChildProcess from "node:child_process";
import * as NodeFS from "node:fs";
import * as NodeFSP from "node:fs/promises";
import * as NodeHttp from "node:http";
import * as NodeOS from "node:os";
import * as NodePath from "node:path";
import * as NodeURL from "node:url";
import { describe, expect, it } from "vite-plus/test";

const waitFor = <T>(directory: string, read: () => Promise<T | undefined>) =>
  new Promise<T>((resolve, reject) => {
    const watcher = NodeFS.watch(directory, () => {
      void check();
    });
    watcher.on("error", reject);
    const check = async () => {
      const result = await read();
      if (result !== undefined) {
        watcher.close();
        resolve(result);
      }
    };
    void check().catch(reject);
  });

const frames = (response: Response) => {
  const reader = response.body!.getReader();
  const decoder = new TextDecoder();
  let buffer = "";
  return {
    async next(): Promise<{ type: string; data: Record<string, unknown> }> {
      while (!buffer.includes("\n\n")) {
        const chunk = await reader.read();
        if (chunk.done) throw new Error("Observer stream closed");
        buffer += decoder.decode(chunk.value, { stream: true });
      }
      const at = buffer.indexOf("\n\n");
      const frame = buffer.slice(0, at);
      buffer = buffer.slice(at + 2);
      return JSON.parse(frame.slice(6)) as { type: string; data: Record<string, unknown> };
    },
    close: () => reader.cancel(),
  };
};

describe("OpenCode observer process", () => {
  it("delivers ordered snapshots and long live updates after subscriber and native stream reconnects", async () => {
    const directory = await NodeFSP.mkdtemp(NodePath.join(NodeOS.tmpdir(), "t3-observer-test-"));
    const lock = NodePath.join(directory, "observer");
    let nativeResponse: NodeHttp.ServerResponse | undefined;
    let nativeConnections = 0;
    let active: Record<string, unknown> = {};
    const native = NodeHttp.createServer((request, response) => {
      if (request.url === "/api/session/active") {
        response
          .writeHead(200, { "content-type": "application/json" })
          .end(JSON.stringify({ data: active }));
      } else if (request.url === "/api/event") {
        nativeConnections += 1;
        nativeResponse = response;
        response.writeHead(200, { "content-type": "text/event-stream" });
        response.flushHeaders();
      } else response.writeHead(404).end();
    });
    await new Promise<void>((resolve) => native.listen(0, "127.0.0.1", resolve));
    const address = native.address();
    if (address === null || typeof address === "string") throw new Error("No native address");
    const entry = NodeURL.fileURLToPath(
      new URL("../../../../../apps/server/src/bin.ts", import.meta.url),
    );
    const worker = NodeChildProcess.spawn(process.execPath, [entry, "__opencode-observer"], {
      stdio: ["pipe", "ignore", "ignore"],
    });
    const exited = new Promise<void>((resolve) => worker.once("exit", () => resolve()));
    const registered = (async () => {
      await waitFor(directory, () =>
        NodeFSP.stat(lock).then(
          () => true,
          () => undefined,
        ),
      );
      return waitFor(lock, async () => {
        const raw = await NodeFSP.readFile(NodePath.join(lock, "registration.json"), "utf8").catch(
          () => undefined,
        );
        return raw === undefined ? undefined : (JSON.parse(raw) as { port: number; token: string });
      });
    })();
    worker.stdin.end(
      JSON.stringify({
        directory: lock,
        baseUrl: `http://127.0.0.1:${address.port}`,
        password: "fixture",
      }),
    );
    try {
      const info = await registered;
      const endpoint = `http://127.0.0.1:${info.port}`;
      const headers = { authorization: `Bearer ${info.token}` };
      expect((await fetch(`${endpoint}/events`)).status).toBe(401);
      const first = frames(await fetch(`${endpoint}/events`, { headers }));
      expect((await first.next()).type).toBe("t3.observer.ready");
      const emit = (type: string, data: unknown) =>
        nativeResponse!.write(`data: ${JSON.stringify({ type, data })}\n\n`);
      const block = { sessionID: "session", assistantMessageID: "message", ordinal: 0 };
      emit("session.execution.started", { sessionID: "session" });
      expect((await first.next()).type).toBe("session.execution.started");
      emit("session.text.delta", { ...block, delta: "prefix" });
      expect((await first.next()).data.delta).toBe("prefix");
      const second = frames(await fetch(`${endpoint}/events`, { headers }));
      expect((await second.next()).data.text).toBe("prefix");
      expect((await second.next()).type).toBe("t3.observer.ready");
      await first.close();
      emit("session.text.delta", { ...block, delta: " tail" });
      expect((await second.next()).data.delta).toBe(" tail");
      emit("session.execution.succeeded", { sessionID: "session" });
      expect((await second.next()).type).toBe("session.execution.succeeded");
      await second.close();

      const beforeDisconnect = frames(await fetch(`${endpoint}/events`, { headers }));
      expect((await beforeDisconnect.next()).type).toBe("t3.observer.ready");
      emit("session.execution.started", { sessionID: "session" });
      expect((await beforeDisconnect.next()).type).toBe("session.execution.started");
      emit("session.text.delta", { ...block, delta: "old prefix" });
      expect((await beforeDisconnect.next()).data.delta).toBe("old prefix");
      active = { session: { type: "running" } };
      // The registration rewrite occurs only after the native stream and active
      // snapshot have both reconnected. No timers or endpoint polling are needed.
      const nativeReconnected = new Promise<void>((resolve, reject) => {
        const watcher = NodeFS.watch(lock, (_event, file) => {
          if (file === "registration.json" && nativeConnections > 1) {
            watcher.close();
            resolve();
          }
        });
        watcher.on("error", reject);
      });
      nativeResponse!.destroy();
      await expect(beforeDisconnect.next()).rejects.toThrow();
      await nativeReconnected;
      const resumed = frames(await fetch(`${endpoint}/events`, { headers }));
      expect((await resumed.next()).type).toBe("t3.observer.gap");
      expect((await resumed.next()).type).toBe("t3.observer.ready");
      // The missing prefix is explicit; both its live suffix and a new block
      // remain observable before either block or execution ends.
      emit("session.text.delta", { ...block, delta: "unknown old tail" });
      expect((await resumed.next()).data).toMatchObject({
        ...block,
        text: "unknown old tail",
        prefixMissing: true,
      });
      for (let index = 0; index < 64; index++)
        emit("session.text.delta", { ...block, delta: " live suffix" });
      for (let index = 0; index < 64; index++)
        expect((await resumed.next()).data.delta).toBe(" live suffix");
      const nextBlock = { ...block, ordinal: 1 };
      emit("session.text.started", nextBlock);
      expect((await resumed.next()).type).toBe("session.text.started");
      const chunk = "LONG_STREAM_".repeat(40);
      for (let index = 0; index < 256; index++)
        emit("session.text.delta", { ...nextBlock, delta: chunk });
      for (let index = 0; index < 256; index++)
        expect((await resumed.next()).data.delta).toBe(chunk);
      const joined = frames(await fetch(`${endpoint}/events`, { headers }));
      expect((await joined.next()).type).toBe("t3.observer.gap");
      expect((await joined.next()).data).toMatchObject({
        ...block,
        text: "unknown old tail" + " live suffix".repeat(64),
        prefixMissing: true,
      });
      expect((await joined.next()).data).toMatchObject({
        ...nextBlock,
        kind: "text",
        text: chunk.repeat(256),
      });
      expect((await joined.next()).type).toBe("t3.observer.ready");
      emit("session.text.delta", { ...nextBlock, delta: " FINAL_LIVE_TOKEN" });
      expect((await resumed.next()).data.delta).toBe(" FINAL_LIVE_TOKEN");
      expect((await joined.next()).data.delta).toBe(" FINAL_LIVE_TOKEN");
      emit("session.execution.succeeded", { sessionID: "session" });
      expect((await resumed.next()).type).toBe("session.execution.succeeded");
      expect((await joined.next()).type).toBe("session.execution.succeeded");
      await resumed.close();
      await joined.close();
      expect((await fetch(`${endpoint}/shutdown`, { method: "POST", headers })).status).toBe(204);
      await exited;
    } finally {
      if (worker.exitCode === null) worker.kill();
      await exited;
      nativeResponse?.destroy();
      native.close();
      await NodeFSP.rm(directory, { recursive: true, force: true });
    }
  });
});
