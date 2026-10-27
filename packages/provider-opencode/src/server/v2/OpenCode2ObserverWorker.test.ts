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
  it("delivers an ordered snapshot and live updates after an earlier subscriber disconnects", async () => {
    const directory = await NodeFSP.mkdtemp(NodePath.join(NodeOS.tmpdir(), "t3-observer-test-"));
    const lock = NodePath.join(directory, "observer");
    let nativeResponse: NodeHttp.ServerResponse | undefined;
    const native = NodeHttp.createServer((request, response) => {
      if (request.url === "/api/session/active") {
        response.writeHead(200, { "content-type": "application/json" }).end('{"data":{}}');
      } else if (request.url === "/api/event") {
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
