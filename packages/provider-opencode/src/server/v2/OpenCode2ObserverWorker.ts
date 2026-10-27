// @effect-diagnostics nodeBuiltinImport:off globalTimers:off globalFetch:off globalDate:off - This detached worker has no Effect runtime.
import * as NodeFSP from "node:fs/promises";
import * as NodeHttp from "node:http";
import * as NodeCrypto from "node:crypto";
import * as NodePath from "node:path";
import { OBSERVER_REVISION, OpenCode2ObserverCache } from "./OpenCode2ObserverCache.ts";

interface WorkerConfig {
  readonly directory: string;
  readonly baseUrl: string;
  readonly password: string;
}

const sleep = (ms: number) => new Promise<void>((resolve) => setTimeout(resolve, ms));
const encode = (event: unknown) => `data: ${JSON.stringify(event)}\n\n`;

/** One worker owns one native subscription. Its cache and subscribers share one JS event loop. */
export async function runOpenCode2ObserverWorker(): Promise<void> {
  let input = "";
  for await (const chunk of process.stdin) input += String(chunk);
  const config = JSON.parse(input) as WorkerConfig;
  try {
    await NodeFSP.mkdir(config.directory, { mode: 0o700 });
  } catch (error) {
    if ((error as NodeJS.ErrnoException).code === "EEXIST") return;
    throw error;
  }
  const owner = NodePath.join(config.directory, "owner.json");
  await NodeFSP.writeFile(owner, JSON.stringify({ pid: process.pid }), { mode: 0o600 });
  const cache = new OpenCode2ObserverCache();
  const token = NodeCrypto.randomBytes(32).toString("hex");
  const clients = new Set<NodeHttp.ServerResponse>();
  let connected = false;
  let server: NodeHttp.Server | undefined;
  let nativeAbort: AbortController | undefined;
  let exiting = false;
  let exitWhenIdle = false;
  const disconnect = () => {
    connected = false;
    cache.invalidate();
    for (const client of clients) client.destroy();
    clients.clear();
  };
  const publish = (event: unknown) => {
    const line = encode(event);
    for (const client of clients) {
      // A slow client must reconnect to a fresh snapshot instead of consuming unbounded memory.
      client.write(line);
      if (client.writableLength > 96 * 1024 * 1024) client.destroy();
    }
  };
  try {
    server = NodeHttp.createServer((request, response) => {
      if (request.headers.authorization !== `Bearer ${token}`) {
        response.writeHead(401).end();
        return;
      }
      if (request.url === "/shutdown" && request.method === "POST") {
        if (cache.hasActive) {
          exitWhenIdle = true;
          response.writeHead(202).end();
          return;
        }
        response.writeHead(204).end();
        exiting = true;
        nativeAbort?.abort();
        return;
      }
      if (request.url !== "/events") {
        response.writeHead(404).end();
        return;
      }
      if (!connected || cache.degraded || clients.size >= 8) {
        response.writeHead(503).end();
        return;
      }
      response.writeHead(200, { "content-type": "text/event-stream", "cache-control": "no-store" });
      for (const block of cache.snapshot()) response.write(encode(block));
      response.write(encode({ type: "t3.observer.ready", data: {} }));
      clients.add(response);
      response.on("close", () => clients.delete(response));
    });
    await new Promise<void>((resolve, reject) => {
      server!.once("error", reject);
      server!.listen(0, "127.0.0.1", resolve);
    });
    const address = server.address();
    if (!address || typeof address === "string") throw new Error("Observer has no TCP address");
    const registration = NodePath.join(config.directory, "registration.json");
    const authorization = `Basic ${Buffer.from(`opencode:${config.password}`, "utf8").toString("base64")}`;
    let failureSince = 0;
    while (true) {
      try {
        const abort = new AbortController();
        nativeAbort = abort;
        const response = await fetch(new URL("/api/event", config.baseUrl), {
          headers: { authorization, accept: "text/event-stream" },
          signal: abort.signal,
        });
        if (!response.ok || !response.body)
          throw new Error(`OpenCode event status ${response.status}`);
        const activeResponse = await fetch(new URL("/api/session/active", config.baseUrl), {
          headers: { authorization },
          signal: AbortSignal.timeout(5000),
        });
        if (!activeResponse.ok) throw new Error("Could not check active OpenCode sessions");
        const active: unknown = await activeResponse.json();
        if (typeof active !== "object" || active === null || Array.isArray(active))
          throw new Error("Invalid active OpenCode sessions");
        const sessions = "data" in active ? active.data : active;
        if (typeof sessions !== "object" || sessions === null || Array.isArray(sessions))
          throw new Error("Invalid active OpenCode session data");
        cache.markGap(Object.keys(sessions));
        connected = true;
        failureSince = 0;
        await NodeFSP.writeFile(
          registration,
          JSON.stringify({
            version: 1,
            revision: OBSERVER_REVISION,
            port: address.port,
            token,
            pid: process.pid,
          }),
          { mode: 0o600 },
        );
        const reader = response.body.getReader();
        const decoder = new TextDecoder();
        let buffer = "";
        while (true) {
          const timeout = setTimeout(() => abort.abort(), 45_000);
          let result: Awaited<ReturnType<typeof reader.read>>;
          try {
            result = await reader.read();
          } finally {
            clearTimeout(timeout);
          }
          if (result.done) throw new Error("OpenCode event stream ended");
          buffer = (buffer + decoder.decode(result.value, { stream: true })).replaceAll(
            "\r\n",
            "\n",
          );
          if (buffer.length > 8 * 1024 * 1024) throw new Error("Oversized OpenCode event");
          let boundary: number;
          while ((boundary = buffer.indexOf("\n\n")) !== -1) {
            const frame = buffer.slice(0, boundary);
            buffer = buffer.slice(boundary + 2);
            const raw = frame
              .split("\n")
              .filter((line) => line.startsWith("data:"))
              .map((line) => line.slice(5).replace(/^ /, ""))
              .join("\n");
            if (!raw) {
              for (const client of clients) {
                client.write(": heartbeat\n\n");
                if (client.writableLength > 96 * 1024 * 1024) client.destroy();
              }
              continue;
            }
            let event: unknown;
            try {
              event = JSON.parse(raw);
            } catch {
              continue;
            }
            if (
              typeof event !== "object" ||
              event === null ||
              !("type" in event) ||
              typeof event.type !== "string"
            )
              continue;
            const wasDegraded = cache.degraded;
            const mapped = cache.accept({ ...event, type: event.type as string });
            if (!wasDegraded && cache.degraded) {
              for (const client of clients) client.destroy();
              clients.clear();
            }
            if (!cache.degraded && mapped !== undefined) publish(mapped);
            if (exitWhenIdle && !cache.hasActive) {
              exiting = true;
              abort.abort();
            }
          }
        }
      } catch {
        disconnect();
        if (exiting) return;
        failureSince ||= Date.now();
        // A replaced or stopped native service must not leave an orphan indefinitely.
        if (Date.now() - failureSince > 120_000) return;
        await sleep(1000);
      }
    }
  } finally {
    disconnect();
    server?.close();
    await NodeFSP.rm(config.directory, { recursive: true, force: true });
  }
}
