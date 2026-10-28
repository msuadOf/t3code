// @effect-diagnostics nodeBuiltinImport:off globalDate:off - Detached loopback infrastructure has no Effect runtime.
import * as NodeHttp from "node:http";
import * as NodeHttps from "node:https";
import * as NodeFSP from "node:fs/promises";
import * as NodePath from "node:path";
import * as NodeCrypto from "node:crypto";

export interface ClaudeModelActivity {
  readonly seq: number;
  readonly sessionId: string;
  readonly requestId: string;
  readonly messageId: string | null;
  readonly type: string;
  readonly data: Record<string, unknown>;
}
const object = (value: unknown): value is Record<string, unknown> =>
  typeof value === "object" && value !== null && !Array.isArray(value);
const UUID = /^[a-f0-9]{8}-(?:[a-f0-9]{4}-){3}[a-f0-9]{12}$/i;
const MAX_BODY = 64 * 1024 * 1024;
const MAX_CACHE = 16 * 1024 * 1024;
const hopHeaders = new Set([
  "host",
  "connection",
  "keep-alive",
  "proxy-authenticate",
  "proxy-authorization",
  "te",
  "trailer",
  "transfer-encoding",
  "upgrade",
]);
function headers(source: NodeHttp.IncomingHttpHeaders): NodeHttp.OutgoingHttpHeaders {
  const excluded = new Set([
    ...hopHeaders,
    ...(source.connection ?? "").split(",").map((value) => value.trim().toLowerCase()),
  ]);
  return Object.fromEntries(
    Object.entries(source).filter(([key]) => !excluded.has(key.toLowerCase())),
  );
}
function sessionIdentity(body: Buffer): string | undefined {
  try {
    const value: unknown = JSON.parse(body.toString("utf8"));
    if (!object(value) || !object(value.metadata)) return;
    const user: unknown =
      typeof value.metadata.user_id === "string"
        ? JSON.parse(value.metadata.user_id)
        : value.metadata.user_id;
    return object(user) && typeof user.session_id === "string" && UUID.test(user.session_id)
      ? user.session_id
      : undefined;
  } catch {
    return;
  }
}

/** The response tap never classifies a model response as a native conversation message. */
export async function startClaudeModelObserver(input: {
  readonly upstreamUrl: string;
  readonly token: string;
  readonly port?: number;
}) {
  const upstream = new URL(input.upstreamUrl);
  if (
    !["http:", "https:"].includes(upstream.protocol) ||
    upstream.username ||
    upstream.password ||
    upstream.search ||
    upstream.hash
  )
    throw new Error("Invalid observer upstream");
  const epoch = NodeCrypto.randomUUID();
  let sequence = 0;
  let bytes = 0;
  const cache: Array<{ event: ClaudeModelActivity; bytes: number }> = [];
  const subscribers = new Set<{ response: NodeHttp.ServerResponse; sessionId: string }>();
  const publish = (event: Omit<ClaudeModelActivity, "seq">) => {
    let data = { ...event, seq: ++sequence };
    let line = `data: ${JSON.stringify(data)}\n\n`;
    if (Buffer.byteLength(line) > MAX_CACHE) {
      data = { ...data, type: "observer.capture_gap", data: { reason: "event-too-large" } };
      line = `data: ${JSON.stringify(data)}\n\n`;
    }
    const size = Buffer.byteLength(line);
    cache.push({ event: data, bytes: size });
    bytes += size;
    while (bytes > MAX_CACHE || cache.length > 20000) bytes -= cache.shift()!.bytes;
    for (const client of subscribers)
      if (client.sessionId === data.sessionId) {
        client.response.write(line);
        if (client.response.writableLength > 1024 * 1024) client.response.destroy();
      }
  };
  const server = NodeHttp.createServer(async (request, response) => {
    const url = new URL(request.url ?? "/", "http://localhost");
    const authorized = request.headers.authorization === `Bearer ${input.token}`;
    if (url.pathname === "/health" && authorized) {
      response
        .writeHead(200, { "content-type": "application/json" })
        .end(JSON.stringify({ version: 1, epoch }));
      return;
    }
    if (url.pathname === "/events" && authorized) {
      const sessionId = url.searchParams.get("sessionId") ?? "";
      const after = Number(url.searchParams.get("after") ?? 0);
      if (!UUID.test(sessionId) || !Number.isSafeInteger(after) || after < 0) {
        response.writeHead(400).end();
        return;
      }
      response.writeHead(200, { "content-type": "text/event-stream", "cache-control": "no-store" });
      const oldest = cache[0]?.event.seq ?? sequence + 1;
      response.write(
        `data: ${JSON.stringify({ type: "observer.snapshot", epoch, seq: sequence, oldestSeq: oldest, gap: after > sequence || after < oldest - 1, events: cache.filter(({ event }) => event.sessionId === sessionId && event.seq > after).map(({ event }) => event) })}\n\n`,
      );
      const client = { response, sessionId };
      subscribers.add(client);
      response.on("close", () => subscribers.delete(client));
      return;
    }
    if (!url.pathname.startsWith(`/proxy/${input.token}/`)) {
      response.writeHead(401).end();
      return;
    }
    if (request.method !== "POST") {
      response.writeHead(405).end();
      return;
    }
    const relative = url.pathname.slice(`/proxy/${input.token}`.length);
    if (!/^\/(?:v1\/)?messages(?:\/count_tokens)?$/.test(relative)) {
      response.writeHead(404).end();
      return;
    }
    const chunks: Buffer[] = [];
    let length = 0;
    try {
      for await (const chunk of request) {
        length += chunk.length;
        if (length > MAX_BODY) {
          response.writeHead(413).end();
          return;
        }
        chunks.push(Buffer.from(chunk));
      }
    } catch {
      response.destroy();
      return;
    }
    const body = Buffer.concat(chunks);
    const sessionId = sessionIdentity(body);
    const requestId = NodeCrypto.randomUUID();
    let messageId: string | null = null;
    let messageStopped = false;
    let requestEnded = false;
    const finish = (reason: "completed" | "upstream-error" | "client-disconnected") => {
      if (!sessionId || relative.endsWith("count_tokens") || requestEnded) return;
      requestEnded = true;
      publish({
        sessionId,
        requestId,
        messageId,
        type: "observer.request_end",
        data: { reason, complete: messageStopped },
      });
    };
    const target = new URL(upstream.href);
    target.pathname = `${upstream.pathname.replace(/\/$/, "")}${relative}`;
    target.search = url.search;
    const nativeHeaders = headers(request.headers);
    // A caller's model credentials travel only on this request, never into observer registration/cache.
    const forward = (target.protocol === "https:" ? NodeHttps : NodeHttp).request(
      target,
      { method: request.method, headers: nativeHeaders },
      (native) => {
        response.writeHead(native.statusCode ?? 502, headers(native.headers));
        const observing = Boolean(
          sessionId &&
          !relative.endsWith("count_tokens") &&
          String(native.headers["content-type"]).includes("text/event-stream") &&
          !native.headers["content-encoding"],
        );
        if (
          sessionId &&
          native.headers["content-encoding"] &&
          String(native.headers["content-type"]).includes("text/event-stream")
        )
          publish({
            sessionId,
            requestId,
            messageId: null,
            type: "observer.capture_gap",
            data: { reason: "compressed-response" },
          });
        const decoder = new TextDecoder();
        let buffered = "";
        let stopped = false;
        const tap = (chunk: Buffer) => {
          if (!observing || stopped) return;
          buffered = (buffered + decoder.decode(chunk, { stream: true })).replace(/\r\n/g, "\n");
          if (buffered.length > 2 * 1024 * 1024) {
            publish({
              sessionId: sessionId!,
              requestId,
              messageId,
              type: "observer.capture_gap",
              data: { reason: "frame-too-large" },
            });
            stopped = true;
            buffered = "";
            return;
          }
          let at: number;
          while ((at = buffered.indexOf("\n\n")) !== -1) {
            const block = buffered.slice(0, at);
            buffered = buffered.slice(at + 2);
            try {
              const raw = block
                .split("\n")
                .filter((line) => line.startsWith("data:"))
                .map((line) => line.slice(5).trimStart())
                .join("\n");
              const value: unknown = JSON.parse(raw);
              if (!object(value) || typeof value.type !== "string") continue;
              if (
                value.type === "message_start" &&
                object(value.message) &&
                typeof value.message.id === "string"
              )
                messageId = value.message.id;
              if (value.type === "message_stop") messageStopped = true;
              if (
                [
                  "message_start",
                  "content_block_start",
                  "content_block_delta",
                  "content_block_stop",
                  "message_delta",
                  "message_stop",
                  "error",
                ].includes(value.type)
              )
                publish({
                  sessionId: sessionId!,
                  requestId,
                  messageId,
                  type: value.type,
                  data: value,
                });
            } catch {
              /* Malformed/unobserved SSE must not change the native response. */
            }
          }
        };
        native.on("data", tap);
        native.on("end", () => finish("completed"));
        native.on("error", () => {
          finish("upstream-error");
          response.destroy();
        });
        native.on("aborted", () => {
          finish("upstream-error");
          response.destroy();
        });
        native.pipe(response);
        response.on("close", () => {
          if (!response.writableFinished) {
            finish("client-disconnected");
            native.destroy();
          }
        });
      },
    );
    // Cancellation before upstream headers must stop the actual model request too.
    response.on("close", () => {
      if (!response.writableFinished) {
        finish("client-disconnected");
        forward.destroy();
      }
    });
    forward.on("error", () => {
      finish("upstream-error");
      if (!response.headersSent) response.writeHead(502).end();
      else response.destroy();
    });
    forward.end(body);
  });
  await new Promise<void>((resolve, reject) => {
    server.once("error", reject);
    server.listen(input.port ?? 0, "127.0.0.1", resolve);
  });
  const address = server.address();
  if (!address || typeof address === "string") throw new Error("Observer has no address");
  return {
    port: address.port,
    epoch,
    close: async () => {
      for (const client of subscribers) client.response.destroy();
      server.closeAllConnections();
      await new Promise<void>((resolve) => server.close(() => resolve()));
    },
  };
}

export async function runClaudeModelObserverWorker() {
  let raw = "";
  for await (const chunk of process.stdin) raw += String(chunk);
  const input = JSON.parse(raw) as {
    directory: string;
    upstreamUrl: string;
    token: string;
    port?: number;
  };
  await NodeFSP.mkdir(input.directory, { recursive: true, mode: 0o700 });
  let lock: NodeFSP.FileHandle;
  try {
    lock = await NodeFSP.open(NodePath.join(input.directory, "owner.lock"), "wx", 0o600);
  } catch {
    return;
  }
  try {
    await lock.writeFile(JSON.stringify({ pid: process.pid }));
    await lock.close();
    const observer = await startClaudeModelObserver(input);
    const registrationPath = NodePath.join(input.directory, "registration.json");
    const temporaryPath = `${registrationPath}.${process.pid}.tmp`;
    await NodeFSP.writeFile(
      temporaryPath,
      JSON.stringify({
        version: 1,
        pid: process.pid,
        port: observer.port,
        token: input.token,
        epoch: observer.epoch,
      }),
      { mode: 0o600 },
    );
    await NodeFSP.rename(temporaryPath, registrationPath);
    process.once("SIGTERM", () => {
      void observer.close().then(async () => {
        await NodeFSP.unlink(NodePath.join(input.directory, "owner.lock")).catch(() => {});
        process.exit(0);
      });
    });
  } catch (error) {
    await NodeFSP.unlink(NodePath.join(input.directory, "owner.lock")).catch(() => {});
    throw error;
  }
}
