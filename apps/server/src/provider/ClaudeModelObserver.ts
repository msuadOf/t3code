// @effect-diagnostics nodeBuiltinImport:off globalTimers:off globalFetch:off globalDate:off - Detached sidecar discovery is process infrastructure.
import * as NodeFSP from "node:fs/promises";
import * as NodePath from "node:path";
import * as NodeCrypto from "node:crypto";
import * as NodeChildProcess from "node:child_process";
import * as NodeSea from "node:sea";

interface Registration {
  version: number;
  pid: number;
  port: number;
  token: string;
  epoch: string;
}
const alive = (pid: number) => {
  try {
    process.kill(pid, 0);
    return true;
  } catch (error) {
    return (error as NodeJS.ErrnoException).code === "EPERM";
  }
};
const readRegistration = async (directory: string): Promise<Registration | undefined> => {
  try {
    const value = JSON.parse(
      await NodeFSP.readFile(NodePath.join(directory, "registration.json"), "utf8"),
    ) as Registration;
    return value.version === 1 &&
      Number.isSafeInteger(value.pid) &&
      value.pid > 0 &&
      Number.isSafeInteger(value.port) &&
      value.port > 0 &&
      value.port < 65536 &&
      /^[a-f0-9]{64}$/.test(value.token) &&
      typeof value.epoch === "string"
      ? value
      : undefined;
  } catch {
    return undefined;
  }
};

/** Explicit opt-in only. A reused sidecar continues forwarding when the T3 host disconnects. */
export async function ensureClaudeModelObserver(input: {
  readonly stateDir: string;
  readonly upstreamUrl: string;
}) {
  const upstream = new URL(input.upstreamUrl);
  if (
    !["http:", "https:"].includes(upstream.protocol) ||
    upstream.username ||
    upstream.password ||
    upstream.search ||
    upstream.hash
  )
    throw new Error("Use an explicit HTTP(S) Messages upstream without URL credentials");
  const directory = NodePath.join(
    input.stateDir,
    "claude-model-observers",
    NodeCrypto.createHash("sha256").update(upstream.href).digest("hex"),
  );
  await NodeFSP.mkdir(directory, { recursive: true, mode: 0o700 });
  for (let attempt = 0; attempt < 150; attempt++) {
    const registration = await readRegistration(directory);
    if (registration && alive(registration.pid)) {
      const url = `http://127.0.0.1:${registration.port}`;
      const response = await fetch(`${url}/health`, {
        headers: { authorization: `Bearer ${registration.token}` },
        signal: AbortSignal.timeout(1000),
      }).catch(() => undefined);
      if (response?.ok) {
        const health = (await response.json()) as { epoch?: string };
        if (health.epoch === registration.epoch)
          return {
            url,
            token: registration.token,
            epoch: registration.epoch,
            modelBaseUrl: `${url}/proxy/${registration.token}`,
          };
      }
    }
    let ownerAlive = false;
    try {
      const owner = JSON.parse(
        await NodeFSP.readFile(NodePath.join(directory, "owner.lock"), "utf8"),
      ) as {
        pid: number;
      };
      ownerAlive = Number.isSafeInteger(owner.pid) && owner.pid > 0 && alive(owner.pid);
      if (!ownerAlive) await NodeFSP.unlink(NodePath.join(directory, "owner.lock")).catch(() => {});
    } catch {
      // A process killed before writing its PID can leave an incomplete lock.
      const stat = await NodeFSP.stat(NodePath.join(directory, "owner.lock")).catch(
        () => undefined,
      );
      if (stat && Date.now() - stat.mtimeMs > 10000)
        await NodeFSP.unlink(NodePath.join(directory, "owner.lock")).catch(() => {});
    }
    if (!ownerAlive && attempt % 10 === 0) {
      const args = NodeSea.isSea()
        ? ["__claude-model-observer"]
        : [process.argv[1]!, "__claude-model-observer"];
      const child = NodeChildProcess.spawn(process.execPath, args, {
        detached: true,
        stdio: ["pipe", "ignore", "ignore"],
        env: { ...process.env, ELECTRON_RUN_AS_NODE: "1" },
        windowsHide: true,
      });
      child.on("error", () => {});
      child.stdin.on("error", () => {});
      child.stdin.end(
        JSON.stringify({
          directory,
          upstreamUrl: upstream.href,
          token: registration?.token ?? NodeCrypto.randomBytes(32).toString("hex"),
          ...(registration ? { port: registration.port } : {}),
        }),
      );
      child.unref();
    }
    await new Promise<void>((resolve) => setTimeout(resolve, 100));
  }
  throw new Error("Claude model observer is unavailable; existing native routes were not changed");
}

export interface ClaudeModelActivitySnapshot {
  readonly type: "observer.snapshot";
  readonly epoch: string;
  readonly seq: number;
  readonly oldestSeq: number;
  readonly gap: boolean;
  readonly events: ReadonlyArray<import("./ClaudeModelObserverWorker.ts").ClaudeModelActivity>;
}

/** Server-only transport. Neither its URL nor its local bearer token belongs in client RPC payloads. */
export async function* observeClaudeModelActivity(input: {
  readonly url: string;
  readonly token: string;
  readonly sessionId: string;
  readonly after?: number;
  readonly signal?: AbortSignal;
}): AsyncGenerator<
  ClaudeModelActivitySnapshot | import("./ClaudeModelObserverWorker.ts").ClaudeModelActivity
> {
  const url = new URL("/events", input.url);
  url.searchParams.set("sessionId", input.sessionId);
  url.searchParams.set("after", String(input.after ?? 0));
  const response = await fetch(url, {
    headers: { authorization: `Bearer ${input.token}` },
    ...(input.signal ? { signal: input.signal } : {}),
  });
  if (!response.ok || !response.body) throw new Error("Claude activity observer unavailable");
  const reader = response.body.getReader();
  const decoder = new TextDecoder();
  let buffered = "";
  try {
    while (true) {
      const chunk = await reader.read();
      if (chunk.done) return;
      buffered += decoder.decode(chunk.value, { stream: true });
      if (buffered.length > 32 * 1024 * 1024)
        throw new Error("Claude activity observer frame exceeds limit");
      let at: number;
      while ((at = buffered.indexOf("\n\n")) !== -1) {
        const frame = buffered.slice(0, at);
        buffered = buffered.slice(at + 2);
        const line = frame.split("\n").find((value) => value.startsWith("data: "));
        if (line)
          yield JSON.parse(line.slice(6)) as
            | ClaudeModelActivitySnapshot
            | import("./ClaudeModelObserverWorker.ts").ClaudeModelActivity;
      }
    }
  } finally {
    await reader.cancel().catch(() => {});
  }
}

export async function readClaudeModelActivitySnapshot(
  input: Parameters<typeof observeClaudeModelActivity>[0],
): Promise<ClaudeModelActivitySnapshot> {
  for await (const frame of observeClaudeModelActivity(input)) {
    if (frame.type === "observer.snapshot" && "events" in frame)
      return frame as ClaudeModelActivitySnapshot;
    throw new Error("Claude activity observer omitted snapshot");
  }
  throw new Error("Claude activity observer closed before snapshot");
}

/** Read-only discovery for observers; it never starts a proxy or changes native routing. */
export async function discoverClaudeModelObserver(input: {
  readonly stateDir: string;
  readonly upstreamUrl: string;
}) {
  const upstream = new URL(input.upstreamUrl);
  if (
    !["http:", "https:"].includes(upstream.protocol) ||
    upstream.username ||
    upstream.password ||
    upstream.search ||
    upstream.hash
  )
    return undefined;
  const directory = NodePath.join(
    input.stateDir,
    "claude-model-observers",
    NodeCrypto.createHash("sha256").update(upstream.href).digest("hex"),
  );
  const registration = await readRegistration(directory);
  if (!registration || !alive(registration.pid)) return undefined;
  const url = `http://127.0.0.1:${registration.port}`;
  try {
    const response = await fetch(`${url}/health`, {
      headers: { authorization: `Bearer ${registration.token}` },
      signal: AbortSignal.timeout(1000),
    });
    if (!response.ok) return undefined;
    const health = (await response.json()) as { epoch?: string };
    return health.epoch === registration.epoch
      ? { url, token: registration.token, epoch: registration.epoch }
      : undefined;
  } catch {
    return undefined;
  }
}
