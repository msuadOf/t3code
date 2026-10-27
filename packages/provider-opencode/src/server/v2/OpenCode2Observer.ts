// @effect-diagnostics nodeBuiltinImport:off globalTimers:off globalFetch:off globalDate:off - Detached observer discovery is process-local infrastructure.
import * as NodeFSP from "node:fs/promises";
import * as NodeCrypto from "node:crypto";
import * as NodePath from "node:path";
import * as NodeChildProcess from "node:child_process";
import * as NodeSea from "node:sea";
import { OBSERVER_REVISION } from "./OpenCode2ObserverCache.ts";

interface Registration {
  readonly version: number;
  readonly revision?: number;
  readonly port: number;
  readonly token: string;
  readonly pid: number;
}
const pause = (ms: number) => new Promise<void>((resolve) => setTimeout(resolve, ms));

function alive(pid: number): boolean {
  if (!Number.isSafeInteger(pid) || pid <= 0) return false;
  try {
    process.kill(pid, 0);
    return true;
  } catch (error) {
    return (error as NodeJS.ErrnoException).code === "EPERM";
  }
}

/** Discovery contains only a loopback port and a local bearer token; native credentials travel over stdin. */
export async function ensureObserver(input: {
  readonly stateDir: string;
  readonly baseUrl: string;
  readonly password: string;
}): Promise<{ readonly url: string; readonly token: string }> {
  const parent = NodePath.join(input.stateDir, "opencode-observers");
  await NodeFSP.mkdir(parent, { recursive: true, mode: 0o700 });
  const key = NodeCrypto.createHash("sha256")
    .update(input.baseUrl)
    .update("\0")
    .update(input.password)
    .digest("hex")
    .slice(0, 32);
  const directory = NodePath.join(parent, key);
  let nextLaunch = 0;
  for (let attempt = 0; attempt < 200; attempt++) {
    try {
      const registration = JSON.parse(
        await NodeFSP.readFile(NodePath.join(directory, "registration.json"), "utf8"),
      ) as Registration;
      let retiring = false;
      if (
        (registration.version !== 1 || registration.revision !== OBSERVER_REVISION) &&
        Number.isInteger(registration.port) &&
        typeof registration.token === "string" &&
        alive(registration.pid)
      ) {
        const shutdown = await fetch(`http://127.0.0.1:${registration.port}/shutdown`, {
          method: "POST",
          headers: { authorization: `Bearer ${registration.token}` },
          signal: AbortSignal.timeout(1000),
        }).catch(() => undefined);
        retiring = shutdown?.status === 204;
      }
      if (
        !retiring &&
        registration.version === 1 &&
        Number.isInteger(registration.port) &&
        registration.port > 0 &&
        registration.port <= 65535 &&
        typeof registration.token === "string" &&
        alive(registration.pid)
      ) {
        const url = `http://127.0.0.1:${registration.port}`;
        const controller = new AbortController();
        const timer = setTimeout(() => controller.abort(), 1000);
        try {
          const response = await fetch(`${url}/events`, {
            headers: { authorization: `Bearer ${registration.token}` },
            signal: controller.signal,
          });
          if (response.ok) {
            controller.abort();
            return { url, token: registration.token };
          }
        } catch {
          /* The worker may be reconnecting to OpenCode. */
        } finally {
          clearTimeout(timer);
        }
      }
    } catch {
      /* A worker may still be starting. */
    }
    try {
      const owner = JSON.parse(
        await NodeFSP.readFile(NodePath.join(directory, "owner.json"), "utf8"),
      ) as {
        pid: number;
      };
      if (!alive(owner.pid)) await NodeFSP.rm(directory, { recursive: true, force: true });
    } catch {
      // A killed worker can leave a lock before it writes its owner.
      const stat = await NodeFSP.stat(directory).catch(() => undefined);
      if (stat !== undefined && Date.now() - stat.mtimeMs > 10_000) {
        await NodeFSP.rm(directory, { recursive: true, force: true });
      }
    }
    const exists = await NodeFSP.stat(directory).then(
      () => true,
      () => false,
    );
    if (!exists && Date.now() >= nextLaunch) {
      const args = NodeSea.isSea()
        ? ["__opencode-observer"]
        : [process.argv[1]!, "__opencode-observer"];
      const worker = NodeChildProcess.spawn(process.execPath, args, {
        detached: true,
        stdio: ["pipe", "ignore", "ignore"],
        env: { ...process.env, ELECTRON_RUN_AS_NODE: "1" },
        windowsHide: true,
      });
      worker.on("error", () => {});
      worker.stdin.on("error", () => {});
      worker.stdin.end(
        JSON.stringify({ directory, baseUrl: input.baseUrl, password: input.password }),
      );
      worker.unref();
      nextLaunch = Date.now() + 1000;
    }
    await pause(100);
  }
  throw new Error("OpenCode observer did not become ready");
}
