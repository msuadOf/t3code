// @effect-diagnostics nodeBuiltinImport:off - Private native-worker routing files are process infrastructure.
import * as NodeFSP from "node:fs/promises";
import * as NodePath from "node:path";
import * as NodeCrypto from "node:crypto";
import { ensureClaudeModelObserver } from "./ClaudeModelObserver.ts";

export interface ClaudeModelObserverBinding {
  readonly observerStateDir: string;
  readonly upstreamUrl: string;
  readonly configDirectory: string;
  readonly instanceId: string;
  readonly nativeSessionId: string;
}
const UUID = /^[a-f0-9]{8}-(?:[a-f0-9]{4}-){3}[a-f0-9]{12}$/i;
const hash = (value: string) => NodeCrypto.createHash("sha256").update(value).digest("hex");
const instanceHash = (instanceId: string) => {
  if (typeof instanceId !== "string" || !instanceId.trim() || instanceId.length > 512)
    throw new Error("Invalid Claude observer provider instance");
  return hash(instanceId);
};
const upstreamHref = (value: string) => {
  let url: URL;
  try {
    url = new URL(value);
  } catch {
    throw new Error("Invalid Claude observer upstream");
  }
  if (
    !["http:", "https:"].includes(url.protocol) ||
    url.username ||
    url.password ||
    url.search ||
    url.hash
  )
    throw new Error("Invalid Claude observer upstream");
  return url.href;
};
export const deriveClaudeModelObserverStateDir = (input: {
  readonly stateDir: string;
  readonly instanceId: string;
}) =>
  NodePath.join(input.stateDir, "claude-model-observer-instances", instanceHash(input.instanceId));

const bindingPath = (input: {
  readonly stateDir: string;
  readonly instanceId: string;
  readonly nativeSessionId: string;
}) => {
  if (typeof input.nativeSessionId !== "string" || !UUID.test(input.nativeSessionId))
    throw new Error("Invalid Claude observer native session identity");
  return NodePath.join(
    input.stateDir,
    "claude-model-observer-bindings",
    instanceHash(input.instanceId),
    `${input.nativeSessionId}.json`,
  );
};
const validateBinding = (value: unknown, stateDir: string): ClaudeModelObserverBinding => {
  if (typeof value !== "object" || value === null || Array.isArray(value))
    throw new Error("Invalid Claude observer binding");
  const item = value as Record<string, unknown>;
  const fields = [
    "observerStateDir",
    "upstreamUrl",
    "configDirectory",
    "instanceId",
    "nativeSessionId",
  ];
  if (
    Object.keys(item).length !== fields.length ||
    fields.some((key) => typeof item[key] !== "string")
  )
    throw new Error("Invalid Claude observer binding");
  const binding = item as unknown as ClaudeModelObserverBinding;
  bindingPath({ ...binding, stateDir });
  if (
    !NodePath.isAbsolute(binding.configDirectory) ||
    binding.configDirectory.includes("\0") ||
    binding.observerStateDir !==
      deriveClaudeModelObserverStateDir({ stateDir, instanceId: binding.instanceId })
  )
    throw new Error("Invalid Claude observer binding paths");
  upstreamHref(binding.upstreamUrl);
  return binding;
};
const atomicPrivateJson = async (path: string, value: unknown) => {
  await NodeFSP.mkdir(NodePath.dirname(path), { recursive: true, mode: 0o700 });
  const temporary = `${path}.${NodeCrypto.randomUUID()}.tmp`;
  try {
    await NodeFSP.writeFile(temporary, JSON.stringify(value), { mode: 0o600, flag: "wx" });
    await NodeFSP.rename(temporary, path);
  } finally {
    await NodeFSP.unlink(temporary).catch(() => {});
  }
};

export async function readClaudeModelObserverBinding(input: {
  readonly stateDir: string;
  readonly instanceId: string;
  readonly nativeSessionId: string;
}): Promise<ClaudeModelObserverBinding | undefined> {
  let raw: string;
  try {
    raw = await NodeFSP.readFile(bindingPath(input), "utf8");
  } catch (error) {
    if ((error as NodeJS.ErrnoException).code === "ENOENT") return undefined;
    throw error;
  }
  let parsed: unknown;
  try {
    parsed = JSON.parse(raw);
  } catch {
    throw new Error("Invalid Claude observer binding JSON");
  }
  const binding = validateBinding(parsed, input.stateDir);
  if (binding.instanceId !== input.instanceId || binding.nativeSessionId !== input.nativeSessionId)
    throw new Error("Claude observer binding identity mismatch");
  return binding;
}

export async function writeClaudeModelObserverBinding(
  input: ClaudeModelObserverBinding & { readonly stateDir: string },
): Promise<void> {
  const binding: ClaudeModelObserverBinding = {
    observerStateDir: input.observerStateDir,
    upstreamUrl: upstreamHref(input.upstreamUrl),
    configDirectory: input.configDirectory,
    instanceId: input.instanceId,
    nativeSessionId: input.nativeSessionId,
  };
  validateBinding(binding, input.stateDir);
  await atomicPrivateJson(bindingPath(input), binding);
}

const settingsPathFor = (observerStateDir: string, upstreamUrl: string) =>
  NodePath.join(observerStateDir, "settings", `${hash(upstreamHref(upstreamUrl))}.json`);
const record = (value: unknown): value is Record<string, unknown> =>
  typeof value === "object" && value !== null && !Array.isArray(value);

/** A durable UUID alone cannot prove an externally recreated worker retained its observer route. */
export async function isClaudeModelObserverBindingCurrent(input: {
  readonly stateDir: string;
  readonly instanceId: string;
  readonly nativeSessionId: string;
  readonly configDirectory: string;
}): Promise<boolean> {
  const binding = await readClaudeModelObserverBinding(input);
  if (!binding || binding.configDirectory !== input.configDirectory) return false;
  const readJson = async (path: string): Promise<unknown> => {
    let raw: string;
    try {
      raw = await NodeFSP.readFile(path, "utf8");
    } catch (error) {
      if ((error as NodeJS.ErrnoException).code === "ENOENT") return undefined;
      throw error;
    }
    try {
      return JSON.parse(raw);
    } catch {
      throw new Error("Invalid Claude observer routing JSON");
    }
  };
  const job = await readJson(
    NodePath.join(
      input.configDirectory,
      "jobs",
      input.nativeSessionId.slice(0, 8).toLowerCase(),
      "state.json",
    ),
  );
  if (job === undefined) return false;
  if (
    !record(job) ||
    typeof job.sessionId !== "string" ||
    !UUID.test(job.sessionId) ||
    !Array.isArray(job.respawnFlags) ||
    job.respawnFlags.some((flag: unknown) => typeof flag !== "string")
  )
    throw new Error("Invalid Claude observer native job routing state");
  if (job.sessionId !== input.nativeSessionId) return false;
  const settingsPath = settingsPathFor(binding.observerStateDir, binding.upstreamUrl);
  const settingsFlags = job.respawnFlags
    .map((flag: string, index: number) =>
      flag === "--settings" || flag.startsWith("--settings=") ? index : -1,
    )
    .filter((index: number) => index >= 0);
  if (
    settingsFlags.length !== 1 ||
    job.respawnFlags[settingsFlags[0]!] !== "--settings" ||
    job.respawnFlags[settingsFlags[0]! + 1] !== settingsPath
  )
    return false;
  let stat;
  try {
    stat = await NodeFSP.lstat(settingsPath);
  } catch (error) {
    if ((error as NodeJS.ErrnoException).code === "ENOENT") return false;
    throw error;
  }
  if (!stat.isFile() || (stat.mode & 0o077) !== 0)
    throw new Error("Claude observer settings must be a private regular file");
  const settings = await readJson(settingsPath);
  if (settings === undefined) return false;
  if (
    !record(settings) ||
    Object.keys(settings).length !== 1 ||
    !record(settings.env) ||
    Object.keys(settings.env).length !== 1 ||
    typeof settings.env.ANTHROPIC_BASE_URL !== "string"
  )
    throw new Error("Invalid Claude observer private settings");
  let route: URL;
  try {
    route = new URL(settings.env.ANTHROPIC_BASE_URL);
  } catch {
    throw new Error("Invalid Claude observer private model route");
  }
  if (
    route.protocol !== "http:" ||
    route.hostname !== "127.0.0.1" ||
    !route.port ||
    route.username ||
    route.password ||
    route.search ||
    route.hash ||
    !/^\/proxy\/[a-f0-9]{64}$/.test(route.pathname)
  )
    throw new Error("Invalid Claude observer private model route");
  return true;
}

/** Private settings keep the model-route bearer secret out of the native worker's argv. */
export async function prepareClaudeModelObserverSettings(input: {
  readonly stateDir: string;
  readonly instanceId: string;
  readonly upstreamUrl: string;
}) {
  const upstreamUrl = upstreamHref(input.upstreamUrl);
  const observerStateDir = deriveClaudeModelObserverStateDir(input);
  const observer = await ensureClaudeModelObserver({ stateDir: observerStateDir, upstreamUrl });
  const settingsPath = settingsPathFor(observerStateDir, upstreamUrl);
  await atomicPrivateJson(settingsPath, { env: { ANTHROPIC_BASE_URL: observer.modelBaseUrl } });
  return { settingsPath, observerStateDir, upstreamUrl };
}
