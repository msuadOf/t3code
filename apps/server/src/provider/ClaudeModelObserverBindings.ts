// @effect-diagnostics nodeBuiltinImport:off - Private native-worker routing files are process infrastructure.
import * as NodeFSP from "node:fs/promises";
import * as NodePath from "node:path";
import * as NodeCrypto from "node:crypto";
import { claudeModelObserverEnvironment } from "./ClaudeModelObserverSettings.ts";
import { ensureClaudeModelObserver } from "./ClaudeModelObserver.ts";

export interface ClaudeModelObserverBinding {
  readonly observerStateDir: string;
  readonly settingsPath: string;
  readonly enabled: boolean;
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
    "settingsPath",
    "upstreamUrl",
    "configDirectory",
    "instanceId",
    "nativeSessionId",
  ];
  if (
    Object.keys(item).length !== fields.length + 1 ||
    typeof item.enabled !== "boolean" ||
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
  if (
    NodePath.dirname(binding.settingsPath) !==
      NodePath.join(binding.observerStateDir, "settings") ||
    !UUID.test(NodePath.basename(binding.settingsPath, ".json")) ||
    !binding.settingsPath.endsWith(".json")
  )
    throw new Error("Invalid Claude observer private settings path");
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
  // Routing is the atomic source of truth. The descriptor flag can lag after a crash
  // between the private settings rename and the descriptor rename.
  const actualRoute = await privateSettingsRoute(binding.settingsPath);
  if (actualRoute === binding.upstreamUrl) return { ...binding, enabled: false };
  if (actualRoute !== undefined && proxyRoute(actualRoute)) return { ...binding, enabled: true };
  return binding;
}

export async function writeClaudeModelObserverBinding(
  input: Omit<ClaudeModelObserverBinding, "enabled"> & {
    readonly enabled?: boolean;
    readonly stateDir: string;
  },
): Promise<void> {
  const binding: ClaudeModelObserverBinding = {
    observerStateDir: input.observerStateDir,
    settingsPath: input.settingsPath,
    enabled: input.enabled ?? true,
    upstreamUrl: upstreamHref(input.upstreamUrl),
    configDirectory: input.configDirectory,
    instanceId: input.instanceId,
    nativeSessionId: input.nativeSessionId,
  };
  validateBinding(binding, input.stateDir);
  await atomicPrivateJson(bindingPath(input), binding);
}

const record = (value: unknown): value is Record<string, unknown> =>
  typeof value === "object" && value !== null && !Array.isArray(value);
const readRoutingJson = async (path: string): Promise<unknown> => {
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
interface BindingIdentity {
  readonly stateDir: string;
  readonly instanceId: string;
  readonly nativeSessionId: string;
  readonly configDirectory: string;
}
const boundJob = async (input: BindingIdentity, binding: ClaudeModelObserverBinding) => {
  if (binding.configDirectory !== input.configDirectory) return undefined;
  const job = await readRoutingJson(
    NodePath.join(
      input.configDirectory,
      "jobs",
      input.nativeSessionId.slice(0, 8).toLowerCase(),
      "state.json",
    ),
  );
  if (job === undefined) return undefined;
  if (
    !record(job) ||
    typeof job.sessionId !== "string" ||
    !UUID.test(job.sessionId) ||
    !Array.isArray(job.respawnFlags) ||
    job.respawnFlags.some((flag: unknown) => typeof flag !== "string")
  )
    throw new Error("Invalid Claude observer native job routing state");
  if (job.sessionId !== input.nativeSessionId) return undefined;
  const settingsFlags = job.respawnFlags
    .map((flag: string, index: number) =>
      flag === "--settings" || flag.startsWith("--settings=") ? index : -1,
    )
    .filter((index: number) => index >= 0);
  if (
    settingsFlags.length !== 1 ||
    job.respawnFlags[settingsFlags[0]!] !== "--settings" ||
    job.respawnFlags[settingsFlags[0]! + 1] !== binding.settingsPath
  )
    return undefined;
  return job;
};
const privateSettingsRoute = async (settingsPath: string): Promise<string | undefined> => {
  let stat;
  try {
    stat = await NodeFSP.lstat(settingsPath);
  } catch (error) {
    if ((error as NodeJS.ErrnoException).code === "ENOENT") return undefined;
    throw error;
  }
  if (!stat.isFile() || (stat.mode & 0o077) !== 0)
    throw new Error("Claude observer settings must be a private regular file");
  const settings = await readRoutingJson(settingsPath);
  if (settings === undefined) return undefined;
  if (
    !record(settings) ||
    Object.keys(settings).length !== 1 ||
    !record(settings.env) ||
    typeof settings.env.ANTHROPIC_BASE_URL !== "string"
  )
    throw new Error("Invalid Claude observer private settings");
  const route = settings.env.ANTHROPIC_BASE_URL;
  const env = settings.env;
  if (Object.keys(env).length !== 1) {
    const expected = claudeModelObserverEnvironment(route);
    if (
      Object.keys(env).length !== Object.keys(expected).length ||
      Object.entries(expected).some(([key, value]) => env[key] !== value)
    )
      throw new Error("Invalid Claude observer private telemetry settings");
  }
  return route;
};
const proxyRoute = (value: string): boolean => {
  let route: URL;
  try {
    route = new URL(value);
  } catch {
    return false;
  }
  return (
    route.protocol === "http:" &&
    route.hostname === "127.0.0.1" &&
    !!route.port &&
    !route.username &&
    !route.password &&
    !route.search &&
    !route.hash &&
    /^\/proxy\/[a-f0-9]{64}$/.test(route.pathname)
  );
};

const routedBinding = async (
  input: BindingIdentity,
): Promise<ClaudeModelObserverBinding | undefined> => {
  const binding = await readClaudeModelObserverBinding(input);
  if (!binding || !(await boundJob(input, binding))) return undefined;
  const route = await privateSettingsRoute(binding.settingsPath);
  if (route === undefined) return undefined;
  if (!URL.canParse(route)) throw new Error("Invalid Claude observer private model route");
  if (route === binding.upstreamUrl) return { ...binding, enabled: false };
  if (proxyRoute(route)) return { ...binding, enabled: true };
  return undefined;
};

/** Includes disabled bindings, but only while the native job retains this owned settings route. */
export async function hasClaudeModelObserverBindingRouting(
  input: BindingIdentity,
): Promise<boolean> {
  return (await routedBinding(input)) !== undefined;
}

/** A durable UUID alone cannot prove an externally recreated worker retained its observer route. */
export async function isClaudeModelObserverBindingCurrent(
  input: BindingIdentity,
): Promise<boolean> {
  return (await routedBinding(input))?.enabled === true;
}

/** Caller must first verify native inventory has no live owner; this helper never stops a worker. */
export async function setClaudeModelObserverEnabled(
  input: BindingIdentity & { readonly enabled: boolean },
): Promise<void> {
  const binding = await readClaudeModelObserverBinding(input);
  if (!binding) throw new Error("Claude observer binding was not found");
  const job = await boundJob(input, binding);
  if (!job) throw new Error("Claude observer binding no longer matches the native worker");
  if (job.state !== "stopped" && job.state !== "failed")
    throw new Error("Stop the native Claude worker before changing model observation");
  const current = await privateSettingsRoute(binding.settingsPath);
  if (current !== undefined && current !== binding.upstreamUrl && !proxyRoute(current))
    throw new Error("Invalid Claude observer private model route");
  const route = input.enabled
    ? (
        await ensureClaudeModelObserver({
          stateDir: binding.observerStateDir,
          upstreamUrl: binding.upstreamUrl,
        })
      ).modelBaseUrl
    : binding.upstreamUrl;
  await atomicPrivateJson(binding.settingsPath, {
    env: input.enabled ? claudeModelObserverEnvironment(route) : { ANTHROPIC_BASE_URL: route },
  });
  await writeClaudeModelObserverBinding({
    ...binding,
    stateDir: input.stateDir,
    enabled: input.enabled,
  });
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
  const settingsPath = NodePath.join(
    observerStateDir,
    "settings",
    `${NodeCrypto.randomUUID()}.json`,
  );
  await atomicPrivateJson(settingsPath, {
    env: claudeModelObserverEnvironment(observer.modelBaseUrl),
  });
  return { settingsPath, observerStateDir, upstreamUrl };
}
