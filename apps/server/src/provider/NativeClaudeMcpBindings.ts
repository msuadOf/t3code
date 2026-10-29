// @effect-diagnostics nodeBuiltinImport:off -- private native worker launch metadata survives the T3 process.
import * as NodeCrypto from "node:crypto";
import * as NodeFSP from "node:fs/promises";
import * as NodePath from "node:path";
import * as NodeNet from "node:net";
import * as NodeOS from "node:os";
import type { McpProviderSessionConfig } from "@t3tools/provider-core/server/mcpSession";

const mutations = new Map<string, Promise<void>>();
async function withMutation<A>(key: string, operation: () => Promise<A>): Promise<A> {
  const previous = mutations.get(key) ?? Promise.resolve();
  let release!: () => void;
  const pending = new Promise<void>((resolve) => {
    release = resolve;
  });
  mutations.set(key, pending);
  await previous;
  try {
    return await operation();
  } finally {
    release();
    if (mutations.get(key) === pending) mutations.delete(key);
  }
}

const UUID = /^[a-f0-9]{8}-(?:[a-f0-9]{4}-){3}[a-f0-9]{12}$/;
export const CLAUDE_MCP_SESSION_METADATA_KEY = "t3/claudeSessionID";

export interface ClaudeMcpCredential {
  readonly threadId: string;
  readonly providerInstanceId: string;
  readonly providerSessionId: string;
  readonly endpoint: string;
  readonly authorizationHeader: string;
}
export interface PreparedClaudeMcp {
  readonly launchDirectory: string;
  readonly mcpConfigPath: string;
}
interface Launch {
  readonly version: 1;
  readonly directory: string;
  readonly credential: ClaudeMcpCredential;
}
interface Binding extends Launch {
  readonly nativeSessionId: string;
  readonly launchDirectory: string;
  readonly revoked: boolean;
}
const missing = (error: unknown) =>
  typeof error === "object" && error !== null && "code" in error && error.code === "ENOENT";

export function assertClaudeMcpEndpoint(
  endpoint: string,
  localAddresses?: readonly string[],
): void {
  const url = new URL(endpoint);
  const host = url.hostname.startsWith("[") ? url.hostname.slice(1, -1) : url.hostname;
  const local =
    host === "localhost" ||
    host === "::1" ||
    (NodeNet.isIP(host) === 4 && host.startsWith("127.")) ||
    (NodeNet.isIP(host) !== 0 &&
      (
        localAddresses ??
        Object.values(NodeOS.networkInterfaces()).flatMap((addresses) =>
          (addresses ?? []).map((address) => address.address),
        )
      ).includes(host));
  if (
    !["http:", "https:"].includes(url.protocol) ||
    !local ||
    url.username ||
    url.password ||
    url.hash
  )
    throw new Error("Native Claude MCP requires a loopback or host-local HTTP endpoint.");
}
async function privatePath(path: string, directory: boolean): Promise<void> {
  const stat = await NodeFSP.lstat(path);
  if (
    stat.isSymbolicLink() ||
    (directory ? !stat.isDirectory() : !stat.isFile()) ||
    (stat.mode & 0o777) !== (directory ? 0o700 : 0o600) ||
    (process.getuid !== undefined && stat.uid !== process.getuid())
  )
    throw new Error("Native Claude MCP metadata must be private and owned by this user.");
}
async function rejectSymlinkAncestors(directory: string): Promise<void> {
  let current = NodePath.resolve(directory);
  for (;;) {
    try {
      if ((await NodeFSP.lstat(current)).isSymbolicLink())
        throw new Error("Native Claude MCP metadata cannot use symlinked directories.");
    } catch (error) {
      if (!missing(error)) throw error;
    }
    const parent = NodePath.dirname(current);
    if (parent === current) return;
    current = parent;
  }
}
async function privateDirectory(directory: string): Promise<void> {
  await privatePath(directory, true);
  if ((await NodeFSP.realpath(directory)) !== NodePath.resolve(directory))
    throw new Error("Native Claude MCP metadata cannot use symlinked directories.");
}
async function readPrivate(path: string): Promise<unknown> {
  await privatePath(path, false);
  return JSON.parse(await NodeFSP.readFile(path, "utf8"));
}
const record = (value: unknown): value is Record<string, unknown> =>
  value !== null && typeof value === "object" && !Array.isArray(value);
function credential(value: unknown): ClaudeMcpCredential {
  if (
    !record(value) ||
    ![
      "threadId",
      "providerInstanceId",
      "providerSessionId",
      "endpoint",
      "authorizationHeader",
    ].every((key) => typeof value[key] === "string" && value[key].length > 0)
  )
    throw new Error("Invalid native Claude MCP credential metadata.");
  assertClaudeMcpEndpoint(String(value.endpoint));
  return {
    threadId: String(value.threadId),
    providerInstanceId: String(value.providerInstanceId),
    providerSessionId: String(value.providerSessionId),
    endpoint: String(value.endpoint),
    authorizationHeader: String(value.authorizationHeader),
  };
}
function decodeLaunch(value: unknown): Launch {
  if (!record(value) || value.version !== 1 || typeof value.directory !== "string")
    throw new Error("Invalid native Claude MCP launch metadata.");
  return { version: 1, directory: value.directory, credential: credential(value.credential) };
}
function decodeBinding(value: unknown): Binding {
  const launch = decodeLaunch(value);
  if (
    !record(value) ||
    typeof value.nativeSessionId !== "string" ||
    !UUID.test(value.nativeSessionId) ||
    typeof value.launchDirectory !== "string" ||
    typeof value.revoked !== "boolean"
  )
    throw new Error("Invalid native Claude MCP binding metadata.");
  return {
    ...launch,
    nativeSessionId: value.nativeSessionId,
    launchDirectory: value.launchDirectory,
    revoked: value.revoked,
  };
}
async function atomicWrite(path: string, value: unknown): Promise<void> {
  const temporary = `${path}.${NodeCrypto.randomUUID()}.tmp`;
  try {
    await NodeFSP.writeFile(temporary, `${JSON.stringify(value)}\n`, { mode: 0o600, flag: "wx" });
    await NodeFSP.rename(temporary, path);
  } finally {
    await NodeFSP.rm(temporary, { force: true });
  }
}
const sameOwner = (left: ClaudeMcpCredential, right: ClaudeMcpCredential) =>
  left.threadId === right.threadId && left.providerInstanceId === right.providerInstanceId;

/** Paths passed to the native worker contain no thread bearer; the relay reads private files. */
export class NativeClaudeMcpBindings {
  readonly directory: string;
  constructor(directory: string) {
    this.directory = directory;
  }
  private sessionPath(nativeSessionId: string): string {
    if (!UUID.test(nativeSessionId)) throw new Error("Invalid native Claude MCP session identity.");
    return NodePath.join(this.directory, `${nativeSessionId}.json`);
  }
  async prepare(input: {
    readonly config: McpProviderSessionConfig;
    readonly launcher: (launchDirectory: string) => {
      readonly command: string;
      readonly args: readonly string[];
      readonly env?: Readonly<Record<string, string>>;
    };
  }): Promise<PreparedClaudeMcp> {
    const config = credential(input.config);
    await rejectSymlinkAncestors(this.directory);
    await NodeFSP.mkdir(this.directory, { mode: 0o700, recursive: true });
    await privateDirectory(this.directory);
    const launchDirectory = await NodeFSP.mkdtemp(NodePath.join(this.directory, "launch-"));
    await NodeFSP.chmod(launchDirectory, 0o700);
    const mcpConfigPath = NodePath.join(launchDirectory, "mcp.json");
    await atomicWrite(NodePath.join(launchDirectory, "launch.json"), {
      version: 1,
      directory: this.directory,
      credential: config,
    });
    const launcher = input.launcher(launchDirectory);
    await atomicWrite(mcpConfigPath, { mcpServers: { "t3-code": { type: "stdio", ...launcher } } });
    return { launchDirectory, mcpConfigPath };
  }
  async bind(input: {
    readonly prepared: PreparedClaudeMcp;
    readonly nativeSessionId: string;
    readonly config: McpProviderSessionConfig;
  }): Promise<void> {
    return withMutation(this.sessionPath(input.nativeSessionId), () => this.bindOwned(input));
  }
  private async bindOwned(input: {
    readonly prepared: PreparedClaudeMcp;
    readonly nativeSessionId: string;
    readonly config: McpProviderSessionConfig;
  }): Promise<void> {
    await privateDirectory(this.directory);
    await privateDirectory(input.prepared.launchDirectory);
    const launch = decodeLaunch(
      await readPrivate(NodePath.join(input.prepared.launchDirectory, "launch.json")),
    );
    const config = credential(input.config);
    if (launch.directory !== this.directory || !sameOwner(launch.credential, config))
      throw new Error("Native Claude MCP launch belongs to another thread.");
    const path = this.sessionPath(input.nativeSessionId);
    const binding: Binding = {
      version: 1,
      directory: this.directory,
      credential: config,
      nativeSessionId: input.nativeSessionId,
      launchDirectory: input.prepared.launchDirectory,
      revoked: false,
    };
    try {
      await NodeFSP.writeFile(path, `${JSON.stringify(binding)}\n`, { flag: "wx", mode: 0o600 });
    } catch (error) {
      if (!(record(error) && error.code === "EEXIST")) throw error;
      const existing = decodeBinding(await readPrivate(path));
      if (
        existing.launchDirectory !== binding.launchDirectory ||
        !sameOwner(existing.credential, config) ||
        existing.revoked
      )
        throw new Error("Native Claude MCP session already has another owner or was revoked.", {
          cause: error,
        });
      await atomicWrite(path, binding);
    }
  }
  async ownership(nativeSessionId: string): Promise<
    | {
        readonly threadId: string;
        readonly providerInstanceId: string;
        readonly prepared: PreparedClaudeMcp;
        readonly revoked: boolean;
      }
    | undefined
  > {
    try {
      await privateDirectory(this.directory);
      const value = decodeBinding(await readPrivate(this.sessionPath(nativeSessionId)));
      if (value.directory !== this.directory || value.nativeSessionId !== nativeSessionId)
        throw new Error("Native Claude MCP binding identity does not match.");
      await privateDirectory(value.launchDirectory);
      return {
        threadId: value.credential.threadId,
        providerInstanceId: value.credential.providerInstanceId,
        prepared: {
          launchDirectory: value.launchDirectory,
          mcpConfigPath: NodePath.join(value.launchDirectory, "mcp.json"),
        },
        revoked: value.revoked,
      };
    } catch (error) {
      if (missing(error)) return undefined;
      throw error;
    }
  }
  async hasBinding(nativeSessionId: string): Promise<boolean> {
    const owner = await this.ownership(nativeSessionId);
    return owner !== undefined && !owner.revoked;
  }
  async recover(input: {
    readonly nativeSessionId: string;
    readonly config: McpProviderSessionConfig;
  }): Promise<void> {
    const owner = await this.ownership(input.nativeSessionId);
    const config = credential(input.config);
    if (
      !owner ||
      owner.revoked ||
      owner.threadId !== config.threadId ||
      owner.providerInstanceId !== config.providerInstanceId
    )
      throw new Error("Native Claude MCP binding cannot be recovered for this owner.");
    await this.bind({
      prepared: owner.prepared,
      nativeSessionId: input.nativeSessionId,
      config: input.config,
    });
  }
  async revoke(input: {
    readonly nativeSessionId: string;
    readonly expectedProviderSessionId?: string;
  }): Promise<void> {
    return withMutation(this.sessionPath(input.nativeSessionId), () => this.revokeOwned(input));
  }
  private async revokeOwned(input: {
    readonly nativeSessionId: string;
    readonly expectedProviderSessionId?: string;
  }): Promise<void> {
    await privateDirectory(this.directory);
    const path = this.sessionPath(input.nativeSessionId);
    let value: Binding;
    try {
      value = decodeBinding(await readPrivate(path));
    } catch (error) {
      if (missing(error)) return;
      throw error;
    }
    if (
      input.expectedProviderSessionId !== undefined &&
      value.credential.providerSessionId !== input.expectedProviderSessionId
    )
      return;
    await atomicWrite(path, { ...value, revoked: true });
  }
}

/** Re-read for every HTTP request, including requests on an established MCP session. */
export async function readClaudeMcpRelayCredential(input: {
  readonly launchDirectory: string;
  readonly nativeSessionId: string;
  readonly discovery: boolean;
}): Promise<ClaudeMcpCredential> {
  if (!UUID.test(input.nativeSessionId))
    throw new Error("Native Claude MCP requires its worker session identity.");
  await privateDirectory(input.launchDirectory);
  const launch = decodeLaunch(
    await readPrivate(NodePath.join(input.launchDirectory, "launch.json")),
  );
  await privateDirectory(launch.directory);
  if (NodePath.dirname(input.launchDirectory) !== launch.directory)
    throw new Error("Native Claude MCP launch provenance does not match.");
  let binding: Binding;
  try {
    binding = decodeBinding(
      await readPrivate(NodePath.join(launch.directory, `${input.nativeSessionId}.json`)),
    );
  } catch (error) {
    if (missing(error) && input.discovery) return launch.credential;
    throw new Error("Native Claude MCP session is not bound.", { cause: error });
  }
  if (
    binding.revoked ||
    binding.nativeSessionId !== input.nativeSessionId ||
    binding.directory !== launch.directory ||
    binding.launchDirectory !== input.launchDirectory ||
    !sameOwner(binding.credential, launch.credential)
  )
    throw new Error("Native Claude MCP session binding is unavailable.");
  return binding.credential;
}
