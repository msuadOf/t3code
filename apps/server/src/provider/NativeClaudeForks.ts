// @effect-diagnostics nodeBuiltinImport:off -- durable T3 ownership metadata is separate from native transcripts.
import * as NodeCrypto from "node:crypto";
import * as NodeFSP from "node:fs/promises";
import * as NodePath from "node:path";

const uuid = /^[a-f0-9]{8}-(?:[a-f0-9]{4}-){3}[a-f0-9]{12}$/;
const registry = (config: string) => NodePath.join(config, "t3-native-forks");
const markerPath = (config: string, settings: string) =>
  NodePath.join(
    registry(config),
    `${NodeCrypto.createHash("sha256").update(settings).digest("hex")}.json`,
  );
const privatePath = async (path: string, directory: boolean) => {
  const stat = await NodeFSP.lstat(path);
  if (
    stat.isSymbolicLink() ||
    (directory ? !stat.isDirectory() : !stat.isFile()) ||
    (stat.mode & 0o777) !== (directory ? 0o700 : 0o600) ||
    (process.getuid !== undefined && stat.uid !== process.getuid())
  )
    throw new Error("Claude fork ownership metadata must be private and owned by this user.");
};
export interface NativeClaudeForkOwner {
  readonly sourceSessionId: string;
  readonly targetThreadId: string;
}

const decodeOwner = (value: unknown): NativeClaudeForkOwner | undefined => {
  if (value === null || typeof value !== "object") return undefined;
  const marker = value as Record<string, unknown>;
  return marker.version === 1 &&
    typeof marker.sourceSessionId === "string" &&
    uuid.test(marker.sourceSessionId) &&
    typeof marker.targetThreadId === "string" &&
    marker.targetThreadId.length > 0
    ? { sourceSessionId: marker.sourceSessionId, targetThreadId: marker.targetThreadId }
    : undefined;
};

/** Preserve ownership even if a later daemon loses the job's saved launch flags. */
export async function associateNativeClaudeFork(
  config: string,
  settings: string,
  sessionId: string,
): Promise<void> {
  if (!uuid.test(sessionId)) throw new Error("Invalid native Claude fork identity.");
  await privatePath(registry(config), true);
  await privatePath(markerPath(config, settings), false);
  await NodeFSP.link(
    markerPath(config, settings),
    NodePath.join(registry(config), `${sessionId}.json`),
  );
}

/** Written before launch and retained across crashes, until the intended T3 owner exists. */
export async function prepareNativeClaudeFork(
  config: string,
  owner: NativeClaudeForkOwner,
  observerSettingsPath?: string,
): Promise<string> {
  if (!uuid.test(owner.sourceSessionId) || !owner.targetThreadId)
    throw new Error("Invalid native Claude fork owner.");
  await NodeFSP.mkdir(registry(config), { recursive: true, mode: 0o700 });
  await privatePath(registry(config), true);
  let settings = observerSettingsPath;
  if (settings === undefined) {
    const directory = await NodeFSP.mkdtemp(NodePath.join(registry(config), "settings-"));
    await NodeFSP.chmod(directory, 0o700);
    settings = NodePath.join(directory, "settings.json");
    await NodeFSP.writeFile(settings, "{}\n", { mode: 0o600, flag: "wx" });
  }
  await privatePath(settings, false);
  await NodeFSP.writeFile(
    markerPath(config, settings),
    JSON.stringify({ version: 1, ...owner, settingsPath: settings }),
    { mode: 0o600, flag: "wx" },
  );
  return settings;
}

/** Only a native job's flags plus a private T3 registry entry establish fork provenance. */
export async function nativeClaudeForkOwner(
  config: string,
  flags: unknown,
  sessionId?: string,
): Promise<NativeClaudeForkOwner | undefined> {
  if (sessionId !== undefined && uuid.test(sessionId)) {
    const file = NodePath.join(registry(config), `${sessionId}.json`);
    try {
      await privatePath(registry(config), true);
      await privatePath(file, false);
      return decodeOwner(JSON.parse(await NodeFSP.readFile(file, "utf8")));
    } catch (error) {
      if (
        !(typeof error === "object" && error !== null && "code" in error && error.code === "ENOENT")
      )
        throw error;
    }
  }
  if (!Array.isArray(flags)) return undefined;
  for (let index = 0; index < flags.length - 1; index++) {
    if (flags[index] !== "--settings" || typeof flags[index + 1] !== "string") continue;
    const settings = flags[index + 1] as string;
    if (!NodePath.isAbsolute(settings)) continue;
    try {
      await privatePath(registry(config), true);
      await privatePath(markerPath(config, settings), false);
      await privatePath(settings, false);
      const marker: unknown = JSON.parse(
        await NodeFSP.readFile(markerPath(config, settings), "utf8"),
      );
      if (marker === null || typeof marker !== "object") continue;
      const value = marker as Record<string, unknown>;
      if (
        value.version === 1 &&
        value.settingsPath === settings &&
        typeof value.sourceSessionId === "string" &&
        uuid.test(value.sourceSessionId) &&
        typeof value.targetThreadId === "string" &&
        value.targetThreadId.length > 0
      )
        return { sourceSessionId: value.sourceSessionId, targetThreadId: value.targetThreadId };
    } catch (error) {
      if (typeof error === "object" && error !== null && "code" in error && error.code === "ENOENT")
        continue;
      throw error;
    }
  }
  return undefined;
}
