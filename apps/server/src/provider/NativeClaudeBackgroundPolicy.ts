// @effect-diagnostics nodeBuiltinImport:off -- private native worker launch settings.
import * as NodeFSP from "node:fs/promises";
import * as NodePath from "node:path";
import * as NodeCrypto from "node:crypto";

const policy = { BASH_DEFAULT_TIMEOUT_MS: "5000", CLAUDE_CODE_DISABLE_BACKGROUND_TASKS: "0" };
const record = (value: unknown): value is Record<string, unknown> =>
  typeof value === "object" && value !== null && !Array.isArray(value);
export function claudeBackgroundPolicyEnabled(env: Record<string, unknown>): boolean {
  const keys = Object.keys(policy);
  if (!keys.some((key) => key in env)) return false;
  if (Object.entries(policy).some(([key, value]) => env[key] !== value))
    throw new Error("Invalid Claude background command policy");
  return true;
}
export function withoutClaudeBackgroundPolicy(
  env: Record<string, unknown>,
): Record<string, unknown> {
  claudeBackgroundPolicyEnabled(env);
  return Object.fromEntries(Object.entries(env).filter(([key]) => !Object.hasOwn(policy, key)));
}
async function privatePath(path: string, directory: boolean) {
  const stat = await NodeFSP.lstat(path);
  if (
    stat.isSymbolicLink() ||
    (directory ? !stat.isDirectory() : !stat.isFile()) ||
    (stat.mode & 0o777) !== (directory ? 0o700 : 0o600) ||
    (process.getuid && stat.uid !== process.getuid())
  )
    throw new Error("Claude background settings must be private and owned");
  const canonical = await NodeFSP.realpath(path);
  if (
    !directory &&
    NodePath.dirname(canonical) !== (await NodeFSP.realpath(NodePath.dirname(path)))
  )
    throw new Error("Claude background settings escaped their directory");
}
export async function readClaudeBackgroundPolicy(settingsPath: string): Promise<boolean> {
  await privatePath(settingsPath, false);
  const value: unknown = JSON.parse(await NodeFSP.readFile(settingsPath, "utf8"));
  if (!record(value) || (value.env !== undefined && !record(value.env)))
    throw new Error("Invalid Claude worker settings");
  return claudeBackgroundPolicyEnabled(value.env ?? {});
}
export async function ownedClaudeBackgroundSettings(
  config: string,
  flags: readonly string[],
): Promise<boolean> {
  const index = flags.indexOf("--settings");
  const path = flags[index + 1];
  if (
    index < 0 ||
    !path ||
    flags.filter((flag) => flag === "--settings" || flag.startsWith("--settings=")).length !== 1
  )
    return false;
  const directory = NodePath.join(config, "t3-native-background-settings");
  if (
    NodePath.dirname(path) !== directory ||
    !/^[a-f0-9-]{36}\.json$/.test(NodePath.basename(path))
  )
    return false;
  await privatePath(directory, true);
  await privatePath(path, false);
  const value: unknown = JSON.parse(await NodeFSP.readFile(path, "utf8"));
  return (
    record(value) &&
    Object.keys(value).length === 1 &&
    record(value.env) &&
    Object.keys(value.env).length === 2 &&
    claudeBackgroundPolicyEnabled(value.env)
  );
}
export async function prepareClaudeBackgroundSettings(
  config: string,
  settingsPath?: string,
): Promise<string> {
  if (settingsPath) {
    await privatePath(settingsPath, false);
    const value: unknown = JSON.parse(await NodeFSP.readFile(settingsPath, "utf8"));
    if (!record(value) || (value.env !== undefined && !record(value.env)))
      throw new Error("Invalid Claude worker settings");
    await NodeFSP.writeFile(
      settingsPath,
      JSON.stringify({ ...value, env: { ...value.env, ...policy } }),
      { mode: 0o600 },
    );
    return settingsPath;
  }
  const directory = NodePath.join(config, "t3-native-background-settings");
  await NodeFSP.mkdir(directory, { recursive: true, mode: 0o700 });
  await privatePath(directory, true);
  const path = NodePath.join(directory, `${NodeCrypto.randomUUID()}.json`);
  await NodeFSP.writeFile(path, JSON.stringify({ env: policy }), { mode: 0o600, flag: "wx" });
  return path;
}
