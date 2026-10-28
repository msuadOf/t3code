// @effect-diagnostics nodeBuiltinImport:off - Isolated private-file routing verification.
import * as NodeFSP from "node:fs/promises";
import * as NodePath from "node:path";
import * as NodeOS from "node:os";
import { afterEach, beforeEach, expect, it, vi } from "vite-plus/test";
import {
  deriveClaudeModelObserverStateDir,
  readClaudeModelObserverBinding,
  isClaudeModelObserverBindingCurrent,
  writeClaudeModelObserverBinding,
  prepareClaudeModelObserverSettings,
} from "./ClaudeModelObserverBindings.ts";
import { ensureClaudeModelObserver } from "./ClaudeModelObserver.ts";
vi.mock("./ClaudeModelObserver.ts", () => ({
  ensureClaudeModelObserver: vi.fn(async () => ({
    modelBaseUrl:
      "http://127.0.0.1:12345/proxy/aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
  })),
}));
let stateDir: string;
const instanceId = "claude-native:test";
const nativeSessionId = "11111111-2222-4333-8444-555555555555";
beforeEach(async () => {
  stateDir = await NodeFSP.mkdtemp(NodePath.join(NodeOS.tmpdir(), "t3-claude-binding-"));
  vi.mocked(ensureClaudeModelObserver).mockClear();
});
afterEach(async () => {
  await NodeFSP.rm(stateDir, { recursive: true, force: true });
});
const descriptor = () => ({
  observerStateDir: deriveClaudeModelObserverStateDir({ stateDir, instanceId }),
  upstreamUrl: "http://127.0.0.1:10000/messages-gateway",
  configDirectory: NodePath.join(stateDir, "native-config"),
  instanceId,
  nativeSessionId,
});
const storedPath = async () => {
  const root = NodePath.join(stateDir, "claude-model-observer-bindings");
  const [instance] = await NodeFSP.readdir(root);
  return NodePath.join(root, instance!, `${nativeSessionId}.json`);
};

it("isolates provider instances and atomically saves a private strict descriptor without route secrets", async () => {
  expect(
    await readClaudeModelObserverBinding({ stateDir, instanceId, nativeSessionId }),
  ).toBeUndefined();
  const binding = descriptor();
  await writeClaudeModelObserverBinding({ stateDir, ...binding });
  expect(await readClaudeModelObserverBinding({ stateDir, instanceId, nativeSessionId })).toEqual(
    binding,
  );
  expect(
    await readClaudeModelObserverBinding({ stateDir, instanceId: "other", nativeSessionId }),
  ).toBeUndefined();
  const path = await storedPath();
  expect((await NodeFSP.stat(path)).mode & 0o777).toBe(0o600);
  expect((await NodeFSP.stat(NodePath.dirname(path))).mode & 0o777).toBe(0o700);
  expect(Object.keys(JSON.parse(await NodeFSP.readFile(path, "utf8")))).toEqual(
    Object.keys(binding),
  );
  expect(await NodeFSP.readdir(NodePath.dirname(path))).toEqual([`${nativeSessionId}.json`]);
  expect(binding.observerStateDir).not.toBe(
    deriveClaudeModelObserverStateDir({ stateDir, instanceId: "other" }),
  );
});

it("rejects damaged, mismatched and credential-bearing bindings instead of hiding them as missing", async () => {
  const input = { stateDir, ...descriptor() };
  await expect(
    writeClaudeModelObserverBinding({ ...input, nativeSessionId: "../../escape" }),
  ).rejects.toThrow();
  await expect(
    writeClaudeModelObserverBinding({ ...input, observerStateDir: "/unrelated" }),
  ).rejects.toThrow();
  await expect(
    writeClaudeModelObserverBinding({ ...input, configDirectory: "relative" }),
  ).rejects.toThrow();
  for (const upstreamUrl of [
    "https://user:secret@example.com",
    "https://example.com/?key=secret",
    "https://example.com/#secret",
    "file:///tmp/model",
  ])
    await expect(writeClaudeModelObserverBinding({ ...input, upstreamUrl })).rejects.toThrow();
  await writeClaudeModelObserverBinding(input);
  const path = await storedPath();
  await NodeFSP.writeFile(path, "broken-json");
  await expect(readClaudeModelObserverBinding(input)).rejects.toThrow("JSON");
  await NodeFSP.writeFile(path, JSON.stringify({ ...descriptor(), instanceId: "other" }));
  await expect(readClaudeModelObserverBinding(input)).rejects.toThrow();
  await NodeFSP.writeFile(path, JSON.stringify({ ...descriptor(), token: "must-not-be-here" }));
  await expect(readClaudeModelObserverBinding(input)).rejects.toThrow();
  await NodeFSP.unlink(path);
  await NodeFSP.mkdir(path);
  await expect(readClaudeModelObserverBinding(input)).rejects.toMatchObject({ code: "EISDIR" });
});

it("prepares reusable 0600 settings with only a per-worker base URL override", async () => {
  const prepared = await prepareClaudeModelObserverSettings({
    stateDir,
    instanceId,
    upstreamUrl: "http://127.0.0.1:10000",
  });
  expect(prepared.upstreamUrl).toBe("http://127.0.0.1:10000/");
  expect(prepared.observerStateDir).toBe(
    deriveClaudeModelObserverStateDir({ stateDir, instanceId }),
  );
  expect(ensureClaudeModelObserver).toHaveBeenCalledWith({
    stateDir: prepared.observerStateDir,
    upstreamUrl: prepared.upstreamUrl,
  });
  expect(JSON.parse(await NodeFSP.readFile(prepared.settingsPath, "utf8"))).toEqual({
    env: {
      ANTHROPIC_BASE_URL:
        "http://127.0.0.1:12345/proxy/aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
    },
  });
  expect((await NodeFSP.stat(prepared.settingsPath)).mode & 0o777).toBe(0o600);
  expect(prepared.settingsPath).not.toContain(
    "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
  );
  expect(
    await prepareClaudeModelObserverSettings({
      stateDir,
      instanceId,
      upstreamUrl: "http://127.0.0.1:10000/",
    }),
  ).toEqual(prepared);
  expect(await NodeFSP.readdir(NodePath.dirname(prepared.settingsPath))).toHaveLength(1);
});

it("rejects stale UUID bindings after an external worker recreation removes or changes native settings flags", async () => {
  const prepared = await prepareClaudeModelObserverSettings({
    stateDir,
    instanceId,
    upstreamUrl: descriptor().upstreamUrl,
  });
  const input = {
    stateDir,
    instanceId,
    nativeSessionId,
    configDirectory: descriptor().configDirectory,
  };
  await writeClaudeModelObserverBinding({ stateDir, ...descriptor() });
  expect(await isClaudeModelObserverBindingCurrent(input)).toBe(false);
  const jobPath = NodePath.join(
    input.configDirectory,
    "jobs",
    nativeSessionId.slice(0, 8),
    "state.json",
  );
  await NodeFSP.mkdir(NodePath.dirname(jobPath), { recursive: true });
  const writeJob = (respawnFlags: unknown, sessionId = nativeSessionId) =>
    NodeFSP.writeFile(jobPath, JSON.stringify({ sessionId, respawnFlags }));
  await writeJob(["--model", "claude-opus"]);
  expect(await isClaudeModelObserverBindingCurrent(input)).toBe(false);
  await writeJob(["--settings", "/unrelated-settings"]);
  expect(await isClaudeModelObserverBindingCurrent(input)).toBe(false);
  await writeJob(["--settings", prepared.settingsPath, "--settings", "/external-override"]);
  expect(await isClaudeModelObserverBindingCurrent(input)).toBe(false);
  await writeJob(["--settings", prepared.settingsPath], "aaaaaaaa-2222-4333-8444-555555555555");
  expect(await isClaudeModelObserverBindingCurrent(input)).toBe(false);
  await writeJob(["--settings", prepared.settingsPath, "--model", "claude-opus"]);
  expect(await isClaudeModelObserverBindingCurrent(input)).toBe(true);
  expect(
    await isClaudeModelObserverBindingCurrent({ ...input, configDirectory: "/different" }),
  ).toBe(false);
  await NodeFSP.unlink(prepared.settingsPath);
  expect(await isClaudeModelObserverBindingCurrent(input)).toBe(false);
});

it("reports damaged native routing state and nonprivate or malformed settings explicitly", async () => {
  const prepared = await prepareClaudeModelObserverSettings({
    stateDir,
    instanceId,
    upstreamUrl: descriptor().upstreamUrl,
  });
  const input = {
    stateDir,
    instanceId,
    nativeSessionId,
    configDirectory: descriptor().configDirectory,
  };
  await writeClaudeModelObserverBinding({ stateDir, ...descriptor() });
  const jobPath = NodePath.join(
    input.configDirectory,
    "jobs",
    nativeSessionId.slice(0, 8),
    "state.json",
  );
  await NodeFSP.mkdir(NodePath.dirname(jobPath), { recursive: true });
  await NodeFSP.writeFile(jobPath, "bad-json");
  await expect(isClaudeModelObserverBindingCurrent(input)).rejects.toThrow("routing JSON");
  await NodeFSP.writeFile(
    jobPath,
    JSON.stringify({ sessionId: nativeSessionId, respawnFlags: [null] }),
  );
  await expect(isClaudeModelObserverBindingCurrent(input)).rejects.toThrow("job routing");
  await NodeFSP.writeFile(
    jobPath,
    JSON.stringify({
      sessionId: nativeSessionId,
      respawnFlags: ["--settings", prepared.settingsPath],
    }),
  );
  await NodeFSP.chmod(prepared.settingsPath, 0o644);
  await expect(isClaudeModelObserverBindingCurrent(input)).rejects.toThrow("private regular file");
  await NodeFSP.chmod(prepared.settingsPath, 0o600);
  await NodeFSP.writeFile(
    prepared.settingsPath,
    JSON.stringify({ env: { ANTHROPIC_BASE_URL: "https://external.invalid/" } }),
  );
  await expect(isClaudeModelObserverBindingCurrent(input)).rejects.toThrow("private model route");
});
