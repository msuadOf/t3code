// @effect-diagnostics nodeBuiltinImport:off - Isolated private-file routing verification.
import * as NodeFSP from "node:fs/promises";
import * as NodePath from "node:path";
import * as NodeOS from "node:os";
import { afterEach, beforeEach, expect, it, vi } from "vite-plus/test";
import {
  deriveClaudeModelObserverStateDir,
  readClaudeModelObserverBinding,
  isClaudeModelObserverBindingCurrent,
  hasClaudeModelObserverBindingRouting,
  setClaudeModelObserverEnabled,
  writeClaudeModelObserverBinding,
  prepareClaudeModelObserverSettings,
} from "./ClaudeModelObserverBindings.ts";
import { claudeModelObserverEnvironment } from "./ClaudeModelObserverSettings.ts";
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
  settingsPath: NodePath.join(
    deriveClaudeModelObserverStateDir({ stateDir, instanceId }),
    "settings",
    "aaaaaaaa-bbbb-4ccc-8ddd-eeeeeeeeeeee.json",
  ),
  enabled: true,
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
  expect(Object.keys(JSON.parse(await NodeFSP.readFile(path, "utf8"))).sort()).toEqual(
    Object.keys(binding).sort(),
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

it("prepares independent private settings with local native tracing and no content logging", async () => {
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
    env: claudeModelObserverEnvironment(
      "http://127.0.0.1:12345/proxy/aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
    ),
  });
  const env = JSON.parse(await NodeFSP.readFile(prepared.settingsPath, "utf8")).env;
  expect(env).toMatchObject({
    ENABLE_BETA_TRACING_DETAILED: "0",
    OTEL_EXPORTER_OTLP_TRACES_COMPRESSION: "none",
    OTEL_LOGS_EXPORTER: "none",
    OTEL_METRICS_EXPORTER: "none",
    OTEL_LOG_RAW_API_BODIES: "0",
    OTEL_LOG_TOOL_CONTENT: "0",
  });
  expect(new URL(env.OTEL_EXPORTER_OTLP_TRACES_ENDPOINT).origin).toBe(
    new URL(env.ANTHROPIC_BASE_URL).origin,
  );
  expect((await NodeFSP.stat(prepared.settingsPath)).mode & 0o777).toBe(0o600);
  expect(prepared.settingsPath).not.toContain(
    "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
  );
  const second = await prepareClaudeModelObserverSettings({
    stateDir,
    instanceId,
    upstreamUrl: "http://127.0.0.1:10000/",
  });
  expect(second.settingsPath).not.toBe(prepared.settingsPath);
  expect(second.observerStateDir).toBe(prepared.observerStateDir);
  expect(await NodeFSP.readdir(NodePath.dirname(prepared.settingsPath))).toHaveLength(2);
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
  await writeClaudeModelObserverBinding({
    stateDir,
    ...descriptor(),
    settingsPath: prepared.settingsPath,
  });
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
  expect(await hasClaudeModelObserverBindingRouting(input)).toBe(false);
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
  await writeClaudeModelObserverBinding({
    stateDir,
    ...descriptor(),
    settingsPath: prepared.settingsPath,
  });
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
  expect(await isClaudeModelObserverBindingCurrent(input)).toBe(false);
});

it("disables and reenables only the stopped worker's private route while retaining its native settings path", async () => {
  const prepared = await prepareClaudeModelObserverSettings({
    stateDir,
    instanceId,
    upstreamUrl: descriptor().upstreamUrl,
  });
  const second = await prepareClaudeModelObserverSettings({
    stateDir,
    instanceId,
    upstreamUrl: descriptor().upstreamUrl,
  });
  const secondSettingsBefore = await NodeFSP.readFile(second.settingsPath, "utf8");
  const input = {
    stateDir,
    instanceId,
    nativeSessionId,
    configDirectory: descriptor().configDirectory,
  };
  await writeClaudeModelObserverBinding({ ...descriptor(), ...prepared, stateDir });
  const jobPath = NodePath.join(
    input.configDirectory,
    "jobs",
    nativeSessionId.slice(0, 8),
    "state.json",
  );
  await NodeFSP.mkdir(NodePath.dirname(jobPath), { recursive: true });
  await NodeFSP.writeFile(
    jobPath,
    JSON.stringify({
      sessionId: nativeSessionId,
      state: "stopped",
      respawnFlags: ["--settings", prepared.settingsPath, "--model", "native-model"],
    }),
  );
  const jobBefore = await NodeFSP.readFile(jobPath, "utf8");
  await setClaudeModelObserverEnabled({ ...input, enabled: false });
  expect(await isClaudeModelObserverBindingCurrent(input)).toBe(false);
  expect(await hasClaudeModelObserverBindingRouting(input)).toBe(true);
  expect(await readClaudeModelObserverBinding(input)).toMatchObject({
    enabled: false,
    settingsPath: prepared.settingsPath,
  });
  expect(JSON.parse(await NodeFSP.readFile(prepared.settingsPath, "utf8"))).toEqual({
    env: { ANTHROPIC_BASE_URL: prepared.upstreamUrl },
  });
  expect(await NodeFSP.readFile(second.settingsPath, "utf8")).toBe(secondSettingsBefore);
  await NodeFSP.writeFile(
    prepared.settingsPath,
    JSON.stringify({ env: { ANTHROPIC_BASE_URL: "https://other-valid-upstream.invalid/" } }),
  );
  expect(await hasClaudeModelObserverBindingRouting(input)).toBe(false);
  await NodeFSP.writeFile(
    prepared.settingsPath,
    JSON.stringify({ env: { ANTHROPIC_BASE_URL: prepared.upstreamUrl } }),
  );
  await setClaudeModelObserverEnabled({ ...input, enabled: true });
  expect(await isClaudeModelObserverBindingCurrent(input)).toBe(true);
  expect(await readClaudeModelObserverBinding(input)).toMatchObject({
    enabled: true,
    settingsPath: prepared.settingsPath,
  });
  expect(await NodeFSP.readFile(second.settingsPath, "utf8")).toBe(secondSettingsBefore);
  expect(await NodeFSP.readFile(jobPath, "utf8")).toBe(jobBefore);
});

it("refuses route changes for active or replaced native workers and rejects settings outside its private instance", async () => {
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
  await expect(
    writeClaudeModelObserverBinding({
      ...descriptor(),
      ...prepared,
      stateDir,
      settingsPath: "/tmp/not-our-settings.json",
    }),
  ).rejects.toThrow("settings path");
  await writeClaudeModelObserverBinding({ ...descriptor(), ...prepared, stateDir });
  const jobPath = NodePath.join(
    input.configDirectory,
    "jobs",
    nativeSessionId.slice(0, 8),
    "state.json",
  );
  await NodeFSP.mkdir(NodePath.dirname(jobPath), { recursive: true });
  const settingsBefore = await NodeFSP.readFile(prepared.settingsPath, "utf8");
  await NodeFSP.writeFile(
    jobPath,
    JSON.stringify({
      sessionId: nativeSessionId,
      state: "working",
      respawnFlags: ["--settings", prepared.settingsPath],
    }),
  );
  await expect(setClaudeModelObserverEnabled({ ...input, enabled: false })).rejects.toThrow(
    "Stop the native",
  );
  await NodeFSP.writeFile(
    jobPath,
    JSON.stringify({ sessionId: nativeSessionId, state: "stopped", respawnFlags: [] }),
  );
  await expect(setClaudeModelObserverEnabled({ ...input, enabled: false })).rejects.toThrow(
    "no longer matches",
  );
  expect(await NodeFSP.readFile(prepared.settingsPath, "utf8")).toBe(settingsBefore);
});

it("uses actual private routing after either half of a settings/descriptor update crashes and keeps recovery reachable", async () => {
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
  await writeClaudeModelObserverBinding({ ...descriptor(), ...prepared, stateDir });
  const descriptorPath = await storedPath();
  const jobPath = NodePath.join(
    input.configDirectory,
    "jobs",
    nativeSessionId.slice(0, 8),
    "state.json",
  );
  await NodeFSP.mkdir(NodePath.dirname(jobPath), { recursive: true });
  await NodeFSP.writeFile(
    jobPath,
    JSON.stringify({
      sessionId: nativeSessionId,
      state: "stopped",
      respawnFlags: ["--settings", prepared.settingsPath],
    }),
  );
  const proxySettings = await NodeFSP.readFile(prepared.settingsPath, "utf8");
  const ensureCalls = vi.mocked(ensureClaudeModelObserver).mock.calls.length;

  // Crash after disabling the actual route but before replacing enabled:true in the descriptor.
  await NodeFSP.writeFile(
    prepared.settingsPath,
    JSON.stringify({ env: { ANTHROPIC_BASE_URL: prepared.upstreamUrl } }),
  );
  expect(JSON.parse(await NodeFSP.readFile(descriptorPath, "utf8")).enabled).toBe(true);
  expect(await readClaudeModelObserverBinding(input)).toMatchObject({ enabled: false });
  expect(await hasClaudeModelObserverBindingRouting(input)).toBe(true);
  expect(await isClaudeModelObserverBindingCurrent(input)).toBe(false);
  expect(vi.mocked(ensureClaudeModelObserver).mock.calls).toHaveLength(ensureCalls);
  expect(JSON.parse(await NodeFSP.readFile(descriptorPath, "utf8")).enabled).toBe(true);
  await setClaudeModelObserverEnabled({ ...input, enabled: false });
  expect(JSON.parse(await NodeFSP.readFile(descriptorPath, "utf8")).enabled).toBe(false);

  // Reverse crash: proxy routing has been enabled, while the advisory descriptor still says false.
  await NodeFSP.writeFile(prepared.settingsPath, proxySettings);
  expect(await readClaudeModelObserverBinding(input)).toMatchObject({ enabled: true });
  expect(await hasClaudeModelObserverBindingRouting(input)).toBe(true);
  expect(await isClaudeModelObserverBindingCurrent(input)).toBe(true);
  expect(JSON.parse(await NodeFSP.readFile(descriptorPath, "utf8")).enabled).toBe(false);
  expect(vi.mocked(ensureClaudeModelObserver).mock.calls).toHaveLength(ensureCalls);
  await setClaudeModelObserverEnabled({ ...input, enabled: true });
  expect(JSON.parse(await NodeFSP.readFile(descriptorPath, "utf8")).enabled).toBe(true);

  // A different model gateway is not our disabled route and must never regain lifecycle ownership.
  await NodeFSP.writeFile(
    prepared.settingsPath,
    JSON.stringify({ env: { ANTHROPIC_BASE_URL: "https://different-gateway.invalid/" } }),
  );
  expect(await hasClaudeModelObserverBindingRouting(input)).toBe(false);
  expect(await isClaudeModelObserverBindingCurrent(input)).toBe(false);
});

it("rejects a private route whose telemetry destination or privacy settings were changed", async () => {
  const prepared = await prepareClaudeModelObserverSettings({
    stateDir,
    instanceId,
    upstreamUrl: descriptor().upstreamUrl,
  });
  await writeClaudeModelObserverBinding({ ...descriptor(), ...prepared, stateDir });
  const saved = JSON.parse(await NodeFSP.readFile(prepared.settingsPath, "utf8"));
  for (const [key, value] of [
    ["OTEL_EXPORTER_OTLP_TRACES_ENDPOINT", "https://external.invalid/v1/traces"],
    ["ENABLE_BETA_TRACING_DETAILED", "1"],
    ["OTEL_LOG_RAW_API_BODIES", "1"],
  ]) {
    await NodeFSP.writeFile(
      prepared.settingsPath,
      JSON.stringify({ env: { ...saved.env, [key!]: value } }),
    );
    await expect(
      readClaudeModelObserverBinding({ stateDir, instanceId, nativeSessionId }),
    ).rejects.toThrow("telemetry settings");
  }
});
