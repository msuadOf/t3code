// @effect-diagnostics nodeBuiltinImport:off -- private worker settings filesystem fixture.
import * as NodeFSP from "node:fs/promises";
import * as NodePath from "node:path";
import * as NodeOS from "node:os";
import { expect, it } from "vite-plus/test";
import {
  prepareClaudeBackgroundSettings,
  ownedClaudeBackgroundSettings,
  readClaudeBackgroundPolicy,
  withoutClaudeBackgroundPolicy,
} from "./NativeClaudeBackgroundPolicy.ts";

it("creates strictly owned policy settings without overriding native command arguments or max timeout", async () => {
  const dir = await NodeFSP.mkdtemp(NodePath.join(NodeOS.tmpdir(), "t3-background-policy-"));
  try {
    const file = await prepareClaudeBackgroundSettings(dir);
    expect(await ownedClaudeBackgroundSettings(dir, ["--settings", file])).toBe(true);
    expect(await readClaudeBackgroundPolicy(file)).toBe(true);
    expect(JSON.parse(await NodeFSP.readFile(file, "utf8"))).toEqual({
      env: { BASH_DEFAULT_TIMEOUT_MS: "5000", CLAUDE_CODE_DISABLE_BACKGROUND_TASKS: "0" },
    });
    expect((await NodeFSP.stat(file)).mode & 0o777).toBe(0o600);
    expect((await NodeFSP.stat(NodePath.dirname(file))).mode & 0o777).toBe(0o700);
    await NodeFSP.writeFile(
      file,
      JSON.stringify({
        env: {
          BASH_DEFAULT_TIMEOUT_MS: "5000",
          CLAUDE_CODE_DISABLE_BACKGROUND_TASKS: "0",
          BASH_MAX_TIMEOUT_MS: "10000",
        },
      }),
    );
    expect(await ownedClaudeBackgroundSettings(dir, ["--settings", file])).toBe(false);
  } finally {
    await NodeFSP.rm(dir, { recursive: true, force: true });
  }
});

it("combines policy with private observer settings and rejects spoofed partial policy", async () => {
  const dir = await NodeFSP.mkdtemp(NodePath.join(NodeOS.tmpdir(), "t3-background-combined-"));
  try {
    const file = NodePath.join(dir, "observer.json");
    await NodeFSP.writeFile(
      file,
      JSON.stringify({ env: { ANTHROPIC_BASE_URL: "http://127.0.0.1:1", OTHER: "retained" } }),
      { mode: 0o600 },
    );
    expect(await prepareClaudeBackgroundSettings(dir, file)).toBe(file);
    const env = JSON.parse(await NodeFSP.readFile(file, "utf8")).env;
    expect(withoutClaudeBackgroundPolicy(env)).toEqual({
      ANTHROPIC_BASE_URL: "http://127.0.0.1:1",
      OTHER: "retained",
    });
    expect(() => withoutClaudeBackgroundPolicy({ BASH_DEFAULT_TIMEOUT_MS: "5000" })).toThrow();
    const link = NodePath.join(dir, "link.json");
    await NodeFSP.symlink(file, link);
    await expect(readClaudeBackgroundPolicy(link)).rejects.toThrow();
  } finally {
    await NodeFSP.rm(dir, { recursive: true, force: true });
  }
});
