// @effect-diagnostics nodeBuiltinImport:off -- Isolated native spool fixtures.
import * as NodeFSP from "node:fs/promises";
import * as NodeOS from "node:os";
import * as NodePath from "node:path";
import { expect, it } from "vite-plus/test";
import { NativeClaudeCommandOutput } from "./NativeClaudeCommandOutput.ts";
const sessionId = "11111111-2222-4333-8444-555555555555";
async function fixture() {
  const base = await NodeFSP.realpath(
    await NodeFSP.mkdtemp(NodePath.join(NodeOS.tmpdir(), "native-output-")),
  );
  const root = NodePath.join(base, `claude-${process.getuid!()}`);
  await NodeFSP.mkdir(root, { mode: 0o700 });
  const dir = NodePath.join(root, "project", sessionId, "tasks");
  await NodeFSP.mkdir(dir, { recursive: true });
  const outputPath = NodePath.join(dir, "task.output");
  return {
    base,
    dir,
    outputPath,
    reader: new NativeClaudeCommandOutput({ sessionId, taskId: "task", outputPath }),
  };
}
it("follows split UTF8 by byte cursor and finalizes only on explicit final sampling", async () => {
  const f = await fixture();
  try {
    expect((await f.reader.sample()).chunk).toBe("");
    const bytes = Buffer.from("中🙂");
    await NodeFSP.writeFile(f.outputPath, bytes.subarray(0, 2), { mode: 0o644 });
    expect((await f.reader.sample()).chunk).toBe("");
    await NodeFSP.appendFile(f.outputPath, bytes.subarray(2));
    expect((await f.reader.sample()).chunk).toBe("中🙂");
    await NodeFSP.appendFile(f.outputPath, "\n[exited with code 0]\n");
    expect((await f.reader.sample()).chunk).toContain("exited");
    await NodeFSP.appendFile(f.outputPath, "still native output");
    expect((await f.reader.sample({ final: true })).output).toContain("still native output");
    expect((await f.reader.sample()).chunk).toBe("");
  } finally {
    await NodeFSP.rm(f.base, { recursive: true, force: true });
  }
});
it("bounds late-join tails and avoids initial UTF8 continuation bytes", async () => {
  const f = await fixture();
  try {
    await NodeFSP.writeFile(f.outputPath, "中".repeat(30000));
    const r = await f.reader.sample({ final: true });
    expect(r.truncated).toBe(true);
    expect(r.output.length).toBeLessThanOrEqual(65536);
    expect(r.output).not.toContain("�");
  } finally {
    await NodeFSP.rm(f.base, { recursive: true, force: true });
  }
});
it("rejects replacement, truncation, symlinks and cross-session paths", async () => {
  const f = await fixture();
  try {
    await NodeFSP.writeFile(f.outputPath, "first");
    await f.reader.sample();
    await NodeFSP.truncate(f.outputPath, 0);
    await expect(f.reader.sample()).rejects.toThrow("identity");
    await NodeFSP.unlink(f.outputPath);
    await NodeFSP.symlink(NodePath.join(f.base, "target"), f.outputPath);
    await NodeFSP.writeFile(NodePath.join(f.base, "target"), "private");
    await expect(
      new NativeClaudeCommandOutput({
        sessionId,
        taskId: "task",
        outputPath: f.outputPath,
      }).sample(),
    ).rejects.toThrow();
    await expect(
      new NativeClaudeCommandOutput({
        sessionId: "aaaaaaaa-2222-4333-8444-555555555555",
        taskId: "task",
        outputPath: f.outputPath,
      }).sample(),
    ).rejects.toThrow("path");
  } finally {
    await NodeFSP.rm(f.base, { recursive: true, force: true });
  }
});
it("drains appended backlog without treating decoded character length as cursor", async () => {
  const f = await fixture();
  try {
    await NodeFSP.writeFile(f.outputPath, "");
    await f.reader.sample();
    await NodeFSP.appendFile(f.outputPath, "🙂".repeat(40000));
    const a = await f.reader.sample();
    expect(a.hasMore).toBe(true);
    const b = await f.reader.sample();
    expect(b.hasMore).toBe(true);
    const c = await f.reader.sample({ final: true });
    expect(c.hasMore).toBe(false);
    expect(a.chunk + b.chunk + c.chunk).toBe("🙂".repeat(40000));
  } finally {
    await NodeFSP.rm(f.base, { recursive: true, force: true });
  }
});

it("rejects hard-linked output and missing final files", async () => {
  const f = await fixture();
  try {
    await expect(f.reader.sample({ final: true })).rejects.toThrow();
    await NodeFSP.writeFile(f.outputPath, "data");
    await NodeFSP.link(f.outputPath, NodePath.join(f.dir, "alias"));
    await expect(f.reader.sample()).rejects.toThrow("identity");
  } finally {
    await NodeFSP.rm(f.base, { recursive: true, force: true });
  }
});
