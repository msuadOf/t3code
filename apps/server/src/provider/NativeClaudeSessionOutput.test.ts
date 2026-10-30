// @effect-diagnostics nodeBuiltinImport:off -- Isolated session spool fixtures.
import * as NodeFSP from "node:fs/promises";
import * as NodeOS from "node:os";
import * as NodePath from "node:path";
import { expect, it } from "vite-plus/test";
import { NativeClaudeSessionOutput } from "./NativeClaudeSessionOutput.ts";
const sessionId = "11111111-2222-4333-8444-555555555555";
async function fixture() {
  const base = await NodeFSP.realpath(
    await NodeFSP.mkdtemp(NodePath.join(NodeOS.tmpdir(), "session-spool-")),
  );
  const root = NodePath.join(base, `claude-${process.getuid!()}`);
  await NodeFSP.mkdir(root, { mode: 0o700 });
  const dir = NodePath.join(root, "project", sessionId, "tasks");
  await NodeFSP.mkdir(dir, { recursive: true });
  return { base, root, dir, reader: new NativeClaudeSessionOutput(base) };
}
it("streams independent session-owned sources without needing tool receipts", async () => {
  const f = await fixture();
  try {
    await NodeFSP.writeFile(NodePath.join(f.dir, "one.output"), "first", { mode: 0o600 });
    await NodeFSP.writeFile(NodePath.join(f.dir, "two.output"), "second", { mode: 0o600 });
    const pages = await f.reader.sample(sessionId, 0);
    expect(pages.map((p) => p.chunk).sort()).toEqual(["first", "second"]);
    await NodeFSP.appendFile(NodePath.join(f.dir, "one.output"), " increment");
    expect(await f.reader.sample(sessionId, 0)).toEqual([
      {
        source: "project/one.output",
        chunk: " increment",
        output: "first increment",
        truncated: false,
      },
    ]);
    expect(await f.reader.sample("aaaaaaaa-2222-4333-8444-555555555555", 0)).toEqual([]);
  } finally {
    await NodeFSP.rm(f.base, { recursive: true, force: true });
  }
});
it("rejects symlink directories and files and does not replay old captures", async () => {
  const f = await fixture();
  try {
    const file = NodePath.join(f.dir, "one.output");
    await NodeFSP.writeFile(file, "private", { mode: 0o600 });
    await NodeFSP.utimes(file, 1, 1);
    expect(await f.reader.sample(sessionId, 2000)).toEqual([]);
    await NodeFSP.symlink(file, NodePath.join(f.dir, "alias.output"));
    expect((await f.reader.sample(sessionId, 0)).map((p) => p.source)).toEqual([
      "project/one.output",
    ]);
    await NodeFSP.rename(NodePath.dirname(f.dir), NodePath.dirname(f.dir) + "-saved");
    await NodeFSP.symlink(NodePath.dirname(f.dir) + "-saved", NodePath.dirname(f.dir));
    expect(await f.reader.sample(sessionId, 0)).toEqual([]);
  } finally {
    await NodeFSP.rm(f.base, { recursive: true, force: true });
  }
});

it("excludes known task sources and permanently rejects replaced files", async () => {
  const f = await fixture();
  try {
    const file = NodePath.join(f.dir, "task.output");
    await NodeFSP.writeFile(file, "one", { mode: 0o600 });
    expect(await f.reader.sample(sessionId, 0, [file])).toEqual([]);
    expect((await f.reader.sample(sessionId, 0))[0]?.chunk).toBe("one");
    await NodeFSP.rename(file, file + ".old");
    await NodeFSP.writeFile(file, "replacement", { mode: 0o600 });
    expect(await f.reader.sample(sessionId, 0)).toEqual([]);
    expect(await f.reader.sample(sessionId, 0)).toEqual([]);
  } finally {
    await NodeFSP.rm(f.base, { recursive: true, force: true });
  }
});

it("reports excessive discovery as a visible gap only once", async () => {
  const f = await fixture();
  try {
    await Promise.all(
      Array.from({ length: 129 }, (_, i) =>
        NodeFSP.writeFile(NodePath.join(f.dir, `task${i}.output`), "line", { mode: 0o600 }),
      ),
    );
    expect(await f.reader.sample(sessionId, 0)).toEqual([
      expect.objectContaining({
        source: "capture-limit",
        truncated: true,
        output: expect.stringContaining("discovery limit reached"),
      }),
    ]);
    expect(await f.reader.sample(sessionId, 0)).toEqual([]);
  } finally {
    await NodeFSP.rm(f.base, { recursive: true, force: true });
  }
});
