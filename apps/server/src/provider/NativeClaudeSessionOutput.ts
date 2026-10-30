// @effect-diagnostics nodeBuiltinImport:off -- Read-only native session spool discovery.
import * as NodeFSP from "node:fs/promises";
import * as NodePath from "node:path";
import { NativeClaudeCommandOutput } from "./NativeClaudeCommandOutput.ts";

const UUID = /^[a-f0-9]{8}-(?:[a-f0-9]{4}-){3}[a-f0-9]{12}$/i;
/** Session identity comes from the native directory, never a guessed tool-call match. */
export class NativeClaudeSessionOutput {
  private limitReported = false;
  private limitNotice() {
    if (this.limitReported) return [];
    this.limitReported = true;
    const output =
      "Session log capture is incomplete: native source discovery limit reached. The native CLI view remains available.";
    return [{ source: "capture-limit", chunk: output, output, truncated: true }];
  }
  private readonly rejected = new Set<string>();
  private readonly gaps = new Set<string>();
  private readonly readers = new Map<string, NativeClaudeCommandOutput>();
  private readonly temporaryDirectory: string;
  // Stock Claude defaults its per-uid task spool to /tmp, including on macOS.
  // os.tmpdir() instead follows TMPDIR and can point at an unrelated directory.
  constructor(temporaryDirectory: string = "/tmp") {
    this.temporaryDirectory = temporaryDirectory;
  }
  async sample(sessionId: string, since: number, excludedPaths: readonly string[] = []) {
    if (!UUID.test(sessionId) || !Number.isFinite(since)) return [];
    const uid = process.getuid?.();
    if (uid === undefined) return [];
    const base = await NodeFSP.realpath(this.temporaryDirectory);
    const root = NodePath.join(base, `claude-${uid}`);
    let directories: string[];
    try {
      const stat = await NodeFSP.lstat(root);
      if (!stat.isDirectory() || stat.isSymbolicLink() || stat.uid !== uid || stat.mode & 0o077)
        return [];
      directories = await NodeFSP.readdir(root);
    } catch {
      return [];
    }
    // Never scan an unbounded native tmp tree or follow arbitrary nested paths.
    if (directories.length > 256) return this.limitNotice();
    const result: { source: string; chunk: string; output: string; truncated: boolean }[] = [];
    for (const directory of directories) {
      if (
        [...String(directory)].some((char) => char.charCodeAt(0) < 32 || char.charCodeAt(0) === 127)
      )
        continue;
      const tasks = NodePath.join(root, String(directory), sessionId, "tasks");
      let names: string[];
      try {
        for (const path of [
          NodePath.dirname(NodePath.dirname(tasks)),
          NodePath.dirname(tasks),
          tasks,
        ]) {
          const stat = await NodeFSP.lstat(path);
          if (!stat.isDirectory() || stat.isSymbolicLink() || stat.uid !== uid || stat.mode & 0o022)
            throw new Error("Unsafe native session directory");
        }
        names = await NodeFSP.readdir(tasks);
      } catch {
        continue;
      }
      if (names.length > 128) {
        result.push(...this.limitNotice());
        continue;
      }
      for (const name of names) {
        if (!/^[a-zA-Z0-9_-]{1,128}\.output$/.test(name)) continue;
        const path = NodePath.join(tasks, name);
        if (this.rejected.has(path) || excludedPaths.includes(path)) continue;
        try {
          const stat = await NodeFSP.lstat(path);
          // Old session files are not replayed into a later human turn. This is a
          // capture boundary, not evidence linking a source to a tool or turn.
          if (stat.mtimeMs < since || stat.isSymbolicLink()) continue;
          let reader = this.readers.get(path);
          if (!reader) {
            if (this.readers.size >= 32) {
              result.push(...this.limitNotice());
              continue;
            }
            reader = new NativeClaudeCommandOutput({
              sessionId,
              taskId: name.slice(0, -7),
              outputPath: path,
            });
            this.readers.set(path, reader);
          }
          const page = await reader.sample();
          if (page.chunk || (page.truncated && !this.gaps.has(path)))
            result.push({
              source: `${directory}/${name}`,
              chunk: page.chunk,
              output: page.output,
              truncated: page.truncated,
            });
          if (page.truncated) this.gaps.add(path);
        } catch {
          if (this.readers.has(path)) {
            this.rejected.add(path);
            this.readers.delete(path);
          }
        }
      }
    }
    return result;
  }
}
