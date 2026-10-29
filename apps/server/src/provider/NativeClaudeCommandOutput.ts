// @effect-diagnostics nodeBuiltinImport:off -- Read-only native file observation interoperates with the native socket client.
import * as NodeFSP from "node:fs/promises";
import * as NodeFS from "node:fs";
import * as NodePath from "node:path";
import * as NodeStringDecoder from "node:string_decoder";

const LIMIT = 64 * 1024;
export class NativeClaudeCommandOutput {
  private cursor: number | undefined;
  private inode: string | undefined;
  private decoder = new NodeStringDecoder.StringDecoder("utf8");
  private output = "";
  private truncated = false;
  private finished = false;
  private readonly input: {
    readonly sessionId: string;
    readonly taskId: string;
    readonly outputPath: string;
  };
  constructor(input: {
    readonly sessionId: string;
    readonly taskId: string;
    readonly outputPath: string;
  }) {
    this.input = input;
  }
  async sample(options: { readonly final?: boolean } = {}) {
    if (this.finished)
      return { chunk: "", output: this.output, truncated: this.truncated, hasMore: false };
    const { sessionId, taskId, outputPath } = this.input;
    const uid = process.getuid?.();
    const taskDir = NodePath.dirname(outputPath),
      sessionDir = NodePath.dirname(taskDir),
      cwdDir = NodePath.dirname(sessionDir),
      root = NodePath.dirname(cwdDir);
    if (
      uid === undefined ||
      !/^[a-f0-9]{8}-(?:[a-f0-9]{4}-){3}[a-f0-9]{12}$/i.test(sessionId) ||
      !/^[a-zA-Z0-9_-]{1,128}$/.test(taskId) ||
      !NodePath.isAbsolute(outputPath) ||
      NodePath.normalize(outputPath) !== outputPath ||
      NodePath.basename(outputPath) !== `${taskId}.output` ||
      NodePath.basename(taskDir) !== "tasks" ||
      NodePath.basename(sessionDir) !== sessionId ||
      NodePath.basename(root) !== `claude-${uid}`
    )
      throw new Error("Unsupported native Claude command output path");
    let file: Awaited<ReturnType<typeof NodeFSP.open>> | undefined;
    try {
      for (const directory of [root, cwdDir, sessionDir, taskDir]) {
        const stat = await NodeFSP.lstat(directory);
        if (
          !stat.isDirectory() ||
          stat.isSymbolicLink() ||
          stat.uid !== uid ||
          (stat.mode & 0o022) !== 0 ||
          (directory === root && (stat.mode & 0o077) !== 0)
        )
          throw new Error("Unsafe native Claude command output directory");
      }
      if ((await NodeFSP.realpath(outputPath)) !== outputPath)
        throw new Error("Aliased native Claude command output");
      const before = await NodeFSP.lstat(outputPath);
      file = await NodeFSP.open(
        outputPath,
        NodeFS.constants.O_RDONLY | NodeFS.constants.O_NOFOLLOW,
      );
      const stat = await file.stat();
      const identity = `${stat.dev}:${stat.ino}`;
      if (
        !stat.isFile() ||
        stat.uid !== uid ||
        stat.nlink !== 1 ||
        (stat.mode & 0o022) !== 0 ||
        before.dev !== stat.dev ||
        before.ino !== stat.ino ||
        (this.inode !== undefined && this.inode !== identity) ||
        (this.cursor !== undefined && stat.size < this.cursor)
      )
        throw new Error("Native Claude command output changed identity");
      this.inode = identity;
      const initial = this.cursor === undefined;
      this.cursor ??= Math.max(0, stat.size - LIMIT);
      if (initial && this.cursor > 0) this.truncated = true;
      const buffer = Buffer.alloc(Math.min(LIMIT, stat.size - this.cursor));
      const { bytesRead } = await file.read(buffer, 0, buffer.length, this.cursor);
      this.cursor += bytesRead;
      let bytes = buffer.subarray(0, bytesRead);
      if (initial && this.truncated) {
        let skip = 0;
        while (skip < bytes.length && (bytes[skip]! & 0xc0) === 0x80) skip++;
        bytes = bytes.subarray(skip);
      }
      const hasMore = this.cursor < stat.size;
      let chunk = this.decoder.write(bytes);
      if (options.final && !hasMore) {
        chunk += this.decoder.end();
        this.finished = true;
      }
      this.output += chunk;
      if (this.output.length > LIMIT) {
        this.output = this.output.slice(-LIMIT);
        if (this.output.charCodeAt(0) >= 0xdc00 && this.output.charCodeAt(0) <= 0xdfff)
          this.output = this.output.slice(1);
        this.truncated = true;
      }
      return { chunk, output: this.output, truncated: this.truncated, hasMore };
    } catch (error) {
      if (
        typeof error === "object" &&
        error !== null &&
        "code" in error &&
        error.code === "ENOENT" &&
        !options.final &&
        this.inode === undefined
      )
        return { chunk: "", output: this.output, truncated: this.truncated, hasMore: false };
      throw error;
    } finally {
      await file?.close();
    }
  }
}
