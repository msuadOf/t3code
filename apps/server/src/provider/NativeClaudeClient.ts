// @effect-diagnostics nodeBuiltinImport:off -- native daemon socket transport and CLI invocation use Node interoperability.
// @effect-diagnostics globalTimers:off -- bound the native socket handshake independently of Effect runtimes.
// @effect-diagnostics globalDate:off -- native daemon dispatch protocol requires a wall-clock timestamp.
import * as NodeChildProcess from "node:child_process";
import * as NodeFSP from "node:fs/promises";
import * as NodeNet from "node:net";
import * as NodePath from "node:path";
import * as NodeUtil from "node:util";

const execFileAsync = NodeUtil.promisify(NodeChildProcess.execFile);
const MAX_FRAME_BYTES = 1024 * 1024;
const SHORT_ID = /^[a-f0-9]{8}$/;

type JsonObject = Record<string, unknown>;
const isObject = (value: unknown): value is JsonObject =>
  value !== null && typeof value === "object" && !Array.isArray(value);

export interface NativeClaudeSession {
  readonly kind: "interactive" | "background";
  readonly cwd: string;
  readonly startedAt: number;
  readonly id?: string;
  readonly sessionId?: string;
  readonly name?: string;
  readonly pid?: number;
  readonly status?: string;
  readonly state?: string;
  readonly waitingFor?: string;
}

export class NativeClaudeError extends Error {
  readonly code: string;
  constructor(code: string, message: string) {
    super(message);
    this.code = code;
    this.name = "NativeClaudeError";
  }
}

export function decodeNativeClaudeInventory(value: unknown): readonly NativeClaudeSession[] {
  if (!Array.isArray(value)) {
    throw new NativeClaudeError("EPROTOCOL", "Claude session inventory is not an array.");
  }
  return value.map((item: unknown) => {
    if (
      !isObject(item) ||
      (item.kind !== "interactive" && item.kind !== "background") ||
      typeof item.cwd !== "string" ||
      typeof item.startedAt !== "number"
    ) {
      throw new NativeClaudeError(
        "EPROTOCOL",
        "Claude returned an invalid session inventory entry.",
      );
    }
    for (const field of ["id", "sessionId", "name", "status", "state", "waitingFor"] as const) {
      if (item[field] !== undefined && typeof item[field] !== "string") {
        throw new NativeClaudeError("EPROTOCOL", `Claude session field ${field} is invalid.`);
      }
    }
    if (item.pid !== undefined && typeof item.pid !== "number") {
      throw new NativeClaudeError("EPROTOCOL", "Claude session PID is invalid.");
    }
    return item as unknown as NativeClaudeSession;
  });
}

export function nativeClaudeSessionIsInDirectory(
  session: NativeClaudeSession,
  root: string,
): boolean {
  const path = NodePath.relative(root, session.cwd);
  return (
    path === "" ||
    (!NodePath.isAbsolute(path) &&
      path !== ".." &&
      !path.startsWith("../") &&
      !path.startsWith("..\\"))
  );
}

export function requireNativeClaudeBackgroundId(session: NativeClaudeSession): string {
  if (session.kind !== "background" || !session.id || !SHORT_ID.test(session.id)) {
    throw new NativeClaudeError(
      "EFOREGROUND",
      "This Claude session is open in a foreground terminal. Use /background or the left-arrow agent-view action in that terminal, then connect to its background session. Starting another --resume process would not share the running session.",
    );
  }
  return session.id;
}

export interface NativeClaudeClientOptions {
  readonly configDirectory?: string;
  readonly platform?: NodeJS.Platform;
  readonly binaryPath?: string;
  readonly environment?: NodeJS.ProcessEnv;
  readonly cwd?: string;
  readonly socketPath?: string;
  readonly timeoutMs?: number;
}

/** Native daemon control is internal. Subscribe/attach transport terminal bytes, not SDK deltas. */
export class NativeClaudeClient {
  private readonly options: NativeClaudeClientOptions;
  constructor(options: NativeClaudeClientOptions = {}) {
    this.options = options;
  }

  async listSessions(directory?: string): Promise<readonly NativeClaudeSession[]> {
    const { stdout } = await execFileAsync(
      this.options.binaryPath || "claude",
      ["agents", "--json", "--all"],
      {
        env: this.options.environment ?? process.env,
        cwd: this.options.cwd,
        timeout: this.options.timeoutMs ?? 5000,
        maxBuffer: 16 * MAX_FRAME_BYTES,
      },
    );
    const sessions = decodeNativeClaudeInventory(JSON.parse(stdout));
    return directory
      ? sessions.filter((session) => nativeClaudeSessionIsInDirectory(session, directory))
      : sessions;
  }

  private async endpoint(): Promise<string> {
    if (this.options.socketPath) return this.options.socketPath;
    // status is read-only; never start/stop the user's supervisor as a side effect of discovery.
    let output: string;
    try {
      const result = await execFileAsync(
        this.options.binaryPath || "claude",
        ["daemon", "status"],
        {
          env: this.options.environment ?? process.env,
          cwd: this.options.cwd,
          timeout: this.options.timeoutMs ?? 5000,
        },
      );
      output = result.stdout;
    } catch (error) {
      output = isObject(error) && typeof error.stdout === "string" ? error.stdout : "";
    }
    const path = /^\s*sock dir:\s*(.+)\s*$/m.exec(output)?.[1]?.trim();
    if (!path || !NodePath.isAbsolute(path)) {
      throw new NativeClaudeError(
        "ENODAEMON",
        "Claude's native supervisor is unavailable. Start a shared session with claude --bg, or background an existing terminal session.",
      );
    }
    return NodePath.join(path, "control.sock");
  }

  private async open(
    request: JsonObject,
    authenticated: boolean,
    signal?: AbortSignal,
  ): Promise<NodeNet.Socket> {
    const path = await this.endpoint();
    const auth = authenticated
      ? (
          await NodeFSP.readFile(
            this.options.configDirectory
              ? NodePath.join(this.options.configDirectory, "daemon", "control.key")
              : NodePath.join(NodePath.dirname(path), "control.key"),
            "utf8",
          )
        ).trim()
      : undefined;
    if (authenticated && !auth)
      throw new NativeClaudeError("EAUTH", "Claude daemon control key is empty.");
    signal?.throwIfAborted();
    return await new Promise<NodeNet.Socket>((resolve, reject) => {
      const socket = NodeNet.createConnection({ path, ...(signal ? { signal } : {}) });
      const failed = (error: Error) => {
        socket.destroy();
        reject(error);
      };
      socket.once("error", failed);
      socket.setTimeout(this.options.timeoutMs ?? 5000, () =>
        failed(new NativeClaudeError("ETIMEOUT", "Claude daemon did not accept the connection.")),
      );
      socket.once("connect", () => {
        socket.removeListener("error", failed);
        socket.setTimeout(0);
        socket.write(`${JSON.stringify({ proto: 1, ...request, ...(auth ? { auth } : {}) })}\n`);
        resolve(socket);
      });
    });
  }

  private async *frames(
    request: JsonObject,
    authenticated: boolean,
    signal?: AbortSignal,
  ): AsyncGenerator<JsonObject> {
    const socket = await this.open(request, authenticated, signal);
    let buffered = Buffer.alloc(0);
    try {
      for await (const chunk of socket) {
        buffered = Buffer.concat([buffered, Buffer.from(chunk)]);
        let newline: number;
        while ((newline = buffered.indexOf(10)) >= 0) {
          if (newline > MAX_FRAME_BYTES)
            throw new NativeClaudeError(
              "EPROTOCOL",
              "Claude daemon frame exceeds the supported size.",
            );
          const frame: unknown = JSON.parse(buffered.subarray(0, newline).toString("utf8"));
          buffered = buffered.subarray(newline + 1);
          if (!isObject(frame))
            throw new NativeClaudeError("EPROTOCOL", "Claude daemon returned an invalid frame.");
          if (frame.ok === false)
            throw new NativeClaudeError(
              typeof frame.code === "string" ? frame.code : "EDAEMON",
              "Claude daemon rejected the request.",
            );
          yield frame;
        }
        if (buffered.length > MAX_FRAME_BYTES)
          throw new NativeClaudeError(
            "EPROTOCOL",
            "Claude daemon frame exceeds the supported size.",
          );
      }
      if (buffered.length)
        throw new NativeClaudeError("EPROTOCOL", "Claude daemon closed with an incomplete frame.");
    } finally {
      socket.destroy();
    }
  }

  private async request(request: JsonObject): Promise<JsonObject> {
    const controller = new AbortController();
    const timer = setTimeout(
      () =>
        controller.abort(
          new NativeClaudeError("ETIMEOUT", "Claude daemon did not answer the request."),
        ),
      this.options.timeoutMs ?? 5000,
    );
    try {
      for await (const frame of this.frames(request, true, controller.signal)) {
        if (frame.ok !== true || frame.op !== request.op)
          throw new NativeClaudeError(
            "EPROTOCOL",
            "Claude daemon returned an unexpected acknowledgement.",
          );
        return frame;
      }
      throw new NativeClaudeError(
        "EPROTOCOL",
        "Claude daemon closed without acknowledging the request.",
      );
    } finally {
      clearTimeout(timer);
    }
  }

  async reply(session: NativeClaudeSession, text: string): Promise<void> {
    await this.request({ op: "reply", short: requireNativeClaudeBackgroundId(session), text });
  }

  /** Events contain terminal output and daemon state; callers must not treat them as SDK messages. */
  subscribe(session: NativeClaudeSession, signal?: AbortSignal): AsyncGenerator<JsonObject> {
    return this.frames(
      { op: "subscribe", short: requireNativeClaudeBackgroundId(session), tail: 200 },
      false,
      signal,
    );
  }

  /** Returns the actual shared PTY. Its bytes include ANSI and input is terminal input. */
  async attach(
    session: NativeClaudeSession,
    dimensions: { readonly cols: number; readonly rows: number },
    signal?: AbortSignal,
  ): Promise<NodeNet.Socket> {
    const short = requireNativeClaudeBackgroundId(session);
    if (
      !Number.isInteger(dimensions.cols) ||
      !Number.isInteger(dimensions.rows) ||
      dimensions.cols < 1 ||
      dimensions.rows < 1
    ) {
      throw new NativeClaudeError(
        "EDIMENSIONS",
        "Claude terminal dimensions must be positive integers.",
      );
    }
    const socket = await this.open({ op: "attach", short, ...dimensions }, true, signal);
    return await new Promise<NodeNet.Socket>((resolve, reject) => {
      let buffered = Buffer.alloc(0);
      const timer = setTimeout(
        () =>
          failed(
            new NativeClaudeError(
              "ETIMEOUT",
              "Claude daemon did not acknowledge terminal attachment.",
            ),
          ),
        this.options.timeoutMs ?? 5000,
      );
      const cleanup = () => {
        clearTimeout(timer);
        socket.off("data", received);
        socket.off("error", failed);
        socket.off("end", ended);
      };
      const failed = (error: Error) => {
        cleanup();
        socket.destroy();
        reject(error);
      };
      const ended = () =>
        failed(
          new NativeClaudeError("EPROTOCOL", "Claude daemon closed before terminal attachment."),
        );
      const received = (chunk: Buffer) => {
        buffered = Buffer.concat([buffered, chunk]);
        const newline = buffered.indexOf(10);
        if (newline < 0) {
          if (buffered.length > MAX_FRAME_BYTES)
            failed(
              new NativeClaudeError(
                "EPROTOCOL",
                "Claude attach acknowledgement exceeds the supported size.",
              ),
            );
          return;
        }
        try {
          const reply: unknown = JSON.parse(buffered.subarray(0, newline).toString("utf8"));
          if (!isObject(reply) || reply.ok !== true || reply.op !== "attach") {
            throw new NativeClaudeError(
              isObject(reply) && typeof reply.code === "string" ? reply.code : "EPROTOCOL",
              "Claude daemon rejected terminal attachment.",
            );
          }
          socket.pause();
          cleanup();
          const remainder = buffered.subarray(newline + 1);
          if (remainder.length) socket.unshift(remainder);
          resolve(socket);
        } catch (error) {
          failed(error instanceof Error ? error : new Error(String(error)));
        }
      };
      socket.on("data", received);
      socket.once("error", failed);
      socket.once("end", ended);
    });
  }

  /** A prompt-less --bg job is lazy until its first native attachment. */
  private async readyBackground(session: NativeClaudeSession): Promise<NativeClaudeSession> {
    if (session.status !== undefined) return session;
    const terminal = await this.attach(session, { cols: 80, rows: 24 });
    terminal.destroy();
    for (let attempt = 0; attempt < 50; attempt += 1) {
      const ready = (await this.listSessions()).find(
        (value) => value.id === session.id && value.sessionId === session.sessionId,
      );
      if (ready?.status !== undefined) return ready;
      if (ready?.state === "failed" || ready?.state === "stopped")
        throw new NativeClaudeError(
          "ETERMINAL",
          "Claude native worker stopped while opening its session.",
        );
      await new Promise<void>((resolve) => setTimeout(resolve, 100));
    }
    throw new NativeClaudeError(
      "ETIMEOUT",
      "Claude native worker did not become ready. Open it in claude attach to finish its setup.",
    );
  }

  private async runCLI(args: readonly string[], cwd: string) {
    const launched = execFileAsync(this.options.binaryPath || "claude", args, {
      env: this.options.environment ?? process.env,
      cwd,
      timeout: 30000,
      maxBuffer: MAX_FRAME_BYTES,
    });
    launched.child.stdin?.end();
    return launched;
  }

  /** CLI attach performs the native, non-forcing wake operation; raw attach cannot wake stopped jobs. */
  private async wakeBackground(
    session: NativeClaudeSession,
    cwd: string,
  ): Promise<NativeClaudeSession> {
    const short = requireNativeClaudeBackgroundId(session);
    const child = NodeChildProcess.spawn(this.options.binaryPath || "claude", ["attach", short], {
      env: this.options.environment ?? process.env,
      cwd,
      stdio: ["pipe", "ignore", "ignore"],
    });
    let failed = false;
    const closed = new Promise<void>((resolve) => {
      child.once("error", () => {
        failed = true;
        resolve();
      });
      child.once("close", () => resolve());
    });
    try {
      for (let attempt = 0; attempt < 100; attempt += 1) {
        if (failed)
          throw new NativeClaudeError("EWAKE", "Could not open Claude's native attach client.");
        const candidates = (await this.listSessions()).filter(
          (value) => value.sessionId === session.sessionId,
        );
        const foreground = candidates.find((value) => value.kind === "interactive");
        if (foreground) requireNativeClaudeBackgroundId(foreground);
        const ready = candidates.find((value) => value.id === short);
        if (ready?.status !== undefined && ready.state !== "stopped" && ready.state !== "failed")
          return ready;
        await new Promise<void>((resolve) => setTimeout(resolve, 100));
      }
      throw new NativeClaudeError(
        "ETIMEOUT",
        "Claude could not reopen this background session. Open it in claude attach to finish its setup.",
      );
    } finally {
      // Only this client's process is terminated; the daemon-owned worker stays alive.
      child.stdin?.destroy();
      if (child.exitCode === null && child.signalCode === null) child.kill("SIGTERM");
      await closed;
    }
  }

  /** Launch one daemon-owned native session; never creates a second writer for a live session. */
  async startBackground(input: {
    readonly sessionId?: string;
    readonly cwd: string;
    readonly model?: string;
    readonly permissionMode?: string;
    readonly resume?: boolean;
    readonly settings?: string;
  }): Promise<NativeClaudeSession> {
    if (this.options.platform === "win32")
      throw new NativeClaudeError(
        "EPLATFORM",
        "Native Claude shared workers currently require a macOS or Linux server host.",
      );
    const inventory = input.sessionId ? await this.listSessions() : [];
    const foreground = inventory.find(
      (value) => value.sessionId === input.sessionId && value.kind === "interactive",
    );
    if (foreground) requireNativeClaudeBackgroundId(foreground);
    const existing = inventory.find((value) => value.sessionId === input.sessionId);
    if (existing) {
      requireNativeClaudeBackgroundId(existing);
      if (existing.state === "failed" || existing.state === "stopped")
        return this.wakeBackground(existing, input.cwd);
      return this.readyBackground(existing);
    }
    if (
      input.resume &&
      (!input.sessionId || !/^[a-f0-9]{8}-(?:[a-f0-9]{4}-){3}[a-f0-9]{12}$/.test(input.sessionId))
    )
      throw new NativeClaudeError(
        "ESESSION",
        "A canonical full Claude session UUID is required to resume its history.",
      );
    const args = [
      "--bg",
      // Saved sessions retain their own options; overrides make the CLI fork a copy.
      ...(input.resume && input.sessionId
        ? ["--resume", input.sessionId]
        : [
            ...(input.model ? ["--model", input.model] : []),
            ...(input.permissionMode ? ["--permission-mode", input.permissionMode] : []),
            ...(input.settings ? ["--settings", input.settings] : []),
          ]),
    ];
    const output = await this.runCLI(args, input.cwd);
    const short = /backgrounded\s*·\s*([a-f0-9]{8})\b/.exec(output.stdout)?.[1];
    if (!short)
      throw new NativeClaudeError(
        "EPROTOCOL",
        "Claude did not return an identifiable background job.",
      );
    const created = (await this.listSessions()).find(
      (session) => session.id === short && session.kind === "background",
    );
    if (!created?.sessionId)
      throw new NativeClaudeError(
        "ENOJOB",
        "Claude did not register the requested background session.",
      );
    requireNativeClaudeBackgroundId(created);
    if (input.resume && created.sessionId !== input.sessionId) {
      // Native CLI can fork when another owner wins its claim. Never submit to that copy.
      await this.runCLI(["stop", short], input.cwd);
      throw new NativeClaudeError(
        "ESESSIONCOPY",
        "Claude could not claim the original session and created a separate copy. That copy was stopped; no message was sent. Reopen the original session after its CLI ownership changes.",
      );
    }
    return this.readyBackground(created);
  }

  async interrupt(session: NativeClaudeSession): Promise<void> {
    const short = requireNativeClaudeBackgroundId(session);
    const verify = async () => {
      const current = (await this.listSessions()).find(
        (candidate) => candidate.sessionId === session.sessionId,
      );
      if (
        session.sessionId === undefined ||
        session.pid === undefined ||
        current?.kind !== "background" ||
        current.id !== short ||
        current.pid !== session.pid ||
        current.state === "stopped" ||
        current.state === "failed"
      ) {
        throw new NativeClaudeError(
          "EOWNER",
          "Claude's native worker changed; no interrupt was sent.",
        );
      }
      if (current.status !== "busy") {
        throw new NativeClaudeError(
          "EIDLE",
          "Claude is not running a turn; no interrupt was sent.",
        );
      }
    };
    await verify();
    // Native attach requires dimensions. Closing it restores the remaining attacher's size.
    const terminal = await this.attach(session, { cols: 80, rows: 24 });
    try {
      await verify();
      await new Promise<void>((resolve, reject) =>
        terminal.write("\u0003", (error) => (error ? reject(error) : resolve())),
      );
    } finally {
      terminal.destroy();
    }
  }

  /** No permission-response method: current daemon acknowledges it without applying a decision. */
  async dispatch(input: {
    readonly sessionId: string;
    readonly cwd: string;
    readonly args: readonly string[];
    readonly environment?: Readonly<Record<string, string>>;
  }): Promise<JsonObject> {
    if (!/^[a-f0-9]{8}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{12}$/.test(input.sessionId))
      throw new NativeClaudeError("ESESSION", "A canonical Claude session UUID is required.");
    const short = input.sessionId.slice(0, 8);
    return this.request({
      op: "dispatch",
      timeoutMs: this.options.timeoutMs ?? 5000,
      d: {
        proto: 1,
        short,
        sessionId: input.sessionId,
        createdAt: Date.now(),
        source: "shell",
        cwd: input.cwd,
        launch: { mode: "prompt", args: ["--session-id", input.sessionId, ...input.args] },
        env: input.environment ?? {},
        isolation: "none",
        respawnFlags: [],
      },
    });
  }
}
