// @effect-diagnostics nodeBuiltinImport:off -- native daemon socket transport and CLI invocation use Node interoperability.
// @effect-diagnostics globalTimers:off -- bound the native socket handshake independently of Effect runtimes.
// @effect-diagnostics globalDate:off -- native daemon dispatch protocol requires a wall-clock timestamp.
import * as NodeChildProcess from "node:child_process";
import * as NodeFSP from "node:fs/promises";
import * as NodeNet from "node:net";
import * as NodePath from "node:path";
import * as NodeUtil from "node:util";

import {
  associateNativeClaudeFork,
  nativeClaudeForkOwner,
  prepareNativeClaudeFork,
} from "./NativeClaudeForks.ts";

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

export interface NativeClaudeModelActivity {
  readonly prepare: () => Promise<{
    readonly settingsPath: string;
    readonly observerStateDir: string;
    readonly upstreamUrl: string;
  }>;
  readonly bind: (
    sessionId: string,
    prepared: Awaited<ReturnType<NativeClaudeModelActivity["prepare"]>>,
  ) => Promise<void>;
  readonly isBound: (sessionId: string) => Promise<boolean>;
  readonly recoverBound?: (sessionId: string) => Promise<void>;
  readonly hasBinding?: (sessionId: string) => Promise<boolean>;
  readonly setEnabled?: (sessionId: string, enabled: boolean) => Promise<void>;
}

export interface NativeClaudeClientOptions {
  readonly modelActivity?: NativeClaudeModelActivity;
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

  /**
   * Queue human input through the native worker's rendezvous, without terminal keystrokes.
   * Native consumes it at a tool boundary, or as the next prompt if the turn finishes first.
   * The acknowledgement proves delivery only: there is no native receipt or atomic turn/PID guard.
   */
  async enqueueInput(session: NativeClaudeSession, text: string): Promise<void> {
    const short = requireNativeClaudeBackgroundId(session);
    if (
      !session.sessionId ||
      !Number.isSafeInteger(session.pid) ||
      session.pid! <= 0 ||
      session.status !== "busy" ||
      text.trim().length === 0
    )
      throw new NativeClaudeError(
        "EQUEUESTATE",
        "Native Claude queued input requires a busy worker identity and nonempty text.",
      );
    const owners = (await this.listSessions()).filter(
      (candidate) => candidate.sessionId === session.sessionId,
    );
    const current = owners[0];
    if (
      owners.length !== 1 ||
      current?.kind !== "background" ||
      current.id !== short ||
      current.pid !== session.pid ||
      current.startedAt !== session.startedAt ||
      current.status !== "busy" ||
      current.state === "stopped" ||
      current.state === "failed"
    )
      throw new NativeClaudeError(
        "EQUEUESTATE",
        "Claude's active worker changed; no queued input was sent.",
      );
    await this.request({ op: "reply", short, text, nextTurn: true });
  }

  /** Events contain terminal output and daemon state; callers must not treat them as SDK messages. */
  subscribe(session: NativeClaudeSession, signal?: AbortSignal): AsyncGenerator<JsonObject> {
    return this.frames(
      { op: "subscribe", short: requireNativeClaudeBackgroundId(session), tail: 200 },
      false,
      signal,
    );
  }

  /** State-only observation never replays terminal output or acquires terminal ownership. */
  async *subscribeState(
    session: NativeClaudeSession,
    signal?: AbortSignal,
  ): AsyncGenerator<JsonObject> {
    for await (const frame of this.frames(
      { op: "subscribe", short: requireNativeClaudeBackgroundId(session), tail: 0 },
      false,
      signal,
    )) {
      if (frame.type === "snapshot" || frame.type === "state" || frame.type === "settled")
        yield frame;
    }
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

  /** Explicit route changes require stopping an idle, already owned native worker. */
  async setModelActivityEnabled(sessionId: string, enabled: boolean): Promise<NativeClaudeSession> {
    if (!/^[a-f0-9]{8}-(?:[a-f0-9]{4}-){3}[a-f0-9]{12}$/.test(sessionId))
      throw new NativeClaudeError("ESESSION", "A canonical Claude session UUID is required.");
    const activity = this.options.modelActivity;
    if (!activity?.hasBinding || !activity.setEnabled || !(await activity.hasBinding(sessionId)))
      throw new NativeClaudeError(
        "EOBSERVERUNAVAILABLE",
        "This native worker has no owned model activity settings to change.",
      );
    const readOwner = async () => {
      const owners = (await this.listSessions()).filter((value) => value.sessionId === sessionId);
      if (owners.length !== 1 || owners[0]?.kind !== "background")
        throw new NativeClaudeError(
          "EOBSERVERSTATE",
          "Claude ownership changed. No further model activity changes were made.",
        );
      const owner = owners[0];
      requireNativeClaudeBackgroundId(owner);
      if (owner.status === "busy" || owner.status === "waiting")
        throw new NativeClaudeError(
          "EBUSY",
          "Finish or interrupt the native Claude turn before changing model activity.",
        );
      return owner;
    };
    let owner = await readOwner();
    const stopped = (job: NativeClaudeSession) => job.state === "stopped" || job.state === "failed";
    if (!stopped(owner)) {
      if (owner.status !== "idle" || !Number.isSafeInteger(owner.pid) || owner.pid! <= 0)
        throw new NativeClaudeError(
          "EOBSERVERSTATE",
          "Claude must expose an idle worker identity before changing model activity.",
        );
      const current = await readOwner();
      if (
        current.id !== owner.id ||
        current.pid !== owner.pid ||
        current.status !== "idle" ||
        stopped(current)
      )
        throw new NativeClaudeError(
          "EOBSERVERSTATE",
          "Claude worker changed before stop. No stop was sent.",
        );
      if (!(await activity.hasBinding(sessionId)))
        throw new NativeClaudeError(
          "EOBSERVERSTATE",
          "Claude model activity ownership changed before stop. No stop was sent.",
        );
      // Native stop has no atomic idle/PID precondition; callers must avoid concurrent CLI input.
      await this.runCLI(["stop", requireNativeClaudeBackgroundId(current)], current.cwd);
      owner = await readOwner();
      if (owner.id !== current.id || !stopped(owner))
        throw new NativeClaudeError(
          "EOBSERVERSTATE",
          "Claude did not remain stopped. Model activity settings were not changed.",
        );
    }
    if (!(await activity.hasBinding(sessionId)))
      throw new NativeClaudeError(
        "EOBSERVERSTATE",
        "Claude model activity ownership changed. Private settings were not changed.",
      );
    try {
      await activity.setEnabled(sessionId, enabled);
    } catch {
      throw new NativeClaudeError(
        "EOBSERVERSETTINGS",
        "Model activity settings could not be changed. This worker was not restarted; inspect it in Claude before continuing.",
      );
    }
    const current = await readOwner();
    if (current.id !== owner.id || !stopped(current))
      throw new NativeClaudeError(
        "EOBSERVERSTATE",
        "Claude ownership changed after the settings update. No restart was sent.",
      );
    const ready = await this.wakeBackground(current, current.cwd);
    if (ready.sessionId !== sessionId || ready.id !== current.id || ready.kind !== "background")
      throw new NativeClaudeError(
        "ESESSIONCOPY",
        "Claude reopened a different native worker; no prompt was sent.",
      );
    return ready;
  }

  /** A binding ledger alone cannot prove a recreated worker retained its private MCP launcher. */
  async hasMcpConfig(session: NativeClaudeSession, expectedPath: string): Promise<boolean> {
    const short = requireNativeClaudeBackgroundId(session);
    const config = this.options.configDirectory ?? this.options.environment?.CLAUDE_CONFIG_DIR;
    if (!config || !session.sessionId) return false;
    let saved: unknown;
    try {
      saved = JSON.parse(
        await NodeFSP.readFile(NodePath.join(config, "jobs", short, "state.json"), "utf8"),
      );
    } catch (cause) {
      if ((cause as NodeJS.ErrnoException).code === "ENOENT") return false;
      throw cause;
    }
    if (
      !isObject(saved) ||
      saved.sessionId !== session.sessionId ||
      saved.daemonShort !== short ||
      !Array.isArray(saved.respawnFlags) ||
      !saved.respawnFlags.every((flag: unknown) => typeof flag === "string")
    )
      return false;
    const flags = saved.respawnFlags as string[];
    const indices = flags.flatMap((flag, index) =>
      flag === "--mcp-config" || flag.startsWith("--mcp-config=") ? [index] : [],
    );
    return (
      indices.length === 1 &&
      flags[indices[0]!] === "--mcp-config" &&
      flags[indices[0]! + 1] === expectedPath
    );
  }

  /** Launch one daemon-owned native session; never creates a second writer for a live session. */
  async startBackground(input: {
    readonly sessionId?: string;
    readonly cwd: string;
    readonly model?: string;
    readonly permissionMode?: string;
    readonly resume?: boolean;
    readonly observeModelActivity?: boolean;
    readonly mcpConfigPath?: string;
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
    const activity = this.options.modelActivity;
    if (input.observeModelActivity && !activity)
      throw new NativeClaudeError(
        "EOBSERVERUNAVAILABLE",
        "Model activity observation is not configured for this Claude provider.",
      );
    const bound =
      input.observeModelActivity && existing?.sessionId
        ? await activity!.isBound(existing.sessionId)
        : false;
    if (existing) {
      requireNativeClaudeBackgroundId(existing);
      if (input.mcpConfigPath)
        throw new NativeClaudeError(
          "EMCPUPGRADE",
          "MCP cannot be installed into an existing native Claude worker. Its launch was not changed.",
        );
      const stopped = existing.state === "failed" || existing.state === "stopped";
      if (input.observeModelActivity && !bound && !stopped)
        throw new NativeClaudeError(
          "EOBSERVERUPGRADE",
          "This Claude worker is already running without model activity observation. Stop it in Claude before explicitly reopening it with observation.",
        );
      if (!input.observeModelActivity || bound) {
        if (
          activity?.recoverBound &&
          existing.sessionId &&
          (bound || (await activity.isBound(existing.sessionId)))
        )
          await activity.recoverBound(existing.sessionId);
        if (stopped) return this.wakeBackground(existing, input.cwd);
        return this.readyBackground(existing);
      }
    }
    if (
      input.resume &&
      (!input.sessionId || !/^[a-f0-9]{8}-(?:[a-f0-9]{4}-){3}[a-f0-9]{12}$/.test(input.sessionId))
    )
      throw new NativeClaudeError(
        "ESESSION",
        "A canonical full Claude session UUID is required to resume its history.",
      );
    const prepared = input.observeModelActivity ? await activity!.prepare() : undefined;
    const args = [
      "--bg",
      ...(input.mcpConfigPath ? ["--mcp-config", input.mcpConfigPath] : []),
      ...(prepared ? ["--settings", prepared.settingsPath] : []),
      // Saved sessions retain their own options; overrides make the CLI fork a copy.
      ...(input.resume && input.sessionId
        ? ["--resume", input.sessionId]
        : [
            ...(input.model ? ["--model", input.model] : []),
            ...(input.permissionMode ? ["--permission-mode", input.permissionMode] : []),
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
    const ready = await this.readyBackground(created);
    if (prepared && ready.sessionId) await activity!.bind(ready.sessionId, prepared);
    return ready;
  }

  /** Native fork owns a new UUID; unlike resume it must never reclaim the source worker. */
  async forkBackground(input: {
    readonly sessionId: string;
    readonly targetThreadId: string;
    readonly cwd: string;
    readonly observeModelActivity?: boolean;
    readonly mcpConfigPath?: string;
    readonly expectedSourceMcpConfigPath?: string;
  }): Promise<NativeClaudeSession> {
    if (this.options.platform === "win32")
      throw new NativeClaudeError(
        "EPLATFORM",
        "Native Claude forks require a macOS or Linux host.",
      );
    if (!/^[a-f0-9]{8}-(?:[a-f0-9]{4}-){3}[a-f0-9]{12}$/.test(input.sessionId))
      throw new NativeClaudeError(
        "ESESSION",
        "A canonical full Claude session UUID is required to fork.",
      );
    const inventory = await this.listSessions();
    const source = inventory.find((job) => job.sessionId === input.sessionId);
    if (
      source === undefined ||
      source.status !== "idle" ||
      source.state === "stopped" ||
      source.state === "failed"
    )
      throw new NativeClaudeError(
        "EFORKBUSY",
        "Open an idle native Claude worker before forking its complete history.",
      );
    requireNativeClaudeBackgroundId(source);
    const activity = this.options.modelActivity;
    if (input.observeModelActivity && !activity)
      throw new NativeClaudeError(
        "EOBSERVERUNAVAILABLE",
        "Model activity observation is not configured for this Claude provider.",
      );
    const config = this.options.configDirectory ?? this.options.environment?.CLAUDE_CONFIG_DIR;
    if (!config)
      throw new NativeClaudeError(
        "ECONFIG",
        "Claude fork requires an explicit configuration directory.",
      );
    const saved: unknown = JSON.parse(
      await NodeFSP.readFile(NodePath.join(config, "jobs", source.id!, "state.json"), "utf8"),
    );
    if (
      !isObject(saved) ||
      saved.sessionId !== input.sessionId ||
      saved.daemonShort !== source.id ||
      !Array.isArray(saved.respawnFlags) ||
      !saved.respawnFlags.every((flag: unknown) => typeof flag === "string")
    )
      throw new NativeClaudeError(
        "EFORKCONFIG",
        "Claude source launch settings could not be verified; no fork was launched.",
      );
    const flags = saved.respawnFlags as string[];
    const mcpIndices = flags.flatMap((flag, index) =>
      flag === "--mcp-config" || flag.startsWith("--mcp-config=") ? [index] : [],
    );
    if (
      mcpIndices.length > 0 &&
      (mcpIndices.length !== 1 ||
        !input.mcpConfigPath ||
        !input.expectedSourceMcpConfigPath ||
        flags[mcpIndices[0]!] !== "--mcp-config" ||
        flags[mcpIndices[0]! + 1] !== input.expectedSourceMcpConfigPath)
    )
      throw new NativeClaudeError(
        "EFORKCONFIG",
        "Claude source MCP configuration is not an owned T3 launcher. No fork was launched.",
      );
    if (input.expectedSourceMcpConfigPath && mcpIndices.length === 0)
      throw new NativeClaudeError(
        "EFORKCONFIG",
        "Claude source no longer has its verified MCP launcher. No fork was launched.",
      );
    const sourceObserver = (await activity?.hasBinding?.(input.sessionId)) ?? false;
    if (sourceObserver && !input.observeModelActivity)
      throw new NativeClaudeError(
        "EFORKCONFIG",
        "Enable independent model activity for the new fork before forking an observed Claude source.",
      );
    const settingsCount = flags.filter(
      (flag) => flag === "--settings" || flag.startsWith("--settings="),
    ).length;
    if (
      settingsCount > 0 &&
      (settingsCount !== 1 || (!(await nativeClaudeForkOwner(config, flags)) && !sourceObserver))
    )
      throw new NativeClaudeError(
        "EFORKCONFIG",
        "This Claude source uses custom launch settings. Fork it in the native CLI to preserve its configuration.",
      );
    const retainedFlags = flags.filter(
      (flag, index) =>
        flag !== "--settings" &&
        flags[index - 1] !== "--settings" &&
        flag !== "--mcp-config" &&
        flags[index - 1] !== "--mcp-config",
    );
    // Explicit settings belong only to this fork; source model/permission flags remain native launch flags.
    const prepared = input.observeModelActivity ? await activity!.prepare() : undefined;
    const settingsPath = await prepareNativeClaudeFork(
      config,
      { sourceSessionId: input.sessionId, targetThreadId: input.targetThreadId },
      prepared?.settingsPath,
    );
    const output = await this.runCLI(
      [
        "--bg",
        "--resume",
        input.sessionId,
        "--fork-session",
        ...retainedFlags,
        ...(input.mcpConfigPath ? ["--mcp-config", input.mcpConfigPath] : []),
        "--settings",
        settingsPath,
      ],
      input.cwd,
    );
    const short = /backgrounded\s*·\s*([a-f0-9]{8})\b/.exec(output.stdout)?.[1];
    const created =
      short === undefined
        ? undefined
        : (await this.listSessions()).find((job) => job.id === short && job.kind === "background");
    if (
      !created?.sessionId ||
      created.sessionId === input.sessionId ||
      inventory.some((job) => job.id === short || job.sessionId === created.sessionId)
    )
      throw new NativeClaudeError(
        "EFORKIDENTITY",
        "Claude did not register a distinct new native fork. No prompt or stop was sent.",
      );
    requireNativeClaudeBackgroundId(created);
    await associateNativeClaudeFork(config, settingsPath, created.sessionId);
    const ready = await this.readyBackground(created);
    if (
      ready.status !== "idle" ||
      ready.state === "failed" ||
      ready.state === "stopped" ||
      ready.cwd !== input.cwd
    )
      throw new NativeClaudeError(
        "EFORKSETUP",
        "Complete the new fork's setup in claude attach before using it.",
      );
    if (prepared) await activity!.bind(ready.sessionId!, prepared);
    return ready;
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
