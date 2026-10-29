import {
  nativeClaudeBackgroundCommands,
  type NativeClaudeBackgroundCommand,
} from "./NativeClaudeBackgroundCommands.ts";
import { nativeClaudeForkOwner } from "./NativeClaudeForks.ts";
// @effect-diagnostics nodeBuiltinImport:off -- read-only native JSONL reader interoperates with the socket client without changing global process environment.
import * as NodeFSP from "node:fs/promises";
import * as NodePath from "node:path";

import type {
  NativeHistoryItem,
  NativeHistoryTurn,
  NativeSession,
  NativeSessionCandidate,
} from "@t3tools/provider-core/server/nativeSessions";
import {
  NativeClaudeClient,
  NativeClaudeError,
  type NativeClaudeSession,
} from "./NativeClaudeClient.ts";

import {
  isNativeClaudeTaskNotification,
  nativeClaudeSubagentLaunches,
  observeNativeClaudeSubagent,
  type NativeClaudeSubagentObservation,
} from "./NativeClaudeSubagents.ts";

function nativeSessionTitle(session: NativeClaudeSession | undefined): string | undefined {
  const name = session?.name?.trim();
  if (
    !name ||
    name.toLowerCase() === session?.id?.toLowerCase() ||
    name.toLowerCase() === session?.sessionId?.toLowerCase()
  )
    return undefined;
  return name;
}

type Entry = Record<string, unknown>;
const object = (value: unknown): value is Entry =>
  value !== null && typeof value === "object" && !Array.isArray(value);
const UUID = /^[a-f0-9]{8}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{12}$/i;

/** An active writer can leave a partial final line. Other malformed lines are not silently skipped. */
export function decodeNativeClaudeTranscript(text: string): readonly Entry[] {
  const lines = text.split("\n");
  const entries: Entry[] = [];
  for (let index = 0; index < lines.length; index++) {
    const line = lines[index]!;
    if (!line.trim()) continue;
    let value: unknown;
    try {
      value = JSON.parse(line);
    } catch (error) {
      if (index === lines.length - 1) break;
      throw error;
    }
    if (object(value)) entries.push(value);
  }
  return entries;
}

function timestamp(value: unknown, fallback: number): number {
  const parsed =
    typeof value === "string" ? Date.parse(value) : typeof value === "number" ? value : Number.NaN;
  return Number.isFinite(parsed) ? parsed : fallback;
}

/** Follow the last persisted conversation leaf so abandoned /rewind branches are not replayed. */
function conversationBranch(entries: readonly Entry[]): readonly Entry[] {
  const nodes = entries.filter(
    (entry) => typeof entry.uuid === "string" && entry.isSidechain !== true,
  );
  const parents = new Set(nodes.map((entry) => entry.parentUuid));
  const byId = new Map(
    entries
      .filter((entry) => typeof entry.uuid === "string")
      .map((entry) => [entry.uuid as string, entry]),
  );
  // Native persistence can flush an assistant before its buffered user/attachment ancestors.
  let cursor = nodes.findLast((entry) => !parents.has(entry.uuid));
  const selected: Entry[] = [];
  const seen = new Set<string>();
  while (cursor && typeof cursor.uuid === "string" && !seen.has(cursor.uuid)) {
    seen.add(cursor.uuid);
    if (
      (cursor.type === "user" || cursor.type === "assistant" || cursor.type === "attachment") &&
      cursor.isSidechain !== true
    )
      selected.push(cursor);
    cursor = typeof cursor.parentUuid === "string" ? byId.get(cursor.parentUuid) : undefined;
  }
  return selected.toReversed();
}

export function nativeClaudeHistory(entries: readonly Entry[]): readonly NativeHistoryTurn[] {
  const turns: Array<{
    id: string;
    status: NativeHistoryTurn["status"];
    startedAt: number;
    completedAt: number | null;
    items: NativeHistoryItem[];
  }> = [];
  const toolItems = new Map<string, { turn: (typeof turns)[number]; index: number }>();
  const assistantTurns = new Map<string, (typeof turns)[number]>();
  const promptTurns = new Map<string, (typeof turns)[number]>();
  const deliveredInputs = new Set<string>();
  for (const entry of conversationBranch(entries)) {
    // Native child reports belong to the original work, not to a new human prompt.
    if (isNativeClaudeTaskNotification(entry)) continue;
    const attachment = entry.attachment;
    // Absorbed input stays inside the current native turn. Its explicit delivery
    // record proves consumption, unlike a queue ACK or an unsaved prompt snapshot.
    if (
      entry.type === "attachment" &&
      object(attachment) &&
      attachment.type === "queued_command" &&
      attachment.commandMode === "prompt" &&
      attachment.humanTurn === true &&
      object(attachment.origin) &&
      attachment.origin.kind === "human" &&
      typeof attachment.delivery_id === "string" &&
      attachment.delivery_id.length > 0 &&
      typeof attachment.prompt === "string" &&
      attachment.prompt.trim().length > 0
    ) {
      const turn = turns.at(-1);
      if (turn && !deliveredInputs.has(attachment.delivery_id)) {
        deliveredInputs.add(attachment.delivery_id);
        turn.items.push({
          id: `queued-input:${attachment.delivery_id}`,
          type: "user",
          inputIntent: "steer",
          title: "Additional CLI input",
          text: attachment.prompt,
          status: "completed",
        });
      }
      continue;
    }
    if (!object(entry.message)) continue;
    const message = entry.message;
    const content =
      typeof message.content === "string"
        ? [{ type: "text", text: message.content }]
        : Array.isArray(message.content)
          ? message.content.filter(object)
          : [];
    // The native CLI writes cancellation as a user-shaped record tied to an assistant message.
    const nativeCancel =
      entry.type === "user" &&
      (typeof entry.interruptedMessageId === "string" ||
        (typeof entry.promptId === "string" &&
          promptTurns.has(entry.promptId) &&
          entry.promptSource === undefined &&
          entry.origin === undefined &&
          content.length === 1 &&
          content[0]?.type === "text" &&
          (content[0].text === "[Request interrupted by user]" ||
            content[0].text === "[Request interrupted by user for tool use]")));
    if (nativeCancel) {
      const interrupted =
        (typeof entry.interruptedMessageId === "string"
          ? assistantTurns.get(entry.interruptedMessageId)
          : undefined) ??
        (typeof entry.promptId === "string" ? promptTurns.get(entry.promptId) : undefined) ??
        turns.at(-1);
      if (interrupted) {
        interrupted.status = "interrupted";
        interrupted.completedAt = timestamp(entry.timestamp, interrupted.startedAt);
        interrupted.items = interrupted.items.map((item) =>
          item.status === "running" ? { ...item, status: "cancelled" } : item,
        );
      }
      continue;
    }
    const isPrompt = entry.type === "user" && content.some((block) => block.type !== "tool_result");
    if (isPrompt || turns.length === 0)
      turns.push({
        id: String(entry.uuid),
        status: "completed",
        startedAt: timestamp(entry.timestamp, 0),
        completedAt: null,
        items: [],
      });
    const turn = turns.at(-1)!;
    if (isPrompt && typeof entry.promptId === "string") promptTurns.set(entry.promptId, turn);
    if (entry.type === "assistant" && typeof message.id === "string")
      assistantTurns.set(message.id, turn);
    for (const [index, block] of content.entries()) {
      const id = `${String(entry.uuid)}:${index}`;
      if (block.type === "tool_result") {
        const target =
          typeof block.tool_use_id === "string" ? toolItems.get(block.tool_use_id) : undefined;
        if (target) {
          const original = target.turn.items[target.index]!;
          target.turn.items[target.index] = {
            ...original,
            ...(object(entry.toolUseResult) ? { result: entry.toolUseResult } : {}),
            status: block.is_error === true ? "failed" : "completed",
            text:
              typeof block.content === "string"
                ? block.content
                : JSON.stringify(block.content ?? ""),
          };
        }
      } else if (block.type === "tool_use") {
        const tool = {
          id: typeof block.id === "string" ? block.id : id,
          type: "tool" as const,
          status: "running" as const,
          title: typeof block.name === "string" ? block.name : "Tool",
          input: object(block.input) ? block.input : {},
          text: "",
        };
        toolItems.set(tool.id, { turn, index: turn.items.length });
        turn.items.push(tool);
      } else if (block.type === "text" && typeof block.text === "string") {
        turn.items.push({
          id,
          type: entry.type === "user" ? "user" : "assistant",
          text: block.text,
          status: "completed",
        });
      } else if (block.type === "thinking" && typeof block.thinking === "string") {
        turn.items.push({ id, type: "reasoning", text: block.thinking, status: "completed" });
      }
    }
    if (
      entry.type === "assistant" &&
      typeof message.stop_reason === "string" &&
      message.stop_reason !== "tool_use"
    )
      turn.completedAt = timestamp(entry.timestamp, turn.startedAt);
  }
  return turns.map((turn, index) =>
    turn.completedAt !== null
      ? turn
      : index < turns.length - 1
        ? { ...turn, status: "interrupted" as const, completedAt: turns[index + 1]!.startedAt }
        : { ...turn, status: "running" as const },
  );
}

export class NativeClaudeSessions {
  private readonly configDirectory: string;
  private readonly client: NativeClaudeClient;
  private lastInventory: readonly NativeClaudeSession[] | undefined;
  private readonly commandCache = new WeakMap<
    readonly Entry[],
    readonly NativeClaudeBackgroundCommand[]
  >();
  private readonly cache = new Map<
    string,
    {
      mtime: number;
      size: number;
      entries: readonly Entry[];
      metadata: { readonly birthtimeMs: number; readonly mtimeMs: number };
    }
  >();

  private async transcript(file: string) {
    const metadata = await NodeFSP.stat(file);
    const cached = this.cache.get(file);
    if (cached && cached.mtime === metadata.mtimeMs && cached.size === metadata.size) return cached;
    const parsed = {
      mtime: metadata.mtimeMs,
      size: metadata.size,
      entries: decodeNativeClaudeTranscript(await NodeFSP.readFile(file, "utf8")),
      metadata,
    };
    // Browsing ordinary history must not retain every transcript in the daemon.
    if (this.cache.size >= 32 && !this.cache.has(file))
      this.cache.delete(this.cache.keys().next().value!);
    this.cache.set(file, parsed);
    return parsed;
  }
  constructor(configDirectory: string, client: NativeClaudeClient) {
    this.configDirectory = configDirectory;
    this.client = client;
  }

  private async files(): Promise<readonly string[]> {
    const projects = NodePath.join(this.configDirectory, "projects");
    const result: string[] = [];
    let directories;
    try {
      directories = await NodeFSP.readdir(projects, { withFileTypes: true });
    } catch (error) {
      if (object(error) && error.code === "ENOENT") return [];
      throw error;
    }
    for (const directory of directories) {
      if (!directory.isDirectory()) continue;
      const root = NodePath.join(projects, directory.name);
      for (const file of await NodeFSP.readdir(root, { withFileTypes: true })) {
        if (file.isFile() && file.name.endsWith(".jsonl") && UUID.test(file.name.slice(0, -6)))
          result.push(NodePath.join(root, file.name));
      }
    }
    return result;
  }

  async list(): Promise<readonly NativeSession[]> {
    return (await this.available(true)).filter((session) => session.sharingState === "shared");
  }

  async open(
    sessionId: string,
    options?: { readonly observeModelActivity?: boolean; readonly mcpConfigPath?: string },
  ): Promise<NativeSession> {
    const session = (await this.available()).find((session) => session.id === sessionId);
    if (!session)
      throw new NativeClaudeError("ENOSESSION", "Claude history is no longer available.");
    await this.client.startBackground({ sessionId, cwd: session.cwd, resume: true, ...options });
    const shared = (await this.list()).find((session) => session.id === sessionId);
    if (!shared) throw new NativeClaudeError("ENOJOB", "Claude did not expose the shared session.");
    return shared;
  }

  async setModelActivityEnabled(sessionId: string, enabled: boolean): Promise<NativeSession> {
    const candidate = (await this.available()).find((session) => session.id === sessionId);
    if (!candidate || candidate.sharingState !== "shared" || candidate.active)
      throw new NativeClaudeError(
        "EOBSERVERSTATE",
        "Model activity can only be changed for an idle shared Claude worker.",
      );
    await this.client.setModelActivityEnabled(sessionId, enabled);
    const shared = (await this.list()).find((session) => session.id === sessionId);
    if (!shared)
      throw new NativeClaudeError(
        "ENOJOB",
        "Claude did not expose the original shared session after restarting.",
      );
    return shared;
  }

  async available(onlyShared = false): Promise<readonly NativeSessionCandidate[]> {
    // CLI inventory failure must be surfaced: otherwise a live writer could appear safely idle.
    const live = await this.client.listSessions();
    this.lastInventory = live;
    const activeById = new Map(
      live
        .filter((session) => session.kind === "background" && session.sessionId)
        .map((session) => [session.sessionId!, session]),
    );
    const sessions = new Map<string, NativeSessionCandidate>();
    for (const file of await this.files()) {
      const fileId = NodePath.basename(file, ".jsonl");
      if (onlyShared && !activeById.has(fileId)) continue;
      const { entries, metadata } = await this.transcript(file);
      const first = entries.find(
        (entry) => typeof entry.cwd === "string" && typeof entry.sessionId === "string",
      );
      if (!first || !UUID.test(String(first.sessionId))) continue;
      const id = String(first.sessionId);
      if (id !== fileId) continue;
      const last = entries.at(-1);
      const renamed = entries.findLast((entry) => typeof entry.customTitle === "string");
      const history = nativeClaudeHistory(entries);
      const title =
        nativeSessionTitle(activeById.get(id)) ??
        (typeof renamed?.customTitle === "string"
          ? renamed.customTitle
          : (history
              .flatMap((turn) => turn.items)
              .find((item) => item.type === "user")
              ?.text.slice(0, 140) ?? "Claude session"));
      const assistant = entries.findLast(
        (entry) => object(entry.message) && typeof entry.message.model === "string",
      );
      const model =
        assistant && object(assistant.message) && typeof assistant.message.model === "string"
          ? assistant.message.model
          : undefined;
      sessions.set(id, {
        id,
        ...(model === undefined ? {} : { model }),
        cwd: String(first.cwd),
        title,
        createdAt: timestamp(first.timestamp, metadata.birthtimeMs),
        updatedAt: timestamp(last?.timestamp, metadata.mtimeMs),
        archived: false,
        historyAvailable: true,
        sharingState: live.some(
          (session) => session.sessionId === id && session.kind === "interactive",
        )
          ? "foreground"
          : activeById.has(id)
            ? "shared"
            : "history",
        canObserveActive: activeById.has(id),
        ...(activeById.has(id) &&
        this.parsedBackgroundCommands(id, entries, history).some(
          (command) => command.status === "running",
        )
          ? { hasPendingBackgroundWork: true }
          : {}),
        active: (() => {
          const live = activeById.get(id);
          return live !== undefined && (live.status === "busy" || live.status === "waiting");
        })(),
      });
    }
    // A freshly backgrounded foreground conversation may not flush its new
    // transcript until its next prompt. Its local job record proves config ownership.
    for (const [id, job] of activeById) {
      if (sessions.has(id) || !UUID.test(id) || !job.id || !/^[a-f0-9]{8}$/.test(job.id)) continue;
      const state = await this.localJob(job);
      if (!state) continue;
      sessions.set(id, {
        id,
        cwd: job.cwd,
        title:
          nativeSessionTitle(job) ??
          (typeof state.intent === "string" ? state.intent : "Claude session"),
        createdAt: timestamp(state.createdAt, job.startedAt),
        updatedAt: timestamp(state.updatedAt, job.startedAt),
        archived: false,
        active: job.status === "busy" || job.status === "waiting",
        canObserveActive: true,
        historyAvailable: false,
        sharingState: "shared",
      });
    }
    for (const [id, candidate] of sessions) {
      const job = activeById.get(id);
      const state = job === undefined ? undefined : await this.localJob(job);
      if (state === undefined && onlyShared) {
        sessions.delete(id);
        continue;
      }
      const forkOwner = await nativeClaudeForkOwner(this.configDirectory, state?.respawnFlags, id);
      if (forkOwner)
        sessions.set(id, {
          ...candidate,
          requiresExistingOwner: { threadId: forkOwner.targetThreadId },
        });
    }
    return [...sessions.values()];
  }

  private async localJob(job: NativeClaudeSession): Promise<Entry | undefined> {
    if (!job.id || !/^[a-f0-9]{8}$/.test(job.id)) return undefined;
    let state: unknown;
    try {
      state = JSON.parse(
        await NodeFSP.readFile(
          NodePath.join(this.configDirectory, "jobs", job.id, "state.json"),
          "utf8",
        ),
      );
    } catch (error) {
      if (object(error) && error.code === "ENOENT") return undefined;
      throw error;
    }
    return object(state) &&
      state.sessionId === job.sessionId &&
      state.daemonShort === job.id &&
      state.cwd === job.cwd
      ? state
      : undefined;
  }

  async confirmedModelMessageIds(sessionId: string): Promise<readonly string[]> {
    if (!UUID.test(sessionId)) throw new Error("Invalid Claude session ID.");
    for (const file of await this.files()) {
      if (NodePath.basename(file, ".jsonl").toLowerCase() !== sessionId.toLowerCase()) continue;
      const entries = (await this.transcript(file)).entries;
      const ids = new Set<string>();
      // Only persisted model IDs establish provenance; transcript entry UUIDs do not.
      for (let index = entries.length - 1; index >= 0 && ids.size < 256; index--) {
        const entry = entries[index]!;
        if (entry.type !== "assistant" || entry.sessionId !== sessionId || !object(entry.message))
          continue;
        if (typeof entry.message.id === "string" && entry.message.id.length > 0)
          ids.add(entry.message.id);
      }
      return [...ids];
    }
    const job = (this.lastInventory ?? (await this.client.listSessions())).find(
      (session) => session.kind === "background" && session.sessionId === sessionId,
    );
    if (job && (await this.localJob(job))) return [];
    throw new Error("Claude session transcript was not found.");
  }

  private parsedBackgroundCommands(
    sessionId: string,
    source: readonly Entry[],
    history?: readonly NativeHistoryTurn[],
  ) {
    const cached = this.commandCache.get(source);
    if (cached) return cached;
    const commands = nativeClaudeBackgroundCommands(
      sessionId,
      history ?? nativeClaudeHistory(source),
      conversationBranch(source),
    );
    this.commandCache.set(source, commands);
    return commands;
  }

  async backgroundCommands(sessionId: string) {
    if (!UUID.test(sessionId)) throw new Error("Invalid Claude session ID.");
    for (const file of await this.files()) {
      if (NodePath.basename(file, ".jsonl") !== sessionId) continue;
      return this.parsedBackgroundCommands(sessionId, (await this.transcript(file)).entries);
    }
    return [];
  }

  /** Read only sidecars named by a canonical parent Agent/Task receipt, never its outputFile path. */
  async subagents(sessionId: string): Promise<readonly NativeClaudeSubagentObservation[]> {
    if (!UUID.test(sessionId)) throw new Error("Invalid Claude session ID.");
    for (const file of await this.files()) {
      if (NodePath.basename(file, ".jsonl") !== sessionId) continue;
      const entries = (await this.transcript(file)).entries;
      const launches = nativeClaudeSubagentLaunches(
        nativeClaudeHistory(entries),
        conversationBranch(entries),
      );
      return await Promise.all(
        launches.map(async (launch) => {
          const base = NodePath.join(
            NodePath.dirname(file),
            sessionId,
            "subagents",
            `agent-${launch.agentId}`,
          );
          let sidecar: { metadata: unknown; entries: readonly Entry[] } | undefined;
          try {
            const [metadata, transcript] = await Promise.all([
              NodeFSP.readFile(`${base}.meta.json`, "utf8").then(
                (text) => JSON.parse(text) as unknown,
              ),
              this.transcript(`${base}.jsonl`),
            ]);
            sidecar = { metadata, entries: transcript.entries };
          } catch (cause) {
            if (!object(cause) || cause.code !== "ENOENT") throw cause;
          }
          return observeNativeClaudeSubagent(
            sessionId,
            launch,
            conversationBranch(entries),
            sidecar,
          );
        }),
      );
    }
    return [];
  }

  async history(
    sessionId: string,
    inventory?: readonly NativeClaudeSession[],
  ): Promise<readonly NativeHistoryTurn[]> {
    if (!UUID.test(sessionId)) throw new Error("Invalid Claude session ID.");
    for (const file of await this.files()) {
      if (!file.endsWith(`/${sessionId}.jsonl`) && !file.endsWith(`\\${sessionId}.jsonl`)) continue;
      const history = nativeClaudeHistory((await this.transcript(file)).entries);
      const live = (inventory ?? this.lastInventory ?? (await this.client.listSessions())).find(
        (session) => session.sessionId === sessionId,
      );
      if (history.length === 0) return history;
      return history.map((turn, index) => {
        if (index !== history.length - 1) return turn;
        if (!live)
          return turn.status === "running" ? { ...turn, status: "interrupted" as const } : turn;
        if (live.status === "busy" || live.status === "waiting")
          return { ...turn, status: "running" as const, completedAt: null };
        if (live.state === "failed") return { ...turn, status: "failed" as const };
        if (live.state === "stopped") return { ...turn, status: "interrupted" as const };
        return turn;
      });
    }
    const job = (inventory ?? this.lastInventory ?? (await this.client.listSessions())).find(
      (session) => session.kind === "background" && session.sessionId === sessionId,
    );
    if (job && (await this.localJob(job))) return [];
    throw new Error("Claude session transcript was not found.");
  }
}
