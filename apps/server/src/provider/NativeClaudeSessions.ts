// @effect-diagnostics nodeBuiltinImport:off -- read-only native JSONL reader interoperates with the socket client without changing global process environment.
import * as NodeFSP from "node:fs/promises";
import * as NodePath from "node:path";

import type {
  NativeHistoryItem,
  NativeHistoryTurn,
  NativeSession,
} from "@t3tools/provider-core/server/nativeSessions";
import { NativeClaudeClient, type NativeClaudeSession } from "./NativeClaudeClient.ts";

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
  const messages = entries.filter(
    (entry) =>
      (entry.type === "user" || entry.type === "assistant") &&
      typeof entry.uuid === "string" &&
      entry.isSidechain !== true,
  );
  const byId = new Map(
    entries
      .filter((entry) => typeof entry.uuid === "string")
      .map((entry) => [entry.uuid as string, entry]),
  );
  let cursor = messages.at(-1);
  const selected: Entry[] = [];
  const seen = new Set<string>();
  while (cursor && typeof cursor.uuid === "string" && !seen.has(cursor.uuid)) {
    seen.add(cursor.uuid);
    if ((cursor.type === "user" || cursor.type === "assistant") && cursor.isSidechain !== true)
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
  for (const entry of conversationBranch(entries)) {
    if (!object(entry.message)) continue;
    const message = entry.message;
    const content =
      typeof message.content === "string"
        ? [{ type: "text", text: message.content }]
        : Array.isArray(message.content)
          ? message.content.filter(object)
          : [];
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
    for (const [index, block] of content.entries()) {
      const id = `${String(entry.uuid)}:${index}`;
      if (block.type === "tool_result") {
        const target =
          typeof block.tool_use_id === "string" ? toolItems.get(block.tool_use_id) : undefined;
        if (target) {
          const original = target.turn.items[target.index]!;
          target.turn.items[target.index] = {
            ...original,
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
          text: JSON.stringify(block.input ?? {}),
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
      message.stop_reason !== "tool_use" &&
      message.stop_reason !== null
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
    // CLI inventory failure must be surfaced: otherwise a live writer could appear safely idle.
    const live = await this.client.listSessions();
    this.lastInventory = live;
    const activeById = new Map(
      live.filter((session) => session.sessionId).map((session) => [session.sessionId!, session]),
    );
    const sessions = new Map<string, NativeSession>();
    for (const file of await this.files()) {
      const { entries, metadata } = await this.transcript(file);
      const first = entries.find(
        (entry) => typeof entry.cwd === "string" && typeof entry.sessionId === "string",
      );
      if (!first || !UUID.test(String(first.sessionId))) continue;
      const id = String(first.sessionId);
      const last = entries.at(-1);
      const renamed = entries.findLast((entry) => typeof entry.customTitle === "string");
      const history = nativeClaudeHistory(entries);
      const title =
        typeof renamed?.customTitle === "string"
          ? renamed.customTitle
          : (history
              .flatMap((turn) => turn.items)
              .find((item) => item.type === "user")
              ?.text.slice(0, 140) ?? "Claude session");
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
        attachable:
          activeById.get(id)?.kind === "background" && activeById.get(id)?.id !== undefined,
        active: (() => {
          const live = activeById.get(id);
          return (
            live !== undefined &&
            (live.pid !== undefined || live.state === "working" || live.state === "blocked")
          );
        })(),
      });
    }
    // The daemon inventory is host-wide, not proven account-scoped. Only expose
    // sessions whose transcript exists under this provider's configuration directory.
    return [...sessions.values()];
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
      if (history.length === 0 || !live) return history;
      return history.map((turn, index) => {
        if (index !== history.length - 1) return turn;
        if (live.status === "busy" || live.state === "blocked" || live.status === "waiting")
          return { ...turn, status: "running" as const, completedAt: null };
        if (live.state === "failed") return { ...turn, status: "failed" as const };
        if (live.state === "stopped") return { ...turn, status: "interrupted" as const };
        return turn;
      });
    }
    throw new Error("Claude session transcript was not found.");
  }
}
