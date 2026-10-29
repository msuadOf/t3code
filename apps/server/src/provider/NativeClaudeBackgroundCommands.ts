import type { NativeHistoryTurn } from "@t3tools/provider-core/server/nativeSessions";

type Entry = Readonly<Record<string, unknown>>;
const object = (v: unknown): v is Entry => typeof v === "object" && v !== null && !Array.isArray(v);
const uuid = /^[a-f0-9]{8}-(?:[a-f0-9]{4}-){3}[a-f0-9]{12}$/i;
export interface NativeClaudeBackgroundCommand {
  readonly turnId: string;
  readonly toolId: string;
  readonly taskId: string;
  readonly outputPath: string;
  readonly command: string;
  readonly startedAt: number;
  readonly status: "running" | "completed" | "failed" | "cancelled";
  readonly completedAt?: number;
}

/** Native receipt ownership, rather than arbitrary model/user text, authorizes observation. */
export function nativeClaudeBackgroundCommands(
  sessionId: string,
  history: readonly NativeHistoryTurn[],
  entries: readonly Entry[],
): readonly NativeClaudeBackgroundCommand[] {
  if (!uuid.test(sessionId)) return [];
  const calls = new Map<string, Entry | null>();
  const receipts = new Map<string, Array<{ taskId: string; outputPath: string }>>();
  const notifications = new Map<
    string,
    { status: NativeClaudeBackgroundCommand["status"]; completedAt?: number } | null
  >();
  const key = (toolId: string, taskId: string, outputPath: string) =>
    JSON.stringify([toolId, taskId, outputPath]);
  for (const entry of entries) {
    if (entry.sessionId !== sessionId || !object(entry.message)) continue;
    const content = entry.message.content;
    if (entry.type === "assistant" && Array.isArray(content)) {
      for (const block of content)
        if (
          object(block) &&
          block.type === "tool_use" &&
          block.name === "Bash" &&
          typeof block.id === "string"
        )
          calls.set(block.id, calls.has(block.id) ? null : entry);
    }
    if (
      entry.type === "user" &&
      object(entry.toolUseResult) &&
      typeof entry.toolUseResult.backgroundTaskId === "string" &&
      Array.isArray(content)
    ) {
      const taskId = entry.toolUseResult.backgroundTaskId;
      const results = content.filter(
        (block: unknown) => object(block) && block.type === "tool_result",
      );
      const block: unknown = results[0];
      if (
        results.length === 1 &&
        object(block) &&
        typeof block.tool_use_id === "string" &&
        block.is_error !== true &&
        typeof block.content === "string" &&
        /^[a-zA-Z0-9_-]{1,128}$/.test(taskId)
      ) {
        const outputPath = /Output is being written to: ([^\r\n]+?\.output)(?:\.|\s|$)/.exec(
          block.content,
        )?.[1];
        if (
          outputPath?.startsWith("/") &&
          outputPath.endsWith(`/${sessionId}/tasks/${taskId}.output`)
        ) {
          const list = receipts.get(block.tool_use_id) ?? [];
          list.push({ taskId, outputPath });
          receipts.set(block.tool_use_id, list);
        }
      }
    }
    if (
      entry.type !== "user" ||
      entry.turnOrigin !== "task_notification" ||
      entry.promptSource !== "system" ||
      !object(entry.origin) ||
      entry.origin.kind !== "task-notification" ||
      entry.origin.producer !== "session-task" ||
      typeof content !== "string" ||
      !content.trim().startsWith("<task-notification>") ||
      !content.trim().endsWith("</task-notification>")
    )
      continue;
    const tag = (name: string) => {
      const matches = [...content.matchAll(new RegExp(`<${name}>([^<]*)</${name}>`, "g"))];
      return matches.length === 1 ? matches[0]?.[1] : undefined;
    };
    const toolId = tag("tool-use-id"),
      taskId = tag("task-id"),
      outputPath = tag("output-file"),
      status = tag("status");
    if (
      !toolId ||
      !taskId ||
      !outputPath ||
      (status !== "completed" && status !== "failed" && status !== "killed" && status !== "stopped")
    )
      continue;
    const mappedStatus = status === "killed" || status === "stopped" ? "cancelled" : status;
    const at = typeof entry.timestamp === "string" ? Date.parse(entry.timestamp) : NaN;
    const id = key(toolId, taskId, outputPath),
      previous = notifications.get(id);
    if (previous === null || (previous && previous.status !== mappedStatus))
      notifications.set(id, null);
    else if (!previous)
      notifications.set(id, {
        status: mappedStatus,
        ...(Number.isFinite(at) ? { completedAt: at } : {}),
      });
  }
  const result: NativeClaudeBackgroundCommand[] = [];
  for (const turn of history)
    for (const item of turn.items) {
      if (item.type !== "tool" || item.title !== "Bash" || typeof item.input?.command !== "string")
        continue;
      const call = calls.get(item.id),
        matches = receipts.get(item.id);
      if (!call || matches?.length !== 1) continue;
      const receipt = matches[0]!;
      const timestamp = typeof call.timestamp === "string" ? Date.parse(call.timestamp) : NaN;
      const terminal = notifications.get(key(item.id, receipt.taskId, receipt.outputPath));
      result.push({
        turnId: turn.id,
        toolId: item.id,
        ...receipt,
        command: item.input.command,
        startedAt: Number.isFinite(timestamp) ? timestamp : turn.startedAt,
        status: "running",
        ...terminal,
      });
    }
  return result;
}
