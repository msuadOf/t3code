import { nativeClaudeTaskNotificationContents } from "./NativeClaudeTaskNotifications.ts";
import type { NativeHistoryTurn } from "@t3tools/provider-core/server/nativeSessions";

type RecordValue = Readonly<Record<string, unknown>>;
const object = (value: unknown): value is RecordValue =>
  typeof value === "object" && value !== null && !Array.isArray(value);
const string = (value: unknown): string | undefined =>
  typeof value === "string" && value.length > 0 ? value : undefined;
const nativeClaudeAgentId = (value: unknown): value is string =>
  typeof value === "string" && /^[a-f0-9]{6,64}$/.test(value);

export interface NativeClaudeSubagentLaunch {
  readonly turnId: string;
  readonly toolId: string;
  readonly agentId: string;
  readonly prompt: string;
  readonly title: string;
  readonly model?: string;
  readonly startedAt: number;
  readonly synchronous: boolean;
}
export interface NativeClaudeSubagentObservation extends NativeClaudeSubagentLaunch {
  readonly status: "running" | "completed" | "failed" | "cancelled" | "unknown";
  readonly result?: string;
  readonly progress?: string;
  readonly completedAt?: number;
}

/** The parent tool receipt identifies the child; async_launched is not a child completion. */
export function nativeClaudeSubagentLaunches(
  history: readonly NativeHistoryTurn[],
  entries: readonly RecordValue[] = [],
): readonly NativeClaudeSubagentLaunch[] {
  return history.flatMap((turn) =>
    turn.items.flatMap((item) => {
      const result = item.result;
      if (
        item.type !== "tool" ||
        (item.title !== "Agent" && item.title !== "Task") ||
        item.status !== "completed" ||
        result === undefined ||
        !nativeClaudeAgentId(result.agentId) ||
        (result.status !== "completed" && result.status !== "async_launched")
      )
        return [];
      const prompt = string(result.prompt) ?? string(item.input?.prompt);
      if (prompt === undefined) return [];
      const model = string(result.resolvedModel);
      const call = entries.find(
        (entry) =>
          entry.type === "assistant" &&
          object(entry.message) &&
          Array.isArray(entry.message.content) &&
          entry.message.content.some(
            (block: unknown) => object(block) && block.type === "tool_use" && block.id === item.id,
          ),
      );
      const at = typeof call?.timestamp === "string" ? Date.parse(call.timestamp) : Number.NaN;
      return [
        {
          turnId: turn.id,
          toolId: item.id,
          agentId: result.agentId,
          prompt,
          title: string(result.description) ?? string(item.input?.description) ?? "Claude subagent",
          ...(model === undefined ? {} : { model }),
          startedAt: Number.isFinite(at) ? at : turn.startedAt,
          synchronous: result.status === "completed",
        },
      ];
    }),
  );
}

/** A user pasting the same XML has none of these native provenance fields. */
export function isNativeClaudeTaskNotification(entry: Readonly<Record<string, unknown>>): boolean {
  return (
    entry.type === "user" &&
    object(entry.origin) &&
    entry.origin.kind === "task-notification" &&
    entry.origin.producer === "session-task"
  );
}

/** The parent receipt and native metadata must both identify this exact sidechain. */
export function nativeClaudeSubagentEntries(
  sessionId: string,
  launch: NativeClaudeSubagentLaunch,
  sidecar: { readonly metadata: unknown; readonly entries: readonly RecordValue[] } | undefined,
): readonly RecordValue[] {
  if (!sidecar || !object(sidecar.metadata) || sidecar.metadata.toolUseId !== launch.toolId)
    return [];
  return sidecar.entries.filter(
    (entry) =>
      entry.agentId === launch.agentId &&
      entry.sessionId === sessionId &&
      entry.isSidechain === true &&
      typeof entry.timestamp === "string" &&
      Date.parse(entry.timestamp) >= launch.startedAt,
  );
}

/** Only the CLI's typed machine envelope can authenticate these protocol tags as lifecycle data. */
export function observeNativeClaudeSubagent(
  sessionId: string,
  launch: NativeClaudeSubagentLaunch,
  notifications: readonly RecordValue[],
  sidecar?: { readonly metadata: unknown; readonly entries: readonly RecordValue[] },
): NativeClaudeSubagentObservation {
  const metadata = sidecar?.metadata;
  const linked = object(metadata) && metadata.toolUseId === launch.toolId;
  const records = nativeClaudeSubagentEntries(sessionId, launch, sidecar);
  const last = records.findLast((entry) => entry.type === "assistant" && object(entry.message));
  const message = last !== undefined && object(last.message) ? last.message : undefined;
  const model = string(message?.model);
  const progress = Array.isArray(message?.content)
    ? message.content
        .filter(object)
        .flatMap((block) =>
          block.type === "text" && typeof block.text === "string" ? [block.text] : [],
        )
        .join("\n")
    : undefined;
  const native = {
    ...launch,
    ...(model === undefined || model === "<synthetic>" ? {} : { model }),
    ...(progress ? { progress } : {}),
  };
  for (const entry of notifications.toReversed()) {
    if (entry.sessionId !== sessionId) continue;
    for (const content of nativeClaudeTaskNotificationContents(entry).toReversed()) {
      const at = typeof entry.timestamp === "string" ? Date.parse(entry.timestamp) : Number.NaN;
      if (!Number.isFinite(at) || at < launch.startedAt) continue;
      // All identity/status fields precede the model-authored result, which is never parsed for status.
      const match =
        /^<task-notification>\s*<task-id>([^<]+)<\/task-id>\s*<tool-use-id>([^<]+)<\/tool-use-id>\s*(?:<output-file>[^<]*<\/output-file>\s*)?<status>(completed|failed|stopped)<\/status>/.exec(
          content,
        );
      if (!match || match[1] !== launch.agentId || match[2] !== launch.toolId) continue;
      const result =
        /<result>([\s\S]*)<\/result>\s*(?:<usage>[\s\S]*<\/usage>\s*)?<\/task-notification>\s*$/.exec(
          content,
        )?.[1];
      return {
        ...native,
        status: match[3] === "stopped" ? "cancelled" : (match[3] as "completed" | "failed"),
        completedAt: at,
        ...(result === undefined ? {} : { result }),
      };
    }
  }
  // finished records only an end, not its outcome. Missing/partial sidecars do not prove termination.
  if (linked && metadata.finished === true) {
    const at = typeof last?.timestamp === "string" ? Date.parse(last.timestamp) : Number.NaN;
    if (last?.isApiErrorMessage === true && Number.isFinite(at))
      return { ...native, status: "failed", completedAt: at };
    if (message?.stop_reason === "end_turn" && Number.isFinite(at))
      return {
        ...native,
        status: "completed",
        completedAt: at,
        ...(progress ? { result: progress } : {}),
      };
    return { ...native, status: "unknown" };
  }
  return { ...native, status: launch.synchronous ? "unknown" : "running" };
}
