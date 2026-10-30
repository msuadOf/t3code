type Entry = Readonly<Record<string, unknown>>;
const object = (value: unknown): value is Entry =>
  typeof value === "object" && value !== null && !Array.isArray(value);

/** Source metadata distinguishes native notices from user/model-authored XML. */
export function nativeClaudeTaskNotificationContents(entry: Entry): readonly string[] {
  if (
    entry.type !== "user" ||
    !object(entry.origin) ||
    entry.origin.kind !== "task-notification" ||
    entry.origin.producer !== "session-task" ||
    !object(entry.message) ||
    typeof entry.message.content !== "string"
  )
    return [];
  const content = entry.message.content;
  const complete = (text: string) =>
    text.startsWith("<task-notification>") && text.endsWith("</task-notification>");
  if (
    entry.turnOrigin === "task_notification" &&
    entry.promptSource === "system" &&
    entry.joinedNoticeLengths === undefined &&
    entry.joinedNoticeOwn === undefined &&
    entry.joinedNoticeRunIds === undefined
  ) {
    const direct = content.trim();
    return complete(direct) ? [direct] : [];
  }
  const lengths = entry.joinedNoticeLengths;
  const own = entry.joinedNoticeOwn;
  const runs = entry.joinedNoticeRunIds;
  if (
    entry.isMeta !== true ||
    !Array.isArray(lengths) ||
    lengths.length === 0 ||
    lengths.length > 1024 ||
    !Array.isArray(own) ||
    own.length !== lengths.length ||
    !Array.isArray(runs) ||
    runs.length !== lengths.length
  )
    return [];
  const result: string[] = [];
  let end = content.length;
  // Native lengths delimit each notice, including foreign notices; XML never determines the split.
  for (let index = lengths.length - 1; index >= 0; index--) {
    const length = lengths[index];
    if (
      !Number.isSafeInteger(length) ||
      length <= 0 ||
      length > end ||
      typeof own[index] !== "boolean" ||
      typeof runs[index] !== "string" ||
      !/^[a-zA-Z0-9_-]{1,128}$/.test(runs[index])
    )
      return [];
    const start = end - length;
    if (index > 0 && (start < 2 || content.slice(start - 2, start) !== "\n\n")) return [];
    if (own[index]) {
      const notice = content.slice(start, end).trim();
      if (!complete(notice)) return [];
      result.push(notice);
    }
    end = start - (index > 0 ? 2 : 0);
  }
  return result.toReversed();
}
