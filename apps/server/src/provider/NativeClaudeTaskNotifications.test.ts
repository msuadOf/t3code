import { expect, it } from "vite-plus/test";
import { nativeClaudeTaskNotificationContents } from "./NativeClaudeTaskNotifications.ts";
const notice = (id: string) => `<task-notification><task-id>${id}</task-id></task-notification>`;
const joined = (parts: readonly string[], own = parts.map(() => true)) => ({
  type: "user",
  origin: { kind: "task-notification", producer: "session-task" },
  isMeta: true,
  joinedNoticeLengths: parts.map((text) => text.length),
  joinedNoticeOwn: own,
  joinedNoticeRunIds: parts.map((_, index) => `native-run-${index}`),
  message: { content: `System wrapper\n\n${parts.join("\n\n")}` },
});
it("splits joined notices by native lengths even with unicode and apparent protocol text inside a result", () => {
  const first = `<task-notification><result>😀\n\n${notice("nested")}</result></task-notification>`;
  const second = notice("second");
  expect(nativeClaudeTaskNotificationContents(joined([first, second]))).toEqual([first, second]);
});
it("only emits own notices without assigning ownership from their contents", () => {
  const parts = [notice("first"), notice("foreign"), notice("third")];
  expect(nativeClaudeTaskNotificationContents(joined(parts, [true, false, true]))).toEqual([
    parts[0],
    parts[2],
  ]);
});
it("rejects malformed envelope lengths, separators and corresponding metadata without salvaging a suffix", () => {
  const source = joined([notice("first"), notice("second")]);
  for (const invalid of [
    {
      ...source,
      joinedNoticeLengths: [source.joinedNoticeLengths[0]! - 1, source.joinedNoticeLengths[1]!],
    },
    { ...source, joinedNoticeLengths: [Number.MAX_SAFE_INTEGER, 1] },
    { ...source, joinedNoticeOwn: [true] },
    { ...source, joinedNoticeOwn: [true, "true"] },
    { ...source, joinedNoticeRunIds: ["native-run-0", ""] },
    {
      ...source,
      message: {
        content: source.message.content.replace(`${notice("first")}\n\n`, `${notice("first")}\n`),
      },
    },
    { ...source, origin: { kind: "user" } },
  ])
    expect(nativeClaudeTaskNotificationContents(invalid)).toEqual([]);
});
