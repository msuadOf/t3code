import { expect, it } from "vite-plus/test";
import { terminalOutputPlainText } from "@t3tools/shared/terminalOutput";
import { NativeClaudeTerminalProgress } from "./NativeClaudeTerminalProgress.ts";

it("applies split ANSI cursor redraws instead of appending progress frames", () => {
  const progress = new NativeClaudeTerminalProgress("session");
  progress.accept({ type: "stream", line: "\u001b]0;private window title\u0007progress 10%" });
  progress.accept({ type: "stream", line: "\r\u001b[" });
  progress.accept({ type: "stream", line: "2Kprogress 90%" });
  const snapshot = progress.snapshot();
  expect(terminalOutputPlainText(snapshot.text)).toBe("progress 90%");
  expect(snapshot.partial).toBe(true);
});

it("resets at reconnect and rejects a baseline from another native session", () => {
  const progress = new NativeClaudeTerminalProgress("one");
  progress.accept({ type: "stream", line: "old screen" });
  progress.accept({ type: "snapshot", record: { sessionId: "other" } });
  progress.accept({ type: "stream", line: "foreign bytes" });
  expect(progress.snapshot().text).toBe("");
  progress.accept({ type: "snapshot", record: { sessionId: "one" } });
  progress.accept({ type: "stream", line: "new screen" });
  expect(progress.snapshot().text).toBe("new screen");
});

it("bounds native output, marks dropped content and ignores nonterminal data", () => {
  const progress = new NativeClaudeTerminalProgress("one");
  progress.accept({ type: "state", line: "not terminal output" });
  expect(progress.snapshot().text).toBe("");
  progress.accept({ type: "stream", line: "x".repeat(100_000) });
  expect(progress.snapshot().text.length).toBeLessThanOrEqual(16 * 1024 + 100);
  expect(progress.snapshot().truncated).toBe(true);
});
