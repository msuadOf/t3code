import { expect, it } from "vite-plus/test";
import { nativeClaudeConfirmedModelIds } from "./NativeClaudeModelMessages.ts";
const sessionId = "session";
const message = (id: string, at = "2026-10-30T00:00:00Z") => ({
  type: "assistant",
  sessionId,
  timestamp: at,
  message: { id },
});
it("confirms distinct persisted child responses even when their text and prompts are identical", () => {
  expect(
    nativeClaudeConfirmedModelIds(
      sessionId,
      [message("main")],
      [
        { agentId: "a1", entries: [message("child-one")] },
        { agentId: "a2", entries: [message("child-two")] },
      ],
    ),
  ).toEqual(["main", "child-one", "child-two"]);
});
it("deduplicates repeated blocks but rejects model IDs reused by different native owners", () => {
  expect(
    nativeClaudeConfirmedModelIds(
      sessionId,
      [message("collision"), message("main")],
      [
        { agentId: "a1", entries: [message("child"), message("child"), message("collision")] },
        { agentId: "a2", entries: [message("collision")] },
      ],
    ),
  ).toEqual(["main", "child"]);
});
it("keeps a bounded recent set without starving newly persisted child messages behind old main history", () => {
  const main = Array.from({ length: 300 }, (_, index) => message(`main-${index}`));
  const result = nativeClaudeConfirmedModelIds(sessionId, main, [
    { agentId: "a1", entries: [message("new-child", "2026-10-30T00:00:01Z")] },
  ]);
  expect(result).toHaveLength(256);
  expect(result[0]).toBe("new-child");
  expect(result[1]).toBe("main-299");
});
it("never treats entry UUIDs, foreign sessions or tool results as persisted model messages", () => {
  expect(
    nativeClaudeConfirmedModelIds(
      sessionId,
      [
        { type: "assistant", sessionId, uuid: "entry-only", message: {} },
        { ...message("foreign"), sessionId: "other" },
        { ...message("user"), type: "user" },
      ],
      [],
    ),
  ).toEqual([]);
});
