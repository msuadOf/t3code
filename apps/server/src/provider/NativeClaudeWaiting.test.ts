import { expect, it } from "vite-plus/test";
import { nativeClaudeWaitingNotice } from "./NativeClaudeWaiting.ts";
const job = {
  kind: "background" as const,
  cwd: "/work",
  startedAt: 1,
  id: "1234abcd",
  status: "waiting",
  waitingFor: "unknown-native-value",
};
const question = {
  type: "tool" as const,
  id: "q",
  title: "AskUserQuestion",
  text: "",
  status: "running" as const,
  input: { questions: [{ question: "Color?", options: [{ label: "Blue", description: "Blue" }] }] },
};
it("reports generic native waiting without inferring an approval from unknown native fields", () => {
  const notice = nativeClaudeWaitingNotice(undefined, job, [], false);
  expect(notice?.status).toBe("running");
  expect(notice?.message).toContain("claude attach 1234abcd");
  expect(notice?.message).not.toContain("unknown-native-value");
  expect(nativeClaudeWaitingNotice(notice, job, [], false)).toBeUndefined();
});
it("leaves supported questions to their actionable form and explains unsupported multiple questions", () => {
  expect(nativeClaudeWaitingNotice(undefined, job, [question], false)).toBeUndefined();
  const multi = {
    ...question,
    input: { questions: [...question.input.questions, ...question.input.questions] },
  };
  expect(nativeClaudeWaitingNotice(undefined, job, [multi], false)?.message).toContain(
    "cannot answer",
  );
  expect(nativeClaudeWaitingNotice(undefined, job, [question, question], false)?.message).toContain(
    "cannot answer",
  );
});
it("replaces a stale waiting notice after continuation, terminal state, or a supported form takes over", () => {
  const previous = { status: "running" as const, message: "Waiting" };
  for (const update of [
    nativeClaudeWaitingNotice(previous, { ...job, status: "working" }, [], false),
    nativeClaudeWaitingNotice(previous, job, [], true),
    nativeClaudeWaitingNotice(previous, job, [question], false),
  ]) {
    expect(update?.status).toBe("completed");
    expect(update?.message).not.toContain("claude attach");
  }
  expect(nativeClaudeWaitingNotice(undefined, undefined, [], false)).toBeUndefined();
  expect(nativeClaudeWaitingNotice(previous, undefined, [], false)).toBeUndefined();
});
