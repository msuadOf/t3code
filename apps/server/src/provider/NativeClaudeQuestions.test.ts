import { expect, it } from "@effect/vitest";
import { nativeClaudeQuestion, nativeClaudeQuestionAnswer } from "./NativeClaudeQuestions.ts";

const input = {
  questions: [
    {
      question: "Choose a color",
      header: "Color",
      options: [
        { label: "Blue", description: "Use blue" },
        { label: "Green", description: "Use green" },
      ],
      multiSelect: false,
    },
  ],
};
it("maps only the native resolver's one-question single-select shape", () => {
  expect(nativeClaudeQuestion(input)).toMatchObject({
    id: "Choose a color",
    allowCustomAnswer: true,
  });
  expect(
    nativeClaudeQuestion({ questions: [...input.questions, ...input.questions] }),
  ).toBeUndefined();
  expect(
    nativeClaudeQuestion({ questions: [{ ...input.questions[0], multiSelect: true }] }),
  ).toBeUndefined();
  expect(
    nativeClaudeQuestion({ questions: [{ ...input.questions[0], options: [{}] }] }),
  ).toBeUndefined();
});
it("canonicalizes one label or free answer but refuses command and control input", () => {
  const question = nativeClaudeQuestion(input)!;
  expect(nativeClaudeQuestionAnswer(question, { [question.id]: ["blue"] })).toBe("Blue");
  expect(nativeClaudeQuestionAnswer(question, { [question.id]: "custom color" })).toBe(
    "custom color",
  );
  for (const value of ["", " /compact", "!echo yes", "a\u001b[B", ["Blue", "Green"]])
    expect(nativeClaudeQuestionAnswer(question, { [question.id]: value })).toBeUndefined();
});
