import { expect, it } from "@effect/vitest";
import {
  nativeClaudeQuestion,
  nativeClaudeQuestionAnswer,
  nativeClaudeQuestionResult,
} from "./NativeClaudeQuestions.ts";

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
it("maps only the native resolver's one-question shape", () => {
  expect(nativeClaudeQuestion(input)).toMatchObject({
    id: "Choose a color",
    allowCustomAnswer: true,
  });
  expect(
    nativeClaudeQuestion({ questions: [...input.questions, ...input.questions] }),
  ).toBeUndefined();
  expect(
    nativeClaudeQuestion({ questions: [{ ...input.questions[0], multiSelect: true }] }),
  ).toMatchObject({ multiSelect: true, allowCustomAnswer: false });
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

it("serializes known multi-select labels and decodes actual native results with stock quoting", () => {
  const labels = ["Blue, Navy", 'Green "forest"', "Comma,no-space", "Back\\slash"];
  const question = nativeClaudeQuestion({
    questions: [
      {
        ...input.questions[0],
        multiSelect: true,
        options: labels.map((label) => ({ label, description: label })),
      },
    ],
  })!;
  const answer = nativeClaudeQuestionAnswer(question, { [question.id]: labels });
  expect(answer).toBe('"Blue, Navy", "Green \\"forest\\"", Comma,no-space, Back\\slash');
  expect(nativeClaudeQuestionResult(question, { answers: { [question.id]: answer } })).toEqual({
    [question.id]: labels,
  });
  expect(nativeClaudeQuestionAnswer(question, { [question.id]: ["blue, navy"] })).toBe(
    '"Blue, Navy"',
  );
  for (const value of [
    [],
    ["unknown"],
    ["Blue, Navy", "unknown"],
    ["Blue, Navy", "Blue, Navy"],
    ["/compact"],
    ["bad\u001b"],
    "Blue, Navy",
  ]) {
    expect(nativeClaudeQuestionAnswer(question, { [question.id]: value })).toBeUndefined();
  }
  for (const value of ['"unterminated', '"Blue, Navy"garbage', '"Blue, Navy", ', 'plain"quote']) {
    expect(
      nativeClaudeQuestionResult(question, { answers: { [question.id]: value } }),
    ).toBeUndefined();
  }
  expect(
    nativeClaudeQuestionResult(question, { answers: { [question.id]: "Native other answer" } }),
  ).toEqual({ [question.id]: "Native other answer" });
});
