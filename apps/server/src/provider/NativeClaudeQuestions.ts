import type {
  OrchestrationV2UserInputQuestion,
  ProviderUserInputAnswers,
} from "@t3tools/contracts";

const record = (value: unknown): value is Record<string, unknown> =>
  typeof value === "object" && value !== null && !Array.isArray(value);

/** The stock native resolver handles only questions[0], as a single label or free text. */
export function nativeClaudeQuestion(input: unknown): OrchestrationV2UserInputQuestion | undefined {
  if (!record(input) || !Array.isArray(input.questions) || input.questions.length !== 1) return;
  const question = input.questions[0];
  if (
    !record(question) ||
    question.multiSelect === true ||
    typeof question.question !== "string" ||
    !question.question.trim() ||
    question.question.length > 2000 ||
    !Array.isArray(question.options) ||
    question.options.length > 50
  )
    return;
  const options: Array<{ label: string; description: string }> = [];
  for (const option of question.options) {
    if (
      !record(option) ||
      typeof option.label !== "string" ||
      !option.label.trim() ||
      typeof option.description !== "string"
    )
      return;
    options.push({ label: option.label, description: option.description });
  }
  return {
    id: question.question,
    header:
      typeof question.header === "string" && question.header.trim() ? question.header : "Question",
    question: question.question,
    options,
    multiSelect: false,
    allowCustomAnswer: true,
    required: true,
  };
}

export function nativeClaudeQuestionAnswer(
  question: OrchestrationV2UserInputQuestion,
  answers: ProviderUserInputAnswers | undefined,
): string | undefined {
  const raw = answers?.[question.id];
  const value = Array.isArray(raw) && raw.length === 1 ? raw[0] : raw;
  if (typeof value !== "string") return;
  const text = value.trim();
  if (
    !text ||
    text.length > 2000 ||
    text.startsWith("/") ||
    text.startsWith("!") ||
    [...text].some((character) => character.charCodeAt(0) < 32 || character.charCodeAt(0) === 127)
  )
    return;
  return (
    question.options.find((option) => option.label.toLowerCase() === text.toLowerCase())?.label ??
    text
  );
}

export function nativeClaudeQuestionResult(
  question: OrchestrationV2UserInputQuestion,
  result: unknown,
): ProviderUserInputAnswers | undefined {
  if (!record(result) || !record(result.answers)) return;
  const answer = result.answers[question.id];
  return typeof answer === "string" && answer.length > 0 ? { [question.id]: answer } : undefined;
}
