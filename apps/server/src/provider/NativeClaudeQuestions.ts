import type {
  OrchestrationV2UserInputQuestion,
  ProviderUserInputAnswers,
} from "@t3tools/contracts";

const record = (value: unknown): value is Record<string, unknown> =>
  typeof value === "object" && value !== null && !Array.isArray(value);

/** The stock resolver addresses only questions[0]; multi-select uses its quoted label codec. */
export function nativeClaudeQuestion(input: unknown): OrchestrationV2UserInputQuestion | undefined {
  if (!record(input) || !Array.isArray(input.questions) || input.questions.length !== 1) return;
  const question = input.questions[0];
  if (
    !record(question) ||
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
    multiSelect: question.multiSelect === true,
    allowCustomAnswer: question.multiSelect !== true,
    required: true,
  };
}

const safeAnswer = (text: string) =>
  text.length > 0 &&
  text.length <= 2000 &&
  !text.startsWith("/") &&
  !text.startsWith("!") &&
  ![...text].some((character) => character.charCodeAt(0) < 32 || character.charCodeAt(0) === 127);

// Stock LLt/W format: JSON-quote labels containing comma-space or quotes, then join with comma-space.
function serializeLabels(labels: readonly string[]): string {
  return labels
    .map((label) => (label.includes(", ") || label.includes('"') ? JSON.stringify(label) : label))
    .join(", ");
}
function parseLabels(text: string): string[] | undefined {
  const labels: string[] = [];
  let rest = text;
  for (;;) {
    if (rest.startsWith('"')) {
      let end = -1;
      let escaped = false;
      for (let index = 1; index < rest.length; index++) {
        const character = rest[index];
        if (escaped) {
          escaped = false;
          continue;
        }
        if (character === "\\") {
          escaped = true;
          continue;
        }
        if (character === '"') {
          end = index;
          break;
        }
      }
      if (end === -1) return;
      try {
        const value: unknown = JSON.parse(rest.slice(0, end + 1));
        if (typeof value !== "string") return;
        labels.push(value);
      } catch {
        return;
      }
      rest = rest.slice(end + 1);
    } else {
      const end = rest.indexOf(", ");
      const label = end === -1 ? rest : rest.slice(0, end);
      if (label.includes('"')) return;
      labels.push(label);
      rest = end === -1 ? "" : rest.slice(end);
    }
    if (!rest) break;
    if (!rest.startsWith(", ")) return;
    rest = rest.slice(2);
    if (!rest) return;
  }
  return labels;
}

export function nativeClaudeQuestionAnswer(
  question: OrchestrationV2UserInputQuestion,
  answers: ProviderUserInputAnswers | undefined,
): string | undefined {
  const raw = answers?.[question.id];
  if (question.multiSelect) {
    if (!Array.isArray(raw) || raw.length === 0 || raw.length > question.options.length) return;
    const labels: string[] = [];
    for (const value of raw) {
      if (typeof value !== "string" || !safeAnswer(value.trim())) return;
      const option = question.options.find(
        (option) => option.label.toLowerCase() === value.trim().toLowerCase(),
      );
      if (!option || !safeAnswer(option.label) || labels.includes(option.label)) return;
      labels.push(option.label);
    }
    const text = serializeLabels(labels);
    return safeAnswer(text) ? text : undefined;
  }
  const value = Array.isArray(raw) && raw.length === 1 ? raw[0] : raw;
  if (typeof value !== "string") return;
  const text = value.trim();
  if (!safeAnswer(text)) return;
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
  if (typeof answer !== "string" || !answer.length) return;
  if (!question.multiSelect) return { [question.id]: answer };
  const labels = parseLabels(answer);
  if (!labels || labels.some((label) => !safeAnswer(label))) return;
  return labels.every((label) => question.options.some((option) => option.label === label))
    ? { [question.id]: labels }
    : { [question.id]: answer };
}
