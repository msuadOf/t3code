// Raw input is an observation, not parsed tool arguments. Keep only a bounded tail.
const MAX_INPUT_TEXT = 64 * 1024;

function boundedTail(text: string): { text: string; truncated: boolean } {
  const bytes = Buffer.from(text, "utf8");
  if (bytes.length <= MAX_INPUT_TEXT) return { text, truncated: false };
  let start = bytes.length - MAX_INPUT_TEXT;
  // Skip UTF-8 continuation bytes so the tail starts at a character boundary.
  while (start < bytes.length && (bytes[start]! & 0xc0) === 0x80) start++;
  return { text: bytes.subarray(start).toString("utf8"), truncated: true };
}

export interface ToolInputObservation {
  text: string;
  incomplete: boolean;
  ended: boolean;
}

export function newToolInput(): ToolInputObservation {
  return { text: "", incomplete: false, ended: false };
}

export function appendToolInput(input: ToolInputObservation, delta: string): boolean {
  if (input.ended) return false;
  const tail = boundedTail(input.text + delta);
  input.text = tail.text;
  input.incomplete ||= tail.truncated;
  return true;
}

export function endToolInput(input: ToolInputObservation, text: string): void {
  const tail = boundedTail(text);
  input.text = tail.text;
  input.incomplete = tail.truncated;
  input.ended = true;
}

export function interruptToolInput(input: ToolInputObservation): boolean {
  if (input.ended) return false;
  input.text = "";
  input.incomplete = true;
  return true;
}
