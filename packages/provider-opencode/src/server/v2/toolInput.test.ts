import { assert, it } from "@effect/vitest";
import { appendToolInput, endToolInput, interruptToolInput, newToolInput } from "./toolInput.ts";

it("bounds native input tails, marks gaps, and accepts a complete final replacement", () => {
  const input = newToolInput();
  appendToolInput(input, "a".repeat(65536));
  appendToolInput(input, "tail");
  assert.lengthOf(input.text, 65536);
  assert.isTrue(input.text.endsWith("tail"));
  assert.isTrue(input.incomplete);
  interruptToolInput(input);
  appendToolInput(input, "suffix");
  assert.deepEqual(input, { text: "suffix", incomplete: true, ended: false });
  endToolInput(input, "complete");
  assert.deepEqual(input, { text: "complete", incomplete: false, ended: true });
  assert.isFalse(appendToolInput(input, "late"));
  endToolInput(input, "x".repeat(65537));
  assert.lengthOf(input.text, 65536);
  assert.isTrue(input.incomplete);
  endToolInput(input, "界".repeat(65536));
  assert.isAtMost(Buffer.byteLength(input.text, "utf8"), 65536);
  assert.notInclude(input.text, "�");
  assert.isTrue(input.incomplete);
});
