import type { ModelActivity, ModelActivityFrame } from "@t3tools/contracts";
import { describe, expect, it } from "vite-plus/test";
import { emptyModelActivityState, reduceModelActivity } from "./modelActivity.ts";

const event = (
  seq: number,
  type: string,
  data: Record<string, unknown> = {},
  overrides: Partial<ModelActivity> = {},
): ModelActivity => ({
  seq,
  sessionId: "session",
  requestId: "request",
  messageId: "model-id",
  type,
  data,
  ...overrides,
});
const snapshot = (
  events: ReadonlyArray<ModelActivity>,
  overrides: Partial<Extract<ModelActivityFrame, { kind: "snapshot" }>> = {},
): ModelActivityFrame => ({
  kind: "snapshot",
  available: true,
  epoch: "epoch",
  seq: events.at(-1)?.seq ?? 0,
  oldestSeq: 1,
  after: 0,
  gap: false,
  events,
  confirmedMessageIds: [],
  ...overrides,
});

describe("transient model activity", () => {
  it("assembles thinking/text by SSE block index and confirms only native model IDs", () => {
    let state = reduceModelActivity(
      emptyModelActivityState,
      snapshot([
        event(1, "content_block_start", {
          index: 0,
          content_block: { type: "thinking", thinking: "Plan" },
        }),
        event(2, "content_block_delta", {
          index: 0,
          delta: { type: "thinking_delta", thinking: " now" },
        }),
        event(3, "content_block_start", {
          index: 1,
          content_block: { type: "text", text: "Hello" },
        }),
        event(4, "message_stop"),
      ]),
    );
    expect(state.requests[0]?.blocks.map((block) => block.text)).toEqual(["Plan now", "Hello"]);
    expect(state.requests[0]?.status).toBe("completed");
    expect(state.requests[0]?.confirmed).toBe(false);
    state = reduceModelActivity(state, {
      kind: "confirmed",
      epoch: "epoch",
      messageIds: ["entry-uuid"],
    });
    expect(state.requests[0]?.confirmed).toBe(false);
    state = reduceModelActivity(state, {
      kind: "confirmed",
      epoch: "epoch",
      messageIds: ["model-id"],
    });
    expect(state.requests[0]?.confirmed).toBe(true);
  });
  it("deduplicates replayed events and rejects prior epochs", () => {
    const chunk = event(2, "content_block_delta", {
      index: 0,
      delta: { type: "text_delta", text: "x" },
    });
    const state = reduceModelActivity(emptyModelActivityState, snapshot([chunk]));
    expect(reduceModelActivity(state, { kind: "event", epoch: "epoch", event: chunk })).toBe(state);
    expect(
      reduceModelActivity(state, { kind: "event", epoch: "old", event: { ...chunk, seq: 3 } }),
    ).toBe(state);
  });
  it("rebases snapshots instead of appending partial windows", () => {
    const previous = reduceModelActivity(
      emptyModelActivityState,
      snapshot([event(1, "message_stop")]),
    );
    const state = reduceModelActivity(
      previous,
      snapshot(
        [
          event(9, "content_block_delta", {
            index: 2,
            delta: { type: "input_json_delta", partial_json: '{"x":' },
          }),
        ],
        { epoch: "new", gap: true },
      ),
    );
    expect(state.gap).toBe(true);
    expect(state.requests).toHaveLength(1);
    expect(state.requests[0]?.blocks[0]?.text).toBe('{"x":');
    expect(state.requests[0]?.confirmed).toBe(false);
  });
  it("continues a matching cursor and exposes a missing baseline", () => {
    const delta = (seq: number, text: string) =>
      event(seq, "content_block_delta", { index: 0, delta: { type: "text_delta", text } });
    const previous = reduceModelActivity(emptyModelActivityState, snapshot([delta(1, "a")]));
    const frame = snapshot([delta(2, "b")], { after: 1 });
    expect(reduceModelActivity(previous, frame).requests[0]?.blocks[0]?.text).toBe("ab");
    const missing = reduceModelActivity(emptyModelActivityState, frame);
    expect(missing.gap).toBe(true);
    expect(missing.requests[0]?.blocks[0]?.text).toBe("b");
  });
  it("bounds retained requests and total text", () => {
    const events = Array.from({ length: 40 }, (_, i) =>
      event(
        i + 1,
        "content_block_start",
        { index: 0, content_block: { type: "text", text: "a".repeat(20000) } },
        { requestId: String(i) },
      ),
    );
    const state = reduceModelActivity(emptyModelActivityState, snapshot(events));
    expect(state.requests).toHaveLength(32);
    expect(
      state.requests
        .flatMap((request) => request.blocks)
        .reduce((sum, block) => sum + block.text.length, 0),
    ).toBe(256 * 1024);
    expect(state.truncated).toBe(true);
  });
  it("exposes capture loss and closes interrupted requests without inventing native records", () => {
    const state = reduceModelActivity(
      emptyModelActivityState,
      snapshot([
        event(1, "observer.capture_gap", { reason: "event-too-large" }),
        event(2, "observer.request_end", { reason: "upstream-error", complete: false }),
      ]),
    );
    expect(state.gap).toBe(true);
    expect(state.requests[0]?.status).toBe("failed");
    expect(state.requests[0]?.confirmed).toBe(false);
  });
  it("never merges different sessions or requests with matching prompts or model IDs", () => {
    const state = reduceModelActivity(
      emptyModelActivityState,
      snapshot([
        event(1, "message_stop"),
        event(2, "message_stop", {}, { sessionId: "other" }),
        event(3, "message_stop", {}, { requestId: "other" }),
      ]),
    );
    expect(state.requests).toHaveLength(3);
  });
});
