/* eslint-disable unicorn/no-array-reverse -- Hermes does not provide toReversed; each reverse below owns a fresh array. */
import type { ModelActivity, ModelActivityFrame } from "@t3tools/contracts";

export interface ModelActivityBlock {
  readonly index: number;
  readonly kind: string;
  readonly text: string;
  readonly complete: boolean;
}
export interface ModelActivityRequest {
  readonly id: string;
  readonly sessionId: string;
  readonly messageId: string | null;
  readonly confirmed: boolean;
  readonly status: "running" | "completed" | "failed";
  readonly blocks: ReadonlyArray<ModelActivityBlock>;
}
export interface ModelActivityState {
  readonly available: boolean;
  readonly epoch: string;
  readonly seq: number;
  readonly gap: boolean;
  readonly truncated: boolean;
  readonly requests: ReadonlyArray<ModelActivityRequest>;
}
export const emptyModelActivityState: ModelActivityState = {
  available: false,
  epoch: "",
  seq: 0,
  gap: false,
  truncated: false,
  requests: [],
};
const MAX_REQUESTS = 32;
const MAX_TEXT = 256 * 1024;
const record = (value: unknown): Readonly<Record<string, unknown>> =>
  typeof value === "object" && value !== null && !Array.isArray(value)
    ? (value as Readonly<Record<string, unknown>>)
    : {};

function applyEvent(state: ModelActivityState, event: ModelActivity): ModelActivityState {
  if (event.seq <= state.seq) return state;
  let requests = [...state.requests];
  const at = requests.findIndex(
    (request) => request.id === event.requestId && request.sessionId === event.sessionId,
  );
  const previous = requests[at];
  let request: ModelActivityRequest = previous ?? {
    id: event.requestId,
    sessionId: event.sessionId,
    messageId: event.messageId,
    confirmed: false,
    status: "running",
    blocks: [],
  };
  request = { ...request, messageId: event.messageId ?? request.messageId };
  const index = event.data.index;
  if (typeof index === "number" && Number.isSafeInteger(index) && index >= 0 && index < 1024) {
    const blocks = [...request.blocks];
    const blockAt = blocks.findIndex((block) => block.index === index);
    const content = record(event.data.content_block);
    const delta = record(event.data.delta);
    let block: ModelActivityBlock = blocks[blockAt] ?? {
      index,
      kind: "unknown",
      text: "",
      complete: false,
    };
    if (event.type === "content_block_start") {
      block = {
        index,
        kind: typeof content.type === "string" ? content.type : "unknown",
        text:
          typeof content.text === "string"
            ? content.text
            : typeof content.thinking === "string"
              ? content.thinking
              : "",
        complete: false,
      };
    } else if (event.type === "content_block_delta") {
      const text =
        typeof delta.text === "string"
          ? delta.text
          : typeof delta.thinking === "string"
            ? delta.thinking
            : typeof delta.partial_json === "string"
              ? delta.partial_json
              : "";
      const kind =
        delta.type === "thinking_delta"
          ? "thinking"
          : delta.type === "text_delta"
            ? "text"
            : delta.type === "input_json_delta"
              ? "tool_use"
              : block.kind;
      block = { ...block, kind, text: block.text + text };
    } else if (event.type === "content_block_stop") block = { ...block, complete: true };
    if (blockAt < 0) blocks.push(block);
    else blocks[blockAt] = block;
    request = { ...request, blocks: blocks.sort((a, b) => a.index - b.index) };
  }
  if (event.type === "message_stop") request = { ...request, status: "completed" };
  if (event.type === "observer.request_end" && request.status === "running")
    request = { ...request, status: event.data.complete === true ? "completed" : "failed" };
  if (event.type === "error" || event.type === "observer.error")
    request = { ...request, status: "failed" };
  if (at < 0) requests.push(request);
  else requests[at] = request;
  let truncated = state.truncated || requests.length > MAX_REQUESTS;
  requests = requests.slice(-MAX_REQUESTS);
  // Keep recent activity bounded even if a provider emits a single enormous block.
  let remaining = MAX_TEXT;
  requests = [...requests]
    .reverse()
    .map((item) => ({
      ...item,
      blocks: [...item.blocks]
        .reverse()
        .map((block) => {
          const text = remaining > 0 ? block.text.slice(-remaining) : "";
          const kept = remaining > 0 ? text : "";
          remaining -= kept.length;
          truncated ||= kept.length !== block.text.length;
          return kept === block.text ? block : { ...block, text: kept };
        })
        .reverse(),
    }))
    .reverse();
  return {
    ...state,
    seq: event.seq,
    gap: state.gap || event.type === "observer.capture_gap",
    truncated,
    requests,
  };
}

export function reduceModelActivity(
  state: ModelActivityState,
  frame: ModelActivityFrame,
): ModelActivityState {
  if (frame.kind === "snapshot") {
    // A snapshot is a complete retained window, never an append to the previous epoch/window.
    const continuing = frame.after > 0 && state.epoch === frame.epoch && state.seq === frame.after;
    let next: ModelActivityState = {
      ...(continuing ? state : emptyModelActivityState),
      available: frame.available,
      epoch: frame.epoch,
      gap: frame.gap || (continuing && state.gap) || (frame.after > 0 && !continuing),
    };
    for (const event of frame.events) next = applyEvent(next, event);
    const confirmed = new Set(frame.confirmedMessageIds);
    return {
      ...next,
      seq: frame.seq,
      requests: next.requests.map((request) => ({
        ...request,
        confirmed:
          request.confirmed || (request.messageId !== null && confirmed.has(request.messageId)),
      })),
    };
  }
  if (frame.epoch !== state.epoch) return state;
  if (frame.kind === "event") return applyEvent(state, frame.event);
  const confirmed = new Set(frame.messageIds);
  return {
    ...state,
    requests: state.requests.map((request) =>
      request.confirmed || request.messageId === null || !confirmed.has(request.messageId)
        ? request
        : { ...request, confirmed: true },
    ),
  };
}
