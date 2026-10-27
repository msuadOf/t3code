import { describe, expect, it } from "vite-plus/test";
import type * as ProviderAdapter from "@t3tools/provider-core/server/ProviderAdapter";
import { makeAssistantStreamingFilter, splitBufferedAssistantText } from "./assistantStreaming.ts";

describe("V2 assistant streaming", () => {
  it("keeps code fences intact", () => {
    expect(splitBufferedAssistantText("Intro\n\n```ts\nx()\n\n")).toEqual({
      ready: "Intro\n\n",
      rest: "```ts\nx()\n\n",
    });
    expect(splitBufferedAssistantText("```ts\nx()\n```\nrest")).toEqual({
      ready: "```ts\nx()\n```\n",
      rest: "rest",
    });
  });
});

it("delivers partial text and thinking immediately without paragraph boundaries", () => {
  const filter = makeAssistantStreamingFilter("token");
  const message = {
    type: "message.updated",
    driver: "opencode",
    message: { id: "reply", role: "assistant", text: "Partial", streaming: true },
  } as Extract<ProviderAdapter.ProviderAdapterV2Event, { type: "message.updated" }>;
  const thinking = {
    type: "turn_item.updated",
    driver: "opencode",
    turnItem: { id: "thinking", type: "reasoning", text: "Checking", streaming: true },
  } as ProviderAdapter.ProviderAdapterV2Event;
  expect(filter(message, 0)).toBe(message);
  expect(filter(thinking, 1)).toBe(thinking);
  const next = { ...message, message: { ...message.message, text: "Partial reply" } };
  expect(filter(next, 2)).toBe(next);
  const complete = { ...next, message: { ...next.message, streaming: false } };
  expect(filter(complete, 3)).toBe(complete);
});

it("keeps an unfinished paragraph buffered when paragraph mode is selected", () => {
  const filter = makeAssistantStreamingFilter("paragraph");
  const partial = {
    type: "message.updated",
    driver: "opencode",
    message: { id: "reply", role: "assistant", text: "Partial", streaming: true },
  } as ProviderAdapter.ProviderAdapterV2Event;
  expect(filter(partial, 0)).toBeNull();
});

it("holds a streamed section heading until its content has a boundary", () => {
  expect(splitBufferedAssistantText("Intro\n\n## Results\n\n")).toEqual({
    ready: "Intro\n\n",
    rest: "## Results\n\n",
  });
  expect(splitBufferedAssistantText("**Results**\n\nBody\n\nNext")).toEqual({
    ready: "**Results**\n\nBody\n\n",
    rest: "Next",
  });
});
