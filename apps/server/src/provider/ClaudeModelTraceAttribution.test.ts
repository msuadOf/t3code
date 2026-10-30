import { expect, it } from "vite-plus/test";
import { claudeModelTraceAttribution } from "./ClaudeModelTraceAttribution.ts";

const sessionId = "11111111-2222-4333-8444-555555555555";
const traceId = "1".repeat(32);
const spanId = "2".repeat(16);
const traceparent = `00-${traceId}-${spanId}-01`;
const payload = (agentId: string | null = null, session = sessionId) => ({
  resourceSpans: [
    {
      scopeSpans: [
        {
          spans: [
            {
              name: "claude_code.llm_request",
              traceId,
              spanId,
              attributes: [
                { key: "session.id", value: { stringValue: session } },
                {
                  key: "query_source_safe",
                  value: {
                    stringValue: agentId ? "agent.builtin.general-purpose" : "repl_main_thread",
                  },
                },
                ...(agentId ? [{ key: "agent_id", value: { stringValue: agentId } }] : []),
                { key: "user_prompt", value: { stringValue: "must-never-leave-collector" } },
              ],
            },
          ],
        },
      ],
    },
  ],
});
const setup = () => {
  const events: Array<{ requestId: string; sessionId: string; data: Record<string, unknown> }> = [];
  const attribution = claudeModelTraceAttribution((event) => events.push(event));
  return { attribution, events };
};

it("joins each HTTP attempt to native source without request contents or cross-session matches", () => {
  const { attribution, events } = setup();
  attribution.observeRequest({ sessionId, requestId: "attempt-1", traceparent });
  attribution.observeRequest({ sessionId, requestId: "retry", traceparent });
  attribution.observeRequest({
    sessionId: "aaaaaaaa-bbbb-4ccc-8ddd-eeeeeeeeeeee",
    requestId: "other",
    traceparent,
  });
  attribution.observeTraces(payload("a123"));
  expect(events.map((event) => event.requestId)).toEqual(["attempt-1", "retry"]);
  expect(events[0]?.data).toEqual({
    state: "identified",
    querySource: "agent.builtin.general-purpose",
    agentId: "a123",
    parentAgentId: null,
  });
  expect(JSON.stringify(events)).not.toContain("must-never-leave-collector");
  attribution.observeTraces(payload("a123"));
  expect(events).toHaveLength(2);
});

it("handles telemetry before a model request and distinguishes identical-content concurrent children", () => {
  const { attribution, events } = setup();
  attribution.observeTraces(payload("a123"));
  attribution.observeRequest({ sessionId, requestId: "first-child", traceparent });
  const second = JSON.parse(JSON.stringify(payload("a456")));
  second.resourceSpans[0].scopeSpans[0].spans[0].spanId = "3".repeat(16);
  attribution.observeRequest({
    sessionId,
    requestId: "second-child",
    traceparent: `00-${traceId}-${"3".repeat(16)}-01`,
  });
  attribution.observeTraces(second);
  expect(events.map((event) => [event.requestId, event.data.agentId])).toEqual([
    ["first-child", "a123"],
    ["second-child", "a456"],
  ]);
});

it("revokes conflicting identities and never recovers them by replaying an older span", () => {
  const { attribution, events } = setup();
  attribution.observeRequest({ sessionId, requestId: "request", traceparent });
  attribution.observeTraces(payload());
  attribution.observeTraces(payload("a123"));
  attribution.observeTraces(payload());
  attribution.observeRequest({ sessionId, requestId: "later-retry", traceparent });
  expect(events.map((event) => event.data.state)).toEqual(["identified", "conflict", "conflict"]);
});

it("ignores missing, zero and invalid contexts and unrelated native spans", () => {
  const { attribution, events } = setup();
  for (const value of [
    undefined,
    "bad",
    `00-${"0".repeat(32)}-${spanId}-01`,
    `00-${traceId}-${"0".repeat(16)}-01`,
  ])
    attribution.observeRequest({ sessionId, requestId: String(value), traceparent: value });
  attribution.observeTraces(payload());
  expect(events).toEqual([]);
  attribution.observeRequest({ sessionId, requestId: "valid", traceparent });
  expect(events).toHaveLength(1);
});

it("rejects duplicate attributes, unrelated spans and oversized identities", () => {
  const { attribution, events } = setup();
  attribution.observeRequest({ sessionId, requestId: "request", traceparent });
  for (const damage of ["duplicate", "name", "length"]) {
    const value = payload();
    const span = value.resourceSpans[0]!.scopeSpans[0]!.spans[0]!;
    if (damage === "duplicate") span.attributes.push(span.attributes[0]!);
    if (damage === "name") span.name = "claude_code.tool";
    if (damage === "length") span.attributes[1]!.value.stringValue = "x".repeat(129);
    attribution.observeTraces(value);
  }
  expect(events).toEqual([]);
});

it("evicts bounded source metadata without guessing identities for a later request", () => {
  const { attribution, events } = setup();
  attribution.observeTraces(payload("a-original"));
  for (let index = 1; index <= 1024; index++) {
    const value = payload();
    value.resourceSpans[0]!.scopeSpans[0]!.spans[0]!.spanId = index.toString(16).padStart(16, "0");
    attribution.observeTraces(value);
  }
  attribution.observeRequest({ sessionId, requestId: "old", traceparent });
  expect(events).toEqual([]);
});
