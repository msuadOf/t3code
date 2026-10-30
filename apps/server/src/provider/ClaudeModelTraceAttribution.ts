const record = (value: unknown): value is Record<string, unknown> =>
  typeof value === "object" && value !== null && !Array.isArray(value);
const UUID = /^[a-f0-9]{8}-(?:[a-f0-9]{4}-){3}[a-f0-9]{12}$/i;
const hexId = (value: unknown, length: number): value is string =>
  typeof value === "string" &&
  new RegExp(`^[a-f0-9]{${length}}$`).test(value) &&
  /[1-9a-f]/.test(value);
type Source =
  | {
      state: "identified";
      querySource: string;
      agentId: string | null;
      parentAgentId: string | null;
    }
  | { state: "conflict" };
const identity = (span: Record<string, unknown>) => {
  if (span.name !== "claude_code.llm_request" || !Array.isArray(span.attributes)) return;
  const attributes = new Map<string, string>();
  for (const item of span.attributes) {
    if (!record(item) || typeof item.key !== "string" || !record(item.value)) continue;
    if (!["session.id", "query_source_safe", "agent_id", "parent_agent_id"].includes(item.key))
      continue;
    if (attributes.has(item.key) || typeof item.value.stringValue !== "string") return;
    attributes.set(item.key, item.value.stringValue);
  }
  const sessionId = attributes.get("session.id");
  const querySource = attributes.get("query_source_safe");
  const agentId = attributes.get("agent_id") ?? null;
  const parentAgentId = attributes.get("parent_agent_id") ?? null;
  if (
    !sessionId ||
    !UUID.test(sessionId) ||
    !querySource ||
    !/^[a-zA-Z0-9_.:-]{1,128}$/.test(querySource)
  )
    return;
  if ([agentId, parentAgentId].some((id) => id !== null && !/^[a-zA-Z0-9_.:-]{1,128}$/.test(id)))
    return;
  return {
    sessionId,
    source: { state: "identified", querySource, agentId, parentAgentId } satisfies Source,
  };
};

/** Native span identity establishes source only; transcript persistence is a separate fact. */
export function claudeModelTraceAttribution(
  publish: (event: { sessionId: string; requestId: string; data: Record<string, unknown> }) => void,
) {
  const buckets = new Map<string, { source?: Source; requests: Map<string, string> }>();
  const bucket = (key: string) => {
    let value = buckets.get(key);
    if (!value) {
      value = { requests: new Map() };
      buckets.set(key, value);
      while (buckets.size > 1024) buckets.delete(buckets.keys().next().value!);
    }
    return value;
  };
  return {
    observeRequest(input: {
      sessionId: string;
      requestId: string;
      traceparent?: string | undefined;
    }) {
      const match = /^00-([a-f0-9]{32})-([a-f0-9]{16})-[a-f0-9]{2}$/.exec(input.traceparent ?? "");
      if (!UUID.test(input.sessionId) || !match || !hexId(match[1], 32) || !hexId(match[2], 16))
        return;
      const value = bucket(`${input.sessionId}:${match[1]}:${match[2]}`);
      if (value.requests.has(input.requestId) || value.requests.size >= 16) return;
      value.requests.set(input.requestId, input.sessionId);
      if (value.source)
        publish({ sessionId: input.sessionId, requestId: input.requestId, data: value.source });
    },
    observeTraces(payload: unknown) {
      if (!record(payload) || !Array.isArray(payload.resourceSpans)) return false;
      for (const resource of payload.resourceSpans) {
        if (!record(resource) || !Array.isArray(resource.scopeSpans)) continue;
        for (const scope of resource.scopeSpans) {
          if (!record(scope) || !Array.isArray(scope.spans)) continue;
          for (const span of scope.spans) {
            if (!record(span) || !hexId(span.traceId, 32) || !hexId(span.spanId, 16)) continue;
            const parsed = identity(span);
            if (!parsed) continue;
            const value = bucket(`${parsed.sessionId}:${span.traceId}:${span.spanId}`);
            if (value.source?.state === "conflict") continue;
            if (JSON.stringify(value.source) === JSON.stringify(parsed.source)) continue;
            value.source = value.source ? { state: "conflict" } : parsed.source;
            for (const [requestId, sessionId] of value.requests)
              publish({ sessionId, requestId, data: value.source });
          }
        }
      }
      return true;
    },
  };
}
