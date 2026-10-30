/** Only T3-owned worker settings enable telemetry; no prompt or raw-body logging is needed. */
export function claudeModelObserverEnvironment(modelBaseUrl: string): Record<string, string> {
  const url = new URL(modelBaseUrl);
  const match = /^\/proxy\/([a-f0-9]{64})$/.exec(url.pathname);
  if (
    url.protocol !== "http:" ||
    url.hostname !== "127.0.0.1" ||
    !url.port ||
    url.username ||
    url.password ||
    url.search ||
    url.hash ||
    !match
  )
    throw new Error("Invalid Claude observer local route");
  return {
    ANTHROPIC_BASE_URL: modelBaseUrl,
    CLAUDE_CODE_ENABLE_TELEMETRY: "1",
    CLAUDE_CODE_ENHANCED_TELEMETRY_BETA: "1",
    CLAUDE_CODE_PROPAGATE_TRACEPARENT: "1",
    ENABLE_BETA_TRACING_DETAILED: "0",
    OTEL_TRACES_EXPORTER: "otlp",
    OTEL_EXPORTER_OTLP_TRACES_COMPRESSION: "none",
    OTEL_EXPORTER_OTLP_TRACES_PROTOCOL: "http/json",
    OTEL_EXPORTER_OTLP_TRACES_ENDPOINT: `${url.origin}/v1/traces`,
    OTEL_EXPORTER_OTLP_TRACES_HEADERS: `Authorization=Bearer ${match[1]}`,
    OTEL_TRACES_EXPORT_INTERVAL: "250",
    OTEL_METRICS_EXPORTER: "none",
    OTEL_LOGS_EXPORTER: "none",
    OTEL_LOG_RAW_API_BODIES: "0",
    OTEL_LOG_USER_PROMPTS: "0",
    OTEL_LOG_ASSISTANT_RESPONSES: "0",
    OTEL_LOG_TOOL_DETAILS: "0",
    OTEL_LOG_TOOL_CONTENT: "0",
  };
}
