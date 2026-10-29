// @effect-diagnostics nodeBuiltinImport:off -- a native worker owns this independent stdio relay process.
import * as Effect from "effect/Effect";
import * as Stream from "effect/Stream";
import { discardResponseBody, responsePayloads } from "@t3tools/shared/mcpResponsePayloads";
import { AcpMcpBridgeError, runAcpMcpStdioBridge } from "../mcp/AcpMcpStdioBridge.ts";
import {
  CLAUDE_MCP_SESSION_METADATA_KEY,
  readClaudeMcpRelayCredential,
  type ClaudeMcpCredential,
} from "./NativeClaudeMcpBindings.ts";

interface Transport {
  readonly key: string;
  readonly credential: ClaudeMcpCredential;
  readonly sessionId: string | null;
}
const object = (value: unknown): value is Record<string, unknown> =>
  value !== null && typeof value === "object" && !Array.isArray(value);

/** Transport state never determines authorization; the private binding is checked for every request. */
export function makeNativeClaudeMcpFetch(options: {
  readonly launchDirectory: string;
  readonly nativeSessionId: string;
  readonly fetchImplementation?: (url: string, init?: RequestInit) => Promise<Response>;
}): ((url: string, init?: RequestInit) => Promise<Response>) & {
  readonly close: () => Promise<void>;
} {
  const fetchImplementation = options.fetchImplementation ?? fetch;
  const controller = new AbortController();
  let transport: Transport | undefined;
  let establishing: { readonly key: string; readonly promise: Promise<Transport> } | undefined;
  let initializeParams: unknown = {
    protocolVersion: "2025-06-18",
    capabilities: {},
    clientInfo: { name: "t3-native-claude", version: "1" },
  };
  const headers = (credential: ClaudeMcpCredential, sessionId: string | null) => ({
    accept: "application/json, text/event-stream",
    "content-type": "application/json",
    authorization: credential.authorizationHeader,
    "mcp-protocol-version": "2025-06-18",
    ...(sessionId === null ? {} : { "mcp-session-id": sessionId }),
  });
  const retire = async (previous: Transport | undefined): Promise<void> => {
    if (!previous?.sessionId) return;
    try {
      const response = await fetchImplementation(previous.credential.endpoint, {
        method: "DELETE",
        headers: headers(previous.credential, previous.sessionId),
        signal: AbortSignal.timeout(1000),
        redirect: "error",
      });
      await Effect.runPromise(discardResponseBody(response));
    } catch {
      // Rotation can revoke the old credential before its transport is retired.
    }
  };
  const establish = async (credential: ClaudeMcpCredential, key: string): Promise<Transport> => {
    const response = await fetchImplementation(credential.endpoint, {
      method: "POST",
      headers: headers(credential, null),
      redirect: "error",
      signal: controller.signal,
      body: JSON.stringify({
        jsonrpc: "2.0",
        id: "t3-native-reconnect",
        method: "initialize",
        params: initializeParams,
      }),
    });
    if (!response.ok) {
      await Effect.runPromise(discardResponseBody(response));
      throw new Error("Native Claude MCP could not establish its current transport.");
    }
    const payloads = await Effect.runPromise(
      Stream.runCollect(
        responsePayloads(
          response,
          () => new AcpMcpBridgeError("Invalid native Claude MCP handshake."),
        ),
      ),
    );
    if (
      !payloads.some(
        (payload) =>
          object(payload) && payload.id === "t3-native-reconnect" && object(payload.result),
      )
    )
      throw new Error("Native Claude MCP handshake was rejected.");
    const next = { key, credential, sessionId: response.headers.get("mcp-session-id") };
    const notified = await fetchImplementation(credential.endpoint, {
      method: "POST",
      headers: headers(credential, next.sessionId),
      redirect: "error",
      signal: controller.signal,
      body: JSON.stringify({ jsonrpc: "2.0", method: "notifications/initialized" }),
    });
    await Effect.runPromise(discardResponseBody(notified));
    if (!notified.ok) throw new Error("Native Claude MCP initialization was rejected.");
    const previous = transport;
    transport = next;
    await retire(previous);
    return next;
  };
  const forward = async (_url: string, init?: RequestInit): Promise<Response> => {
    if (typeof init?.body !== "string") throw new Error("Invalid native Claude MCP request.");
    const message: unknown = JSON.parse(init.body);
    if (!object(message))
      throw new Error("Native Claude MCP requires individual JSON-RPC messages.");
    const discovery =
      message.method === "initialize" ||
      message.method === "server/discover" ||
      message.method === "notifications/initialized" ||
      message.method === "tools/list" ||
      message.method === "ping";
    const credential = await readClaudeMcpRelayCredential({ ...options, discovery });
    const key = `${credential.endpoint}\0${credential.providerSessionId}\0${credential.authorizationHeader}`;
    if (message.method === "initialize") {
      initializeParams = message.params;
      const response = await fetchImplementation(credential.endpoint, {
        ...init,
        headers: headers(credential, null),
        redirect: "error",
        signal: controller.signal,
      });
      if (response.ok) {
        const previous = transport;
        transport = { key, credential, sessionId: response.headers.get("mcp-session-id") };
        await retire(previous);
      }
      return response;
    }
    if (transport?.key !== key) {
      if (establishing?.key !== key) establishing = { key, promise: establish(credential, key) };
      try {
        await establishing.promise;
      } finally {
        if (establishing?.key === key) establishing = undefined;
      }
    }
    const current = transport;
    if (!current || current.key !== key)
      throw new Error("Native Claude MCP transport changed during authorization.");
    if (message.method === "tools/call") {
      const params = object(message.params) ? message.params : {};
      message.params = {
        ...params,
        _meta: {
          ...(object(params._meta) ? params._meta : {}),
          [CLAUDE_MCP_SESSION_METADATA_KEY]: options.nativeSessionId,
        },
      };
    }
    return fetchImplementation(credential.endpoint, {
      ...init,
      headers: headers(credential, current.sessionId),
      body: JSON.stringify(message),
      redirect: "error",
      signal: controller.signal,
    });
  };
  return Object.assign(forward, {
    close: async () => {
      controller.abort();
      await retire(transport);
    },
  });
}

export function runNativeClaudeMcpRelay(options: {
  readonly launchDirectory: string;
  readonly environment?: NodeJS.ProcessEnv;
  readonly input?: NodeJS.ReadableStream;
  readonly output?: { write(chunk: string): unknown };
  readonly fetchImplementation?: (url: string, init?: RequestInit) => Promise<Response>;
}): Effect.Effect<void> {
  const nativeSessionId = (options.environment ?? process.env).CLAUDE_CODE_SESSION_ID ?? "";
  const relayFetch = makeNativeClaudeMcpFetch({ ...options, nativeSessionId });
  return runAcpMcpStdioBridge({
    endpoint: "http://127.0.0.1/mcp",
    authorization: "",
    input: options.input ?? process.stdin,
    output: options.output ?? process.stdout,
    fetchImplementation: relayFetch,
  }).pipe(Effect.ensuring(Effect.tryPromise(() => relayFetch.close()).pipe(Effect.ignore)));
}
