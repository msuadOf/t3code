// @effect-diagnostics nodeBuiltinImport:off -- isolated private native MCP metadata fixtures.
import * as NodeFSP from "node:fs/promises";
import * as NodeOS from "node:os";
import * as NodePath from "node:path";
import { EnvironmentId, ProviderInstanceId, ThreadId } from "@t3tools/contracts";
import { expect, it } from "@effect/vitest";
import {
  NativeClaudeMcpBindings,
  assertClaudeMcpEndpoint,
  readClaudeMcpRelayCredential,
} from "./NativeClaudeMcpBindings.ts";
import { makeNativeClaudeMcpFetch } from "./NativeClaudeMcpRelay.ts";

const A = "aaaaaaaa-1234-1234-1234-123456789abc";
const B = "bbbbbbbb-1234-1234-1234-123456789abc";
const config = (thread = "one", generation = "first") => ({
  environmentId: EnvironmentId.make("env"),
  threadId: ThreadId.make(thread),
  providerInstanceId: ProviderInstanceId.make("claude"),
  providerSessionId: generation,
  authorizationHeader: `Bearer ${generation}`,
  endpoint: "http://127.0.0.1:1234/mcp",
  browserToolsAvailable: false,
});
const launcher = (directory: string) => ({ command: "node", args: ["relay", directory] });

it("limits bootstrap to discovery and preserves exact native ownership across recover and revocation", async () => {
  const root = await NodeFSP.realpath(
    await NodeFSP.mkdtemp(NodePath.join(NodeOS.tmpdir(), "claude-mcp-")),
  );
  try {
    const ledger = new NativeClaudeMcpBindings(NodePath.join(root, "bindings"));
    const prepared = await ledger.prepare({ config: config(), launcher });
    const read = (nativeSessionId: string, discovery = false) =>
      readClaudeMcpRelayCredential({
        launchDirectory: prepared.launchDirectory,
        nativeSessionId,
        discovery,
      });
    expect((await read(A, true)).providerSessionId).toBe("first");
    await expect(read(A)).rejects.toThrow("not bound");
    await ledger.bind({ prepared, nativeSessionId: A, config: config() });
    expect((await read(A)).threadId).toBe("one");
    await expect(read(B)).rejects.toThrow("not bound");
    const second = await ledger.prepare({ config: config("two"), launcher });
    await ledger.bind({ prepared: second, nativeSessionId: B, config: config("two") });
    await expect(read(B, true)).rejects.toThrow("unavailable");
    expect(
      (
        await readClaudeMcpRelayCredential({
          launchDirectory: second.launchDirectory,
          nativeSessionId: B,
          discovery: false,
        })
      ).threadId,
    ).toBe("two");
    await expect(ledger.recover({ nativeSessionId: A, config: config("other") })).rejects.toThrow(
      "owner",
    );
    const reopened = new NativeClaudeMcpBindings(ledger.directory);
    await reopened.recover({ nativeSessionId: A, config: config("one", "second") });
    expect((await read(A)).providerSessionId).toBe("second");
    await reopened.revoke({ nativeSessionId: A, expectedProviderSessionId: "first" });
    expect(await reopened.hasBinding(A)).toBe(true);
    await reopened.revoke({ nativeSessionId: A, expectedProviderSessionId: "second" });
    expect(await reopened.hasBinding(A)).toBe(false);
    await expect(read(A, true)).rejects.toThrow("unavailable");
    await expect(reopened.recover({ nativeSessionId: A, config: config() })).rejects.toThrow(
      "owner",
    );
    expect(await NodeFSP.readFile(prepared.mcpConfigPath, "utf8")).not.toContain("Bearer");
  } finally {
    await NodeFSP.rm(root, { recursive: true, force: true });
  }
});

it("rejects symlinked metadata and non-loopback credential destinations", async () => {
  const root = await NodeFSP.realpath(
    await NodeFSP.mkdtemp(NodePath.join(NodeOS.tmpdir(), "claude-mcp-")),
  );
  try {
    const real = NodePath.join(root, "real");
    await NodeFSP.mkdir(real, { mode: 0o700 });
    const link = NodePath.join(root, "link");
    await NodeFSP.symlink(real, link);
    await expect(
      new NativeClaudeMcpBindings(link).prepare({ config: config(), launcher }),
    ).rejects.toThrow("symlinked");
    await expect(
      new NativeClaudeMcpBindings(real).prepare({
        config: { ...config(), endpoint: "https://example.com/mcp" },
        launcher,
      }),
    ).rejects.toThrow("loopback");
    await expect(
      new NativeClaudeMcpBindings(real).prepare({
        config: { ...config(), endpoint: "http://127.evil.test/mcp" },
        launcher,
      }),
    ).rejects.toThrow("loopback");
  } finally {
    await NodeFSP.rm(root, { recursive: true, force: true });
  }
});

it("re-handshakes after credential rotation and never uses a cached transport as authorization", async () => {
  const root = await NodeFSP.realpath(
    await NodeFSP.mkdtemp(NodePath.join(NodeOS.tmpdir(), "claude-mcp-")),
  );
  try {
    const ledger = new NativeClaudeMcpBindings(NodePath.join(root, "bindings"));
    const prepared = await ledger.prepare({ config: config(), launcher });
    const requests: {
      method: string;
      authorization: string | null;
      session: string | null;
      message: Record<string, unknown>;
    }[] = [];
    const fetchImplementation = async (_url: string, init?: RequestInit) => {
      if (init?.method === "DELETE") return new Response(null, { status: 204 });
      const message = JSON.parse(String(init?.body)) as Record<string, unknown>;
      const headers = new Headers(init?.headers);
      requests.push({
        method: String(message.method),
        authorization: headers.get("authorization"),
        session: headers.get("mcp-session-id"),
        message,
      });
      return message.method === "notifications/initialized"
        ? new Response(null, { status: 202 })
        : Response.json(
            {
              jsonrpc: "2.0",
              id: message.id,
              result:
                message.method === "initialize"
                  ? { protocolVersion: "2025-06-18", capabilities: {} }
                  : {},
            },
            { headers: { "mcp-session-id": `session-${headers.get("authorization")}` } },
          );
    };
    const forward = makeNativeClaudeMcpFetch({
      launchDirectory: prepared.launchDirectory,
      nativeSessionId: A,
      fetchImplementation,
    });
    const send = (method: string, params = {}) =>
      forward("http://ignored", {
        method: "POST",
        headers: { "mcp-session-id": "untrusted-old-session" },
        body: JSON.stringify({ jsonrpc: "2.0", id: 1, method, params }),
      });
    await send("initialize");
    await expect(send("tools/call")).rejects.toThrow("not bound");
    await ledger.bind({ prepared, nativeSessionId: A, config: config() });
    await send("tools/call", { name: "probe", _meta: { "t3/claudeSessionID": B } });
    await ledger.recover({ nativeSessionId: A, config: config("one", "second") });
    await send("tools/call");
    const last = requests.at(-1)!;
    expect(last.authorization).toBe("Bearer second");
    expect(last.session).toBe("session-Bearer second");
    expect((last.message.params as { _meta: unknown })._meta).toEqual({ "t3/claudeSessionID": A });
    expect(requests.filter((value) => value.method === "initialize")).toHaveLength(2);
    await ledger.revoke({ nativeSessionId: A });
    const count = requests.length;
    await expect(send("tools/call")).rejects.toThrow("unavailable");
    expect(requests).toHaveLength(count);
  } finally {
    await NodeFSP.rm(root, { recursive: true, force: true });
  }
});

it("cancels an in-flight transport and terminates its HTTP session when the relay closes", async () => {
  const root = await NodeFSP.realpath(
    await NodeFSP.mkdtemp(NodePath.join(NodeOS.tmpdir(), "claude-mcp-")),
  );
  try {
    const ledger = new NativeClaudeMcpBindings(NodePath.join(root, "bindings"));
    const prepared = await ledger.prepare({ config: config(), launcher });
    await ledger.bind({ prepared, nativeSessionId: A, config: config() });
    const methods: string[] = [];
    const fetchImplementation = async (_url: string, init?: RequestInit): Promise<Response> => {
      methods.push(init?.method ?? "GET");
      if (init?.method === "DELETE") return new Response(null, { status: 204 });
      const message = JSON.parse(String(init?.body)) as { method: string };
      if (message.method === "initialize")
        return Response.json({ result: {} }, { headers: { "mcp-session-id": "owned-session" } });
      return new Promise((_resolve, reject) => {
        const aborted = () => reject(new Error("aborted"));
        if (init?.signal?.aborted) aborted();
        else init?.signal?.addEventListener("abort", aborted, { once: true });
      });
    };
    const forward = makeNativeClaudeMcpFetch({
      launchDirectory: prepared.launchDirectory,
      nativeSessionId: A,
      fetchImplementation,
    });
    await forward("http://ignored", { body: JSON.stringify({ method: "initialize" }) });
    const result = forward("http://ignored", {
      body: JSON.stringify({ method: "tools/call" }),
    }).then(
      () => "completed",
      (error: unknown) => (error instanceof Error ? error.message : "unknown"),
    );
    await forward.close();
    expect(await result).toBe("aborted");
    expect(methods.filter((method) => method === "DELETE")).toHaveLength(1);
  } finally {
    await NodeFSP.rm(root, { recursive: true, force: true });
  }
});

it("accepts a daemon bound to its own tailnet address without forwarding to another host", () => {
  expect(() => assertClaudeMcpEndpoint("http://100.72.1.2:3773/mcp", ["100.72.1.2"])).not.toThrow();
  expect(() => assertClaudeMcpEndpoint("http://100.72.1.3:3773/mcp", ["100.72.1.2"])).toThrow(
    "host-local",
  );
  expect(() =>
    assertClaudeMcpEndpoint("http://[fd7a:115c:a1e0::1]:3773/mcp", ["fd7a:115c:a1e0::1"]),
  ).not.toThrow();
  expect(() => assertClaudeMcpEndpoint("https://127.evil.test/mcp", ["127.0.0.1"])).toThrow();
});
