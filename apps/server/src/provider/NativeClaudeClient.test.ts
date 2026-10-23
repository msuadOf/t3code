// @effect-diagnostics nodeBuiltinImport:off -- native UNIX socket fixture lives in an isolated temporary directory.
import * as NodeFSP from "node:fs/promises";
import * as NodeNet from "node:net";
import * as NodeOS from "node:os";
import * as NodePath from "node:path";

import { describe, expect, it } from "@effect/vitest";

import {
  NativeClaudeClient,
  decodeNativeClaudeInventory,
  nativeClaudeSessionIsInDirectory,
  requireNativeClaudeBackgroundId,
  type NativeClaudeSession,
} from "./NativeClaudeClient.ts";

const session: NativeClaudeSession = {
  kind: "background",
  cwd: "/work/project",
  startedAt: 1,
  id: "1234abcd",
  sessionId: "1234abcd-1234-1234-1234-123456789abc",
};

async function withDaemon(
  run: (socket: NodeNet.Socket, request: Record<string, unknown>) => void,
  test: (client: NativeClaudeClient) => Promise<void>,
) {
  const directory = await NodeFSP.mkdtemp(NodePath.join(NodeOS.tmpdir(), "t3-claude-"));
  const socketPath = NodePath.join(directory, "control.sock");
  await NodeFSP.mkdir(NodePath.join(directory, "daemon"));
  await NodeFSP.writeFile(NodePath.join(directory, "daemon", "control.key"), "test-control-key\n", {
    mode: 0o600,
  });
  const sockets = new Set<NodeNet.Socket>();
  const server = NodeNet.createServer((socket) => {
    sockets.add(socket);
    socket.once("close", () => sockets.delete(socket));
    let buffered = "";
    socket.on("data", (chunk) => {
      buffered += chunk.toString();
      const newline = buffered.indexOf("\n");
      if (newline >= 0) {
        socket.removeAllListeners("data");
        run(socket, JSON.parse(buffered.slice(0, newline)) as Record<string, unknown>);
      }
    });
  });
  await new Promise<void>((resolve, reject) => {
    server.once("error", reject);
    server.listen(socketPath, resolve);
  });
  try {
    await test(new NativeClaudeClient({ socketPath, configDirectory: directory }));
  } finally {
    for (const socket of sockets) socket.destroy();
    await new Promise<void>((resolve) => server.close(() => resolve()));
    await NodeFSP.rm(directory, { recursive: true, force: true });
  }
}

describe("Claude native shared-session transport", () => {
  it("rejects foreground sessions instead of opening an independent resume writer", () => {
    expect(() => requireNativeClaudeBackgroundId({ ...session, kind: "interactive" })).toThrow(
      "/background",
    );
    expect(() => decodeNativeClaudeInventory([{ kind: "background", cwd: 4 }])).toThrow(
      "invalid session",
    );
  });

  it("filters directories at path boundaries", () => {
    expect(nativeClaudeSessionIsInDirectory(session, "/work")).toBe(true);
    expect(nativeClaudeSessionIsInDirectory({ ...session, cwd: "/work-other" }, "/work")).toBe(
      false,
    );
  });

  it("sends authenticated input to the existing daemon worker", async () => {
    let received: Record<string, unknown> | undefined;
    await withDaemon(
      (socket, request) => {
        received = request;
        socket.end('{"ok":true,"op":"reply"}\n');
      },
      async (client) => {
        await client.reply(session, "continue this same worker");
      },
    );
    expect(received).toEqual({
      proto: 1,
      op: "reply",
      short: session.id,
      text: "continue this same worker",
      auth: "test-control-key",
    });
  });

  it("propagates native rejection rather than reporting a successful send", async () => {
    await withDaemon(
      (socket) => socket.end('{"ok":false,"code":"ENOJOB","error":"job not found"}\n'),
      async (client) => {
        await expect(client.reply(session, "hello")).rejects.toMatchObject({ code: "ENOJOB" });
      },
    );
  });

  it("keeps subscribe terminal output separate from structured agent deltas", async () => {
    await withDaemon(
      (socket) => {
        socket.write('{"type":"snap');
        socket.end(
          'shot","record":{},"streamTail":[]}\n{"type":"stream","line":"\\u001b[32mterminal"}\n',
        );
      },
      async (client) => {
        const frames = [];
        for await (const frame of client.subscribe(session)) frames.push(frame);
        expect(frames.map((frame) => frame.type)).toEqual(["snapshot", "stream"]);
        expect(frames[1]?.line).toBe("\u001b[32mterminal");
      },
    );
  });

  it("preserves PTY bytes coalesced with the attach acknowledgement", async () => {
    await withDaemon(
      (socket) => socket.end('{"ok":true,"op":"attach"}\n\u001b[32mshared terminal'),
      async (client) => {
        const terminal = await client.attach(session, { cols: 80, rows: 24 });
        const chunks = [];
        for await (const chunk of terminal) chunks.push(Buffer.from(chunk));
        expect(Buffer.concat(chunks).toString()).toBe("\u001b[32mshared terminal");
      },
    );
  });
});

it("uses the exact --bg short ID to bind Claude's assigned UUID and closes stdin", async () => {
  const directory = await NodeFSP.mkdtemp(NodePath.join(NodeOS.tmpdir(), "t3-claude-launch-"));
  const binary = NodePath.join(directory, "fake-claude");
  const inventory = JSON.stringify([
    { ...session, status: "idle" },
    { ...session, id: "ffffffff", sessionId: "ffffffff-1234-1234-1234-123456789abc" },
  ]);
  await NodeFSP.writeFile(
    binary,
    `#!/bin/sh
if [ "$1" = "--bg" ]; then
  for arg in "$@"; do [ "$arg" = "--session-id" ] && exit 41; done
  read input && exit 42
  printf 'backgrounded · 1234abcd\\n'
else
  printf '%s' '${inventory}'
fi
`,
    { mode: 0o700 },
  );
  try {
    const client = new NativeClaudeClient({ binaryPath: binary });
    const created = await client.startBackground({ cwd: directory });
    expect(created.sessionId).toBe(session.sessionId);
    expect(created.id).toBe("1234abcd");
  } finally {
    await NodeFSP.rm(directory, { recursive: true, force: true });
  }
});
