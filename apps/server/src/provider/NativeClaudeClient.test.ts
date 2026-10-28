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

  it("interrupts the same busy worker through a disposable native terminal", async () => {
    let received: Record<string, unknown> | undefined;
    let resolveInput!: (input: Buffer) => void;
    const input = new Promise<Buffer>((resolve) => {
      resolveInput = resolve;
    });
    const busy = { ...session, pid: 42, status: "busy" };
    await withDaemon(
      (socket, request) => {
        received = request;
        socket.once("data", resolveInput);
        socket.write('{"ok":true,"op":"attach"}\n');
      },
      async (client) => {
        client.listSessions = async () => [busy];
        await client.interrupt(busy);
        expect(await input).toEqual(Buffer.from([3]));
      },
    );
    expect(received).toEqual({
      proto: 1,
      op: "attach",
      short: session.id,
      auth: "test-control-key",
      cols: 80,
      rows: 24,
    });
  });

  it("does not send terminal control to an idle worker", async () => {
    let attached = false;
    await withDaemon(
      (socket) => {
        attached = true;
        socket.end('{"ok":true,"op":"attach"}\n');
      },
      async (client) => {
        const idle = { ...session, pid: 42, status: "idle" };
        client.listSessions = async () => [idle];
        await expect(client.interrupt(idle)).rejects.toMatchObject({ code: "EIDLE" });
      },
    );
    expect(attached).toBe(false);
  });

  it("does not interrupt a replacement worker after terminal attachment", async () => {
    let written = false;
    const busy = { ...session, pid: 42, status: "busy" };
    await withDaemon(
      (socket) => {
        socket.on("data", () => {
          written = true;
        });
        socket.write('{"ok":true,"op":"attach"}\n');
      },
      async (client) => {
        let reads = 0;
        client.listSessions = async () => [{ ...busy, pid: ++reads === 1 ? 42 : 43 }];
        await expect(client.interrupt(busy)).rejects.toMatchObject({ code: "EOWNER" });
      },
    );
    expect(written).toBe(false);
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

  it("subscribes to live state without terminal replay and cancels the socket", async () => {
    let closed: Promise<void> | undefined;
    await withDaemon(
      (socket, request) => {
        expect(request.tail).toBe(0);
        expect(request.op).toBe("subscribe");
        closed = new Promise((resolve) => socket.once("close", resolve));
        socket.write(
          '{"type":"snapshot","record":{}}\n{"type":"stream","line":"terminal bytes"}\n{"type":"state","patch":{"tempo":"active"}}\n',
        );
      },
      async (client) => {
        const controller = new AbortController();
        const reader = client.subscribeState(session, controller.signal);
        expect((await reader.next()).value?.type).toBe("snapshot");
        expect((await reader.next()).value).toEqual({ type: "state", patch: { tempo: "active" } });
        controller.abort();
        await reader.return(undefined);
        await closed;
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

async function withLauncher(
  script: (directory: string) => string,
  test: (client: NativeClaudeClient, directory: string) => Promise<void>,
) {
  const directory = await NodeFSP.mkdtemp(NodePath.join(NodeOS.tmpdir(), "t3-claude-claim-"));
  const binary = NodePath.join(directory, "fake-claude");
  await NodeFSP.writeFile(binary, `#!/bin/sh\n${script(directory)}`, { mode: 0o700 });
  try {
    await test(new NativeClaudeClient({ binaryPath: binary }), directory);
  } finally {
    await NodeFSP.rm(directory, { recursive: true, force: true });
  }
}

describe("Claude canonical history claim", () => {
  it("resumes an inactive full UUID without model or other overrides that would fork saved options", async () => {
    await withLauncher(
      (directory) => `
if [ "$1" = "agents" ]; then
  if [ -f '${directory}/ready' ]; then printf '%s' '${JSON.stringify([{ ...session, status: "idle" }])}'; else printf '[]'; fi
elif [ "$1" = "--bg" ]; then
  [ "$#" = "3" ] && [ "$2" = "--resume" ] && [ "$3" = '${session.sessionId}' ] || exit 41
  touch '${directory}/ready'
  printf 'backgrounded · 1234abcd (idle — send a prompt to start)\n'
else exit 42
fi
`,
      async (client, directory) => {
        const resumed = await client.startBackground({
          cwd: directory,
          sessionId: session.sessionId!,
          resume: true,
          model: "must-not-override-saved-options",
          permissionMode: "default",
        });
        expect(resumed.sessionId).toBe(session.sessionId);
      },
    );
  });

  it("wakes a stopped job with a disposable non-forcing CLI attach and retains its UUID", async () => {
    await withLauncher(
      (directory) => `
if [ "$1" = "agents" ]; then
  if [ -f '${directory}/ready' ]; then printf '%s' '${JSON.stringify([{ ...session, status: "idle", state: "working", pid: 42 }])}'; else printf '%s' '${JSON.stringify([{ ...session, state: "stopped" }])}'; fi
elif [ "$1" = "attach" ] && [ "$2" = "1234abcd" ]; then
  touch '${directory}/ready'
  while read input; do :; done
else exit 42
fi
`,
      async (client, directory) => {
        const resumed = await client.startBackground({
          cwd: directory,
          sessionId: session.sessionId!,
          resume: true,
        });
        expect(resumed).toMatchObject({
          sessionId: session.sessionId,
          id: session.id,
          pid: 42,
          state: "working",
        });
      },
    );
  });

  it("stops a native fallback copy without attaching or sending to a different UUID", async () => {
    const copied = {
      ...session,
      id: "ffffffff",
      sessionId: "ffffffff-1234-1234-1234-123456789abc",
      status: "idle",
    };
    await withLauncher(
      (directory) => `
printf '%s\n' "$*" >> '${directory}/calls'
if [ "$1" = "agents" ]; then
  if [ -f '${directory}/ready' ]; then printf '%s' '${JSON.stringify([copied])}'; else printf '[]'; fi
elif [ "$1" = "--bg" ]; then
  touch '${directory}/ready'
  printf 'backgrounded · ffffffff (idle — send a prompt to start)\n'
elif [ "$1" = "stop" ] && [ "$2" = "ffffffff" ]; then exit 0
else exit 42
fi
`,
      async (client, directory) => {
        await expect(
          client.startBackground({ cwd: directory, sessionId: session.sessionId!, resume: true }),
        ).rejects.toMatchObject({ code: "ESESSIONCOPY" });
        const calls = await NodeFSP.readFile(NodePath.join(directory, "calls"), "utf8");
        expect(calls).toContain("stop ffffffff");
        expect(calls).not.toContain("attach");
        expect(calls).not.toContain("reply");
      },
    );
  });
});

describe("explicit native model activity observation", () => {
  it("refuses to upgrade a running unbound worker without preparing settings or launching", async () => {
    let prepared = 0;
    await withLauncher(
      () => `
if [ "$1" = "agents" ]; then printf '%s' '${JSON.stringify([{ ...session, status: "idle" }])}'; else exit 41; fi
`,
      async (_client, directory) => {
        const client = new NativeClaudeClient({
          binaryPath: NodePath.join(directory, "fake-claude"),
          modelActivity: {
            prepare: async () => {
              prepared++;
              throw new Error("must not prepare");
            },
            isBound: async () => false,
            bind: async () => {
              throw new Error("must not bind");
            },
          },
        });
        await expect(
          client.startBackground({
            cwd: directory,
            sessionId: session.sessionId!,
            resume: true,
            observeModelActivity: true,
          }),
        ).rejects.toMatchObject({ code: "EOBSERVERUPGRADE" });
        expect(prepared).toBe(0);
      },
    );
  });

  it("reuses a bound worker without changing its launch or observer settings", async () => {
    const recovered: string[] = [];
    await withLauncher(
      () => `
if [ "$1" = "agents" ]; then printf '%s' '${JSON.stringify([{ ...session, status: "idle" }])}'; else exit 41; fi
`,
      async (_client, directory) => {
        const client = new NativeClaudeClient({
          binaryPath: NodePath.join(directory, "fake-claude"),
          modelActivity: {
            prepare: async () => {
              throw new Error("must not prepare");
            },
            isBound: async () => true,
            recoverBound: async (id) => {
              recovered.push(id);
            },
            bind: async () => {
              throw new Error("must not bind again");
            },
          },
        });
        expect(
          (
            await client.startBackground({
              cwd: directory,
              sessionId: session.sessionId!,
              resume: true,
              observeModelActivity: true,
            })
          ).sessionId,
        ).toBe(session.sessionId);
      },
    );
    expect(recovered).toEqual([session.sessionId]);
  });

  it("binds only the verified historical UUID and passes only a settings path in argv", async () => {
    let binding: string | undefined;
    const prepared = {
      settingsPath: "/observer/settings.json",
      observerStateDir: "/private/state",
      upstreamUrl: "https://private-upstream.invalid/token-secret",
    };
    await withLauncher(
      (directory) => `
if [ "$1" = "agents" ]; then
 if [ -f '${directory}/ready' ]; then printf '%s' '${JSON.stringify([{ ...session, status: "idle" }])}'; else printf '[]'; fi
elif [ "$1" = "--bg" ]; then
 printf '%s\\n' "$@" > '${directory}/argv'
 touch '${directory}/ready'; printf 'backgrounded · 1234abcd\\n'
else exit 41; fi
`,
      async (_client, directory) => {
        const client = new NativeClaudeClient({
          binaryPath: NodePath.join(directory, "fake-claude"),
          modelActivity: {
            prepare: async () => prepared,
            isBound: async () => false,
            bind: async (id, value) => {
              expect(value).toEqual(prepared);
              binding = id;
            },
          },
        });
        await client.startBackground({
          cwd: directory,
          sessionId: session.sessionId!,
          resume: true,
          observeModelActivity: true,
        });
        expect(binding).toBe(session.sessionId);
        expect(
          (await NodeFSP.readFile(NodePath.join(directory, "argv"), "utf8")).trim().split("\n"),
        ).toEqual(["--bg", "--settings", prepared.settingsPath, "--resume", session.sessionId]);
      },
    );
  });

  it("never prepares observation settings for default launches", async () => {
    await withLauncher(
      () => `printf '%s' '${JSON.stringify([{ ...session, status: "idle" }])}'`,
      async (_client, directory) => {
        const client = new NativeClaudeClient({
          binaryPath: NodePath.join(directory, "fake-claude"),
          modelActivity: {
            prepare: async () => {
              throw new Error("default must not prepare");
            },
            isBound: async () => {
              throw new Error("default must not inspect binding");
            },
            bind: async () => {
              throw new Error("default must not bind");
            },
          },
        });
        expect(
          (
            await client.startBackground({
              cwd: directory,
              sessionId: session.sessionId!,
              resume: true,
            })
          ).sessionId,
        ).toBe(session.sessionId);
      },
    );
  });
});

it("does not bind an observer when the native historical claim forks a different UUID", async () => {
  let bindings = 0;
  await withLauncher(
    (directory) => `
if [ "$1" = "agents" ]; then
 if [ -f '${directory}/ready' ]; then printf '%s' '${JSON.stringify([{ ...session, sessionId: "aaaaaaaa-1234-1234-1234-123456789abc", status: "idle" }])}'; else printf '[]'; fi
elif [ "$1" = "--bg" ]; then touch '${directory}/ready'; printf 'backgrounded · 1234abcd\\n'
elif [ "$1" = "stop" ]; then touch '${directory}/stopped'; else exit 41; fi
`,
    async (_client, directory) => {
      const client = new NativeClaudeClient({
        binaryPath: NodePath.join(directory, "fake-claude"),
        modelActivity: {
          prepare: async () => ({
            settingsPath: "/observer/settings.json",
            observerStateDir: "/observer",
            upstreamUrl: "https://upstream.invalid",
          }),
          isBound: async () => false,
          bind: async () => {
            bindings++;
          },
        },
      });
      await expect(
        client.startBackground({
          cwd: directory,
          sessionId: session.sessionId!,
          resume: true,
          observeModelActivity: true,
        }),
      ).rejects.toMatchObject({ code: "ESESSIONCOPY" });
      expect(bindings).toBe(0);
      expect((await NodeFSP.stat(NodePath.join(directory, "stopped"))).isFile()).toBe(true);
    },
  );
});

it.each([true, false])(
  "ordinary reopen recovers only an already-bound route (%s)",
  async (bound) => {
    const recovered: string[] = [];
    await withLauncher(
      () => `
if [ "$1" = "agents" ]; then printf '%s' '${JSON.stringify([{ ...session, status: "idle" }])}'; else exit 41; fi
`,
      async (_client, directory) => {
        const client = new NativeClaudeClient({
          binaryPath: NodePath.join(directory, "fake-claude"),
          modelActivity: {
            prepare: async () => {
              throw new Error("reopen must not prepare a new route");
            },
            isBound: async () => bound,
            recoverBound: async (id) => {
              recovered.push(id);
            },
            bind: async () => {
              throw new Error("reopen must not bind a new route");
            },
          },
        });
        expect(
          (
            await client.startBackground({
              cwd: directory,
              sessionId: session.sessionId!,
              resume: true,
            })
          ).sessionId,
        ).toBe(session.sessionId);
      },
    );
    expect(recovered).toEqual(bound ? [session.sessionId] : []);
  },
);

describe("owned model activity lifecycle changes", () => {
  it("stops an idle owned worker, changes private settings, and wakes the same UUID for enable and disable", async () => {
    await withLauncher(
      (directory) => `
printf '%s\\n' "$*" >> '${directory}/calls'
if [ "$1" = "agents" ]; then cat '${directory}/inventory'
elif [ "$1" = "stop" ] && [ "$2" = "1234abcd" ]; then printf '%s' '${JSON.stringify([{ ...session, cwd: directory, state: "stopped" }])}' > '${directory}/inventory'
elif [ "$1" = "attach" ] && [ "$2" = "1234abcd" ]; then
  printf '%s' '${JSON.stringify([{ ...session, cwd: directory, status: "idle", state: "working", pid: 43 }])}' > '${directory}/inventory'
  while read input; do :; done
else exit 42
fi
`,
      async (_client, directory) => {
        await NodeFSP.writeFile(
          NodePath.join(directory, "inventory"),
          JSON.stringify([
            { ...session, cwd: directory, status: "idle", state: "working", pid: 42 },
          ]),
        );
        const changed: boolean[] = [];
        const client = new NativeClaudeClient({
          binaryPath: NodePath.join(directory, "fake-claude"),
          modelActivity: {
            prepare: async () => {
              throw new Error("must retain original launch");
            },
            bind: async () => {
              throw new Error("must retain binding");
            },
            isBound: async () => false,
            hasBinding: async () => true,
            setEnabled: async (id, enabled) => {
              expect(id).toBe(session.sessionId);
              const inventory = JSON.parse(
                await NodeFSP.readFile(NodePath.join(directory, "inventory"), "utf8"),
              );
              expect(inventory[0].state).toBe("stopped");
              changed.push(enabled);
            },
          },
        });
        expect((await client.setModelActivityEnabled(session.sessionId!, false)).sessionId).toBe(
          session.sessionId,
        );
        expect((await client.setModelActivityEnabled(session.sessionId!, true)).sessionId).toBe(
          session.sessionId,
        );
        expect(changed).toEqual([false, true]);
        const calls = await NodeFSP.readFile(NodePath.join(directory, "calls"), "utf8");
        expect(calls.match(/stop 1234abcd/g)).toHaveLength(2);
        expect(calls).not.toContain("--bg");
        expect(calls).not.toContain("--resume");
        expect(calls).not.toContain("reply");
      },
    );
  });
  it("refuses unbound, busy and racing owners without stopping them", async () => {
    await withLauncher(
      (directory) => `printf '%s\\n' "$*" >> '${directory}/calls'; exit 42`,
      async (_client, directory) => {
        let bound = false;
        let mode = "idle";
        let reads = 0;
        class Client extends NativeClaudeClient {
          override async listSessions(): Promise<readonly NativeClaudeSession[]> {
            reads++;
            return [
              {
                ...session,
                pid: 42,
                status: mode === "race" && reads === 2 ? "busy" : mode === "race" ? "idle" : mode,
              },
            ];
          }
        }
        let changes = 0;
        const client = new Client({
          binaryPath: NodePath.join(directory, "fake-claude"),
          modelActivity: {
            prepare: async () => {
              throw new Error("not used");
            },
            bind: async () => {},
            isBound: async () => false,
            hasBinding: async () => bound,
            setEnabled: async () => {
              changes++;
            },
          },
        });
        await expect(client.setModelActivityEnabled("bad", false)).rejects.toMatchObject({
          code: "ESESSION",
        });
        await expect(
          client.setModelActivityEnabled(session.sessionId!, false),
        ).rejects.toMatchObject({ code: "EOBSERVERUNAVAILABLE" });
        bound = true;
        mode = "busy";
        await expect(
          client.setModelActivityEnabled(session.sessionId!, false),
        ).rejects.toMatchObject({ code: "EBUSY" });
        mode = "waiting";
        await expect(
          client.setModelActivityEnabled(session.sessionId!, false),
        ).rejects.toMatchObject({ code: "EBUSY" });
        mode = "race";
        reads = 0;
        await expect(
          client.setModelActivityEnabled(session.sessionId!, false),
        ).rejects.toMatchObject({ code: "EBUSY" });
        expect(changes).toBe(0);
        await expect(NodeFSP.stat(NodePath.join(directory, "calls"))).rejects.toMatchObject({
          code: "ENOENT",
        });
      },
    );
  });
  it("keeps a stopped worker stopped if its settings callback fails", async () => {
    await withLauncher(
      () => "exit 42",
      async (_client, directory) => {
        class Client extends NativeClaudeClient {
          override async listSessions(): Promise<readonly NativeClaudeSession[]> {
            return [{ ...session, cwd: directory, state: "stopped" }];
          }
        }
        const client = new Client({
          binaryPath: NodePath.join(directory, "fake-claude"),
          modelActivity: {
            prepare: async () => {
              throw new Error("not used");
            },
            bind: async () => {},
            isBound: async () => false,
            hasBinding: async () => true,
            setEnabled: async () => {
              throw new Error("private failure");
            },
          },
        });
        await expect(
          client.setModelActivityEnabled(session.sessionId!, false),
        ).rejects.toMatchObject({ code: "EOBSERVERSETTINGS" });
      },
    );
  });
});

it.each(["busy", "missing"] as const)(
  "does not edit settings when the owner becomes %s after stop",
  async (mode) => {
    let changes = 0;
    await withLauncher(
      (directory) => `
if [ "$1" = "agents" ]; then
  if [ -f '${directory}/stopped' ]; then printf '%s' '${JSON.stringify(mode === "busy" ? [{ ...session, cwd: directory, status: "busy", pid: 99 }] : [])}'; else printf '%s' '${JSON.stringify([{ ...session, cwd: directory, status: "idle", pid: 42 }])}'; fi
elif [ "$1" = "stop" ]; then touch '${directory}/stopped'
else exit 42
fi
`,
      async (_client, directory) => {
        const client = new NativeClaudeClient({
          binaryPath: NodePath.join(directory, "fake-claude"),
          modelActivity: {
            prepare: async () => {
              throw new Error("not used");
            },
            bind: async () => {},
            isBound: async () => false,
            hasBinding: async () => true,
            setEnabled: async () => {
              changes++;
            },
          },
        });
        await expect(
          client.setModelActivityEnabled(session.sessionId!, false),
        ).rejects.toMatchObject({ code: mode === "busy" ? "EBUSY" : "EOBSERVERSTATE" });
        expect(changes).toBe(0);
      },
    );
  },
);

it.each(["idle", "stopped"] as const)(
  "does not stop or edit an externally replaced %s owner after binding ownership changes",
  async (state) => {
    await withLauncher(
      (directory) => `printf '%s\\n' "$*" >> '${directory}/calls'; exit 42`,
      async (_client, directory) => {
        let checks = 0;
        let changes = 0;
        class Client extends NativeClaudeClient {
          override async listSessions(): Promise<readonly NativeClaudeSession[]> {
            return [
              {
                ...session,
                cwd: directory,
                pid: 77,
                status: "idle",
                state: state === "stopped" ? "stopped" : "working",
              },
            ];
          }
        }
        const client = new Client({
          binaryPath: NodePath.join(directory, "fake-claude"),
          modelActivity: {
            prepare: async () => {
              throw new Error("must not prepare");
            },
            bind: async () => {},
            isBound: async () => false,
            hasBinding: async () => ++checks === 1,
            setEnabled: async () => {
              changes++;
            },
          },
        });
        await expect(
          client.setModelActivityEnabled(session.sessionId!, false),
        ).rejects.toMatchObject({ code: "EOBSERVERSTATE" });
        expect(checks).toBe(2);
        expect(changes).toBe(0);
        await expect(NodeFSP.stat(NodePath.join(directory, "calls"))).rejects.toMatchObject({
          code: "ENOENT",
        });
      },
    );
  },
);
