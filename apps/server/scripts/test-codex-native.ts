/** Manual isolated native protocol check: node scripts/test-codex-native.ts /path/to/codex */
import * as NodeAssert from "node:assert/strict";
import * as NodeChildProcess from "node:child_process";
import * as NodeEvents from "node:events";
import * as NodeFSP from "node:fs/promises";
import * as NodeHttp from "node:http";
import * as NodePath from "node:path";
import * as NodeTimersPromises from "node:timers/promises";
import { NodeWS } from "@effect/platform-node/NodeSocket";

type Frame = { id?: string | number; method?: string; params?: any; result?: any; error?: any };
const binary = process.argv[2];
NodeAssert.ok(binary, "Usage: node scripts/test-codex-native.ts /path/to/codex");
const directory = await NodeFSP.mkdtemp("/tmp/t3-codex-native-check-");
const home = NodePath.join(directory, "home");
const workspace = NodePath.join(directory, "work");
await NodeFSP.mkdir(home);
await NodeFSP.mkdir(workspace);
const socketPath = NodePath.join(directory, "engine.sock");
const clients: NativeClient[] = [];
const connections = new Set<NodeHttp.ServerResponse>();
let releaseStream: (() => void) | undefined;
let modelRequests = 0;
let modelMode: "stream" | "answer" | "approval" = "stream";
const server = NodeHttp.createServer(async (request, response) => {
  connections.add(response);
  response.on("close", () => connections.delete(response));
  const chunks: Buffer[] = [];
  for await (const chunk of request) chunks.push(Buffer.from(chunk));
  if (!request.url?.endsWith("/responses")) {
    response.writeHead(404).end();
    return;
  }
  const body = JSON.parse(Buffer.concat(chunks).toString());
  modelRequests += 1;
  const id = `resp_${modelRequests}`;
  const itemId = `msg_${modelRequests}`;
  response.writeHead(200, { "Content-Type": "text/event-stream" });
  const emit = (event: any) =>
    response.write(`event: ${event.type}\ndata: ${JSON.stringify(event)}\n\n`);
  emit({ type: "response.created", response: { id } });
  if (modelMode === "approval") {
    modelMode = "answer";
    const tool = body.tools.find((tool: any) => tool.name.endsWith("exec_command"));
    if (!tool)
      throw new Error(
        `Mock expected exec_command tool: ${body.tools.map((tool: any) => tool.name)}`,
      );
    emit({
      type: "response.output_item.done",
      item: {
        type: "function_call",
        id: "fc_approval",
        call_id: "call_approval",
        name: tool.name,
        arguments: JSON.stringify({
          cmd: "echo NATIVE_APPROVAL_CHECK",
          sandbox_permissions: "require_escalated",
          justification: "Isolated test; approve or deny this harmless command.",
        }),
      },
    });
  } else {
    emit({
      type: "response.output_item.added",
      output_index: 0,
      item: { type: "message", id: itemId, role: "assistant", content: [] },
    });
    emit({
      type: "response.content_part.added",
      item_id: itemId,
      output_index: 0,
      content_index: 0,
      part: { type: "output_text", text: "" },
    });
    emit({
      type: "response.output_text.delta",
      item_id: itemId,
      output_index: 0,
      content_index: 0,
      delta: "BEFORE",
    });
    if (modelMode === "stream")
      await new Promise<void>((resolve) => {
        releaseStream = resolve;
      });
    emit({
      type: "response.output_text.delta",
      item_id: itemId,
      output_index: 0,
      content_index: 0,
      delta: " AFTER",
    });
    emit({
      type: "response.output_item.done",
      output_index: 0,
      item: {
        type: "message",
        id: itemId,
        role: "assistant",
        content: [{ type: "output_text", text: "BEFORE AFTER" }],
      },
    });
  }
  emit({
    type: "response.completed",
    response: { id, usage: { input_tokens: 12, output_tokens: 3, total_tokens: 15 } },
  });
  response.end();
});
await new Promise<void>((resolve) => server.listen(0, "127.0.0.1", resolve));
const address = server.address();
NodeAssert.ok(address && typeof address !== "string");
await NodeFSP.writeFile(
  NodePath.join(home, "config.toml"),
  `model = "mock-model"
model_provider = "isolated_mock"
approval_policy = "on-request"
sandbox_mode = "read-only"
check_for_update_on_startup = false
[model_providers.isolated_mock]
name = "Isolated native protocol test"
base_url = "http://127.0.0.1:${address.port}/v1"
wire_api = "responses"
requires_openai_auth = false
supports_websockets = false
request_max_retries = 0
stream_max_retries = 0
[analytics]
enabled = false
[feedback]
enabled = false
[projects."${workspace}"]
trust_level = "trusted"
`,
);
let stderr = "";
const child = NodeChildProcess.spawn(binary, ["app-server", "--listen", `unix://${socketPath}`], {
  cwd: workspace,
  env: {
    PATH: process.env.PATH,
    HOME: home,
    CODEX_HOME: home,
    TMPDIR: directory,
    LANG: "en_US.UTF-8",
  },
  stdio: ["ignore", "ignore", "pipe"],
});
child.stderr.on("data", (chunk) => {
  stderr = (stderr + chunk.toString()).slice(-12_000);
});

class NativeClient {
  socket: InstanceType<typeof NodeWS.WebSocket>;
  frames: Frame[] = [];
  pending = new Map<number, { resolve: (value: any) => void; reject: (error: Error) => void }>();
  nextId = 0;
  constructor() {
    this.socket = new NodeWS.WebSocket(`ws+unix://${socketPath}:/`, {
      perMessageDeflate: false,
      handshakeTimeout: 1000,
    });
    this.socket.on("message", (data) => {
      const frame = JSON.parse(data.toString()) as Frame;
      this.frames.push(frame);
      if (!frame.method && typeof frame.id === "number") {
        const pending = this.pending.get(frame.id);
        if (pending) {
          this.pending.delete(frame.id);
          if (frame.error) pending.reject(new Error(JSON.stringify(frame.error)));
          else pending.resolve(frame.result);
        }
      }
    });
  }
  async request(method: string, params: any) {
    const id = ++this.nextId;
    const result = new Promise<any>((resolve, reject) => this.pending.set(id, { resolve, reject }));
    this.socket.send(JSON.stringify({ id, method, params }));
    let timer: ReturnType<typeof setTimeout> | undefined;
    try {
      return await Promise.race([
        result,
        new Promise<never>((_, reject) => {
          timer = setTimeout(() => reject(new Error(`Timeout: ${method}`)), 15_000);
        }),
      ]);
    } finally {
      clearTimeout(timer);
      this.pending.delete(id);
    }
  }
  async wait(predicate: (frame: Frame) => boolean, after = 0) {
    for (let i = 0; i < 300; i++) {
      const match = this.frames.slice(after).find(predicate);
      if (match) return match;
      await NodeTimersPromises.setTimeout(50);
    }
    throw new Error(
      `Notification timeout; methods: ${this.frames
        .slice(after)
        .map((frame) => frame.method)
        .join(",")}`,
    );
  }
}
async function connect() {
  const client = new NativeClient();
  await NodeEvents.once(client.socket, "open");
  clients.push(client);
  await client.request("initialize", {
    clientInfo: { name: "t3_native_isolated_test", version: "1" },
    capabilities: { experimentalApi: true },
  });
  client.socket.send(JSON.stringify({ method: "initialized" }));
  return client;
}
const resume = (client: NativeClient, threadId: string) =>
  client.request("thread/resume", {
    threadId,
    excludeTurns: true,
    initialTurnsPage: { limit: 30, sortDirection: "desc", itemsView: "full" },
  });
try {
  let first: NativeClient | undefined;
  for (let i = 0; i < 100 && !first; i++) {
    try {
      first = await connect();
    } catch {
      await NodeTimersPromises.setTimeout(50);
    }
  }
  NodeAssert.ok(first, `App server did not start: ${stderr}`);
  const second = await connect();
  const started = await first.request("thread/start", { cwd: workspace });
  const threadId = started.thread.id;
  await first.request("turn/start", {
    threadId,
    input: [{ type: "text", text: "Reply using the mock stream.", text_elements: [] }],
  });
  const delta = await first.wait(
    (frame) => frame.method === "item/agentMessage/delta" && frame.params.delta === "BEFORE",
  );
  const resumed = await resume(second, threadId);
  const active = resumed.initialTurnsPage.data.find((turn: any) => turn.id === delta.params.turnId);
  NodeAssert.ok(active, "Resume snapshot must include the running native turn");
  const text = active.items
    .filter((item: any) => item.type === "agentMessage")
    .map((item: any) => item.text)
    .join("");
  // Current native versions may omit the unfinished assistant item from this
  // snapshot. Its final item/completed restores the complete text below.
  NodeAssert.ok(["", "BEFORE"].includes(text), "Unexpected active assistant snapshot");
  const boundary = second.frames.length;
  releaseStream?.();
  await second.wait((frame) => frame.method === "turn/completed", boundary);
  const after = second.frames
    .slice(boundary)
    .filter((frame) => frame.method === "item/agentMessage/delta")
    .map((frame) => frame.params.delta)
    .join("");
  NodeAssert.equal(after, " AFTER", "Post-resume stream must not replay earlier deltas");
  const completed = second.frames
    .slice(boundary)
    .find(
      (frame) => frame.method === "item/completed" && frame.params.item.type === "agentMessage",
    );
  NodeAssert.equal(
    completed?.params.item.text,
    "BEFORE AFTER",
    "Final native item replaces partial display with complete text",
  );
  console.log(
    "PASS: same native running turn, post-resume streaming, final item restores exact complete text",
  );
  if (!text)
    console.log(
      "LIMIT: native resume omits unfinished assistant prefix; it becomes available at item/completed",
    );
  modelMode = "answer";
  await NodeEvents.once((second.socket.close(), second.socket), "close");
  const firstBoundary = first.frames.length;
  await first.request("turn/start", {
    threadId,
    input: [
      { type: "text", text: "Continue after the other client disconnects.", text_elements: [] },
    ],
  });
  await first.wait((frame) => frame.method === "turn/completed", firstBoundary);
  NodeAssert.equal(child.exitCode, null);
  console.log("PASS: disconnecting client B leaves client A and its engine running");
  const third = await connect();
  await resume(third, threadId);
  modelMode = "approval";
  const approvalBoundary = first.frames.length;
  await first.request("turn/start", {
    threadId,
    input: [{ type: "text", text: "Test an approval callback.", text_elements: [] }],
  });
  const approval = await third.wait(
    (frame) => frame.method === "item/commandExecution/requestApproval",
  );
  third.socket.send(JSON.stringify({ id: approval.id, result: { decision: "cancel" } }));
  await first.wait(
    (frame) => frame.method === "serverRequest/resolved" && frame.params.requestId === approval.id,
    approvalBoundary,
  );
  await first.wait((frame) => frame.method === "turn/completed", approvalBoundary);
  console.log(
    "PASS: client B answers a tool approval; client A receives native resolved and terminal events",
  );
  console.log(`PASS: ${modelRequests} local mock model requests; isolated home removed on exit`);
} catch (error) {
  console.error(stderr);
  throw error;
} finally {
  releaseStream?.();
  for (const client of clients) client.socket.terminate();
  child.kill("SIGTERM");
  await Promise.race([NodeEvents.once(child, "exit"), NodeTimersPromises.setTimeout(2000)]);
  if (child.exitCode === null && child.signalCode === null) child.kill("SIGKILL");
  for (const response of connections) response.destroy();
  await new Promise<void>((resolve) => server.close(() => resolve()));
  await NodeFSP.rm(directory, { recursive: true, force: true });
}
