// @effect-diagnostics nodeBuiltinImport:off -- Real in-memory compression streams, without wall-clock waits.
import * as NodeZlib from "node:zlib";
import { expect, it } from "vite-plus/test";
import { claudeModelResponseTap } from "./ClaudeModelResponseTap.ts";

it.each([
  { encoding: "gzip", create: NodeZlib.createGzip, flush: NodeZlib.constants.Z_SYNC_FLUSH },
  {
    encoding: "br",
    create: NodeZlib.createBrotliCompress,
    flush: NodeZlib.constants.BROTLI_OPERATION_FLUSH,
  },
  { encoding: "deflate", create: NodeZlib.createDeflate, flush: NodeZlib.constants.Z_SYNC_FLUSH },
])(
  "delivers $encoding chunks before the original stream finishes",
  async ({ encoding, create, flush }) => {
    const chunks: Buffer[] = [];
    const gaps: string[] = [];
    let first!: () => void;
    const firstChunk = new Promise<void>((resolve) => {
      first = resolve;
    });
    let finish!: () => void;
    let ended = false;
    const done = new Promise<void>((resolve) => {
      finish = resolve;
    });
    const tap = claudeModelResponseTap({
      encoding,
      onData: (chunk) => {
        chunks.push(chunk);
        first();
        return true;
      },
      onGap: (reason) => gaps.push(reason),
      onEnd: () => {
        ended = true;
        finish();
      },
    });
    const encoder = create();
    encoder.on("data", (chunk) => tap.write(chunk));
    encoder.on("end", () => tap.end());
    try {
      encoder.write("正文🙂");
      encoder.flush(flush);
      await firstChunk;
      expect(Buffer.concat(chunks).toString()).toBe("正文🙂");
      expect(ended).toBe(false);
      encoder.end("思考");
      await done;
      expect(Buffer.concat(chunks).toString()).toBe("正文🙂思考");
      expect(gaps).toEqual([]);
    } finally {
      tap.abort();
      encoder.destroy();
    }
  },
);

it("reports corrupt compression once and completes observation without failing native forwarding", async () => {
  const gaps: string[] = [];
  const chunks: Buffer[] = [];
  let finish!: () => void;
  const ended = new Promise<void>((resolve) => {
    finish = resolve;
  });
  const tap = claudeModelResponseTap({
    encoding: "gzip",
    onData: (chunk) => {
      chunks.push(chunk);
      return true;
    },
    onGap: (reason) => gaps.push(reason),
    onEnd: () => finish(),
  });
  tap.write(Buffer.from("not gzip"));
  tap.end();
  await ended;
  expect(gaps).toEqual(["invalid-compressed-response"]);
  expect(chunks).toEqual([]);
});

it("bounds compressed observer backlog and never finishes an aborted capture", () => {
  const gaps: string[] = [];
  let endings = 0;
  const tap = claudeModelResponseTap({
    encoding: "gzip",
    onData: () => true,
    onGap: (reason) => gaps.push(reason),
    onEnd: () => endings++,
  });
  tap.write(Buffer.alloc(1024 * 1024 + 1));
  tap.write(Buffer.alloc(1024 * 1024 + 1));
  expect(gaps).toEqual(["compressed-observer-backlog"]);
  tap.abort();
  tap.end();
  expect(endings).toBe(0);
});

it("stops decoding when the parser reaches its own bound and reports unsupported encodings", () => {
  let chunks = 0;
  const gaps: string[] = [];
  let endings = 0;
  const plain = claudeModelResponseTap({
    encoding: "identity",
    onData: () => {
      chunks++;
      return false;
    },
    onGap: (reason) => gaps.push(reason),
    onEnd: () => endings++,
  });
  plain.write(Buffer.from("first"));
  plain.write(Buffer.from("ignored"));
  plain.end();
  const unsupported = claudeModelResponseTap({
    encoding: "gzip, br",
    onData: () => {
      throw new Error("Should not parse unsupported bytes");
    },
    onGap: (reason) => gaps.push(reason),
    onEnd: () => endings++,
  });
  unsupported.write(Buffer.from("original bytes"));
  unsupported.end();
  expect(chunks).toBe(1);
  expect(endings).toBe(2);
  expect(gaps).toEqual(["unsupported-content-encoding"]);
});
