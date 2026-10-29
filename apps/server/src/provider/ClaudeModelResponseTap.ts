// @effect-diagnostics nodeBuiltinImport:off -- Detached observer infrastructure uses Node streams.
import * as NodeZlib from "node:zlib";

/** Decode only the observer's copy; native response bytes and backpressure stay untouched. */
export function claudeModelResponseTap(input: {
  readonly encoding: string | undefined;
  readonly onData: (chunk: Buffer) => boolean;
  readonly onGap: (reason: string) => void;
  readonly onEnd: () => void;
}) {
  const encoding = input.encoding?.trim().toLowerCase();
  const decoder =
    encoding === "gzip"
      ? NodeZlib.createGunzip()
      : encoding === "br"
        ? NodeZlib.createBrotliDecompress()
        : encoding === "deflate"
          ? NodeZlib.createInflate()
          : undefined;
  let stopped = false;
  let ended = false;
  let decodedEnd = false;
  let finished = false;
  const finish = () => {
    if (!finished && ended && (stopped || !decoder || decodedEnd)) {
      finished = true;
      input.onEnd();
    }
  };
  const stop = (reason?: string) => {
    if (stopped) return;
    stopped = true;
    decoder?.destroy();
    if (reason) input.onGap(reason);
    finish();
  };
  if (encoding && encoding !== "identity" && !decoder) stop("unsupported-content-encoding");
  decoder?.on("data", (chunk: Buffer) => {
    if (!stopped && !input.onData(chunk)) stop();
  });
  decoder?.on("error", () => stop("invalid-compressed-response"));
  decoder?.on("end", () => {
    decodedEnd = true;
    finish();
  });
  return {
    write(chunk: Buffer) {
      if (stopped || ended) return;
      if (decoder) {
        decoder.write(chunk);
        // A slow observer must never hold up the actual model response or grow without bound.
        if (decoder.writableLength > 1024 * 1024) stop("compressed-observer-backlog");
      } else if (!input.onData(chunk)) stop();
    },
    end() {
      ended = true;
      if (!stopped && decoder) decoder.end();
      finish();
    },
    abort() {
      stopped = true;
      finished = true;
      decoder?.destroy();
    },
  };
}
