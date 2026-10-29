import * as Effect from "effect/Effect";

const LIMIT = 64 * 1024;

export interface ShellOutputCursor {
  cursor: number | undefined;
  truncated?: boolean;
  replace?: boolean;
}

export interface ShellOutputPage {
  readonly output: string;
  readonly cursor: number;
  readonly size: number;
  readonly truncated: boolean;
}

/** A single serial reader; caller owns its fiber and retains state across reconnects. */
export const tailShellOutput = Effect.fn("OpenCode.tailShellOutput")(
  function* <E>(options: {
    readonly state: ShellOutputCursor;
    readonly read: (cursor: number) => Effect.Effect<ShellOutputPage, E>;
    readonly onChunk: (chunk: string, truncated: boolean, replace: boolean) => Effect.Effect<void>;
  }) {
    let truncated = options.state.truncated ?? false;
    options.state.replace ??= options.state.cursor === undefined;
    if (options.state.cursor === undefined) {
      const probe = yield* options.read(Number.MAX_SAFE_INTEGER);
      options.state.cursor = Math.max(0, probe.size - LIMIT);
      truncated = options.state.cursor > 0 || probe.truncated;
      options.state.truncated = truncated;
    }
    while (true) {
      const cursor: number = options.state.cursor;
      const page: ShellOutputPage = yield* options.read(cursor);
      if (page.cursor > cursor) {
        yield* options.onChunk(page.output, truncated || page.truncated, options.state.replace);
        options.state.cursor = page.cursor;
        truncated = false;
        options.state.truncated = false;
        options.state.replace = false;
      } else if (truncated || options.state.replace) {
        yield* options.onChunk("", truncated, options.state.replace);
        truncated = false;
        options.state.truncated = false;
        options.state.replace = false;
      }
      if (page.cursor <= cursor || page.cursor >= page.size) yield* Effect.sleep("100 millis");
    }
  },
  Effect.ignore({ log: true }),
);
