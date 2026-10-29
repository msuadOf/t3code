import { describe, expect, it } from "vite-plus/test";
import { OpenCode2ObserverCache } from "./OpenCode2ObserverCache.ts";

const delta = (sessionID: string, delta: string) => ({
  type: "session.text.delta",
  data: { sessionID, assistantMessageID: "message", ordinal: 0, delta },
});
const end = (sessionID: string, text: string) => ({
  type: "session.text.ended",
  data: { sessionID, assistantMessageID: "message", ordinal: 0, text },
});

describe("OpenCode observer cache", () => {
  it("publishes cumulative text and retains it for a later subscriber until completion", () => {
    const cache = new OpenCode2ObserverCache();
    expect(cache.accept(delta("one", "hello"))).toEqual(delta("one", "hello"));
    expect(cache.accept(delta("one", " world"))).toEqual(delta("one", " world"));
    expect(cache.snapshot()[0]).toMatchObject({ data: { text: "hello world" } });
    expect(cache.snapshot()).toHaveLength(1);
    cache.accept(end("one", "hello world"));
    expect(cache.snapshot()).toEqual([]);
  });

  it("suppresses incomplete deltas for an observed gap while other sessions stream", () => {
    const cache = new OpenCode2ObserverCache();
    cache.markGap(["one"]);
    expect(cache.snapshot()).toEqual([{ type: "t3.observer.gap", data: { sessionID: "one" } }]);
    expect(cache.accept(delta("one", "tail"))).toBeUndefined();
    expect(cache.accept(delta("two", "complete prefix"))).toEqual(delta("two", "complete prefix"));
    expect(cache.accept(end("one", "complete value"))).toEqual(end("one", "complete value"));
    cache.accept({ type: "session.execution.succeeded", data: { sessionID: "one" } });
    expect(cache.snapshot()).toHaveLength(1);
  });

  it("marks only the affected session when the bounded cache is full", () => {
    const cache = new OpenCode2ObserverCache(600);
    expect(cache.accept(delta("one", "a"))).toEqual(delta("one", "a"));
    expect(cache.accept(delta("one", "x".repeat(100)))).toMatchObject({ type: "t3.observer.gap" });
    expect(cache.accept(delta("one", "tail"))).toBeUndefined();
    expect(cache.accept(delta("two", "b"))).toEqual(delta("two", "b"));
  });
});

describe("OpenCode observer after a native stream gap", () => {
  it.each(["text", "reasoning"] as const)(
    "streams a newly started %s block without accepting the missing prefix of an earlier block",
    (kind) => {
      const cache = new OpenCode2ObserverCache();
      cache.markGap(["session"]);
      const block = { sessionID: "session", assistantMessageID: "new-message", ordinal: 0 };
      const started = { type: `session.${kind}.started`, data: block };
      const next = {
        type: `session.${kind}.delta`,
        data: { ...block, delta: "NEW_COMPLETE_PREFIX " },
      };
      expect(cache.accept(delta("session", "missing-prefix-tail"))).toBeUndefined();
      expect(cache.accept(started)).toEqual(started);
      expect(cache.accept(next)).toEqual(next);
      expect(cache.snapshot()).toEqual([
        { type: "t3.observer.gap", data: { sessionID: "session" } },
        { type: "t3.observer.text", data: { ...block, kind, text: "NEW_COMPLETE_PREFIX " } },
      ]);
      const tail = { type: `session.${kind}.delta`, data: { ...block, delta: "NEXT_TOKEN" } };
      expect(cache.accept(tail)).toEqual(tail);
      expect(cache.snapshot()[1]).toMatchObject({
        data: { text: "NEW_COMPLETE_PREFIX NEXT_TOKEN" },
      });
      expect(cache.accept(delta("session", "still-incomplete-old-tail"))).toBeUndefined();
    },
  );
});
