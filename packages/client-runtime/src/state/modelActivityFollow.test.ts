import { describe, expect, it } from "vite-plus/test";
import {
  modelActivityFollowOffset,
  resolveModelActivityFollowTarget,
} from "./modelActivityFollow.ts";

describe("model activity request follow geometry", () => {
  it("follows growing latest response rather than old requests below it", () => {
    // Latest response ends at 900; old requests extend the list to 5000.
    expect(
      modelActivityFollowOffset({ following: true, targetBottom: 900, viewportHeight: 240 }),
    ).toBe(660);
    expect(
      modelActivityFollowOffset({ following: true, targetBottom: 1100, viewportHeight: 240 }),
    ).toBe(860);
  });
  it("preserves manual reading until explicitly resumed", () => {
    expect(
      modelActivityFollowOffset({ following: false, targetBottom: 1200, viewportHeight: 240 }),
    ).toBeNull();
    expect(
      modelActivityFollowOffset({ following: true, targetBottom: 1200, viewportHeight: 240 }),
    ).toBe(960);
  });
  it("does not jump when a saved request collapses or the anchor unmounts", () => {
    expect(
      modelActivityFollowOffset({ following: true, targetBottom: null, viewportHeight: 240 }),
    ).toBeNull();
  });
  it("keeps short responses visible and recomputes after viewport resize", () => {
    expect(
      modelActivityFollowOffset({ following: true, targetBottom: 100, viewportHeight: 240 }),
    ).toBe(0);
    expect(
      modelActivityFollowOffset({ following: true, targetBottom: 900, viewportHeight: 400 }),
    ).toBe(500);
    expect(
      modelActivityFollowOffset({ following: true, targetBottom: 900, viewportHeight: 0 }),
    ).toBeNull();
  });
});

const request = (id: string, status: "running" | "completed" | "failed", confirmed = false) => ({
  id,
  sessionId: "s",
  status,
  confirmed,
});

describe("model activity follow target", () => {
  it("prefers running body over newer completed auxiliary response", () => {
    expect(
      resolveModelActivityFollowTarget(
        [request("body", "running"), request("title", "completed")],
        null,
      ),
    ).toBe("s:body");
  });
  it("chooses latest running request and retains it after it completes", () => {
    const requests = [request("old", "running"), request("new", "running")];
    expect(resolveModelActivityFollowTarget(requests, null)).toBe("s:new");
    expect(
      resolveModelActivityFollowTarget([requests[0]!, request("new", "completed")], "s:new"),
    ).toBe("s:new");
  });
  it("does not jump to older unsaved activity on native confirmation", () => {
    expect(
      resolveModelActivityFollowTarget(
        [request("old", "running"), request("body", "completed", true)],
        "s:body",
      ),
    ).toBeNull();
    expect(
      resolveModelActivityFollowTarget(
        [request("old", "running"), request("body", "completed", true), request("next", "running")],
        "s:body",
      ),
    ).toBe("s:next");
  });
  it("does not open a failed response or return to older activity", () => {
    expect(
      resolveModelActivityFollowTarget(
        [request("old", "running"), request("body", "failed")],
        "s:body",
      ),
    ).toBeNull();
  });
  it("recovers after old request eviction without sharing anchors across epochs", () => {
    expect(resolveModelActivityFollowTarget([request("next", "running")], "s:evicted")).toBe(
      "s:next",
    );
    expect(resolveModelActivityFollowTarget([request("history", "completed")], null)).toBe(
      "s:history",
    );
  });
});
