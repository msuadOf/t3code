import { expect, it, vi } from "vite-plus/test";
import { page } from "vite-plus/test/browser";
import "../../index.css";
import {
  emptyModelActivityState,
  reduceModelActivity,
} from "@t3tools/client-runtime/state/model-activity";
import type { ModelActivity, ModelActivityFrame } from "@t3tools/contracts";
import { render } from "../../../test/render";

vi.mock(import("../../state/server"), () => ({ serverEnvironment: {} }) as never);
import { ModelActivityView } from "./ModelActivityPanel";

it("shows live text before source confirmation and preserves it when source conflicts", async () => {
  await page.viewport(390, 844);
  const event = (seq: number, type: string, data: Record<string, unknown>): ModelActivity => ({
    seq,
    type,
    data,
    sessionId: "fixture",
    requestId: "request",
    messageId: "msg-fixture",
  });
  const snapshot: ModelActivityFrame = {
    kind: "snapshot",
    available: true,
    epoch: "fixture",
    seq: 1,
    oldestSeq: 1,
    after: 0,
    gap: false,
    confirmedMessageIds: [],
    events: [
      event(1, "content_block_delta", {
        index: 0,
        delta: { type: "text_delta", text: "Live fixture response" },
      }),
    ],
  };
  let activity = reduceModelActivity(emptyModelActivityState, snapshot);
  // The activity renderer has no route-dependent behavior.
  await using screen = await render(<ModelActivityView activity={activity} failed={false} />, {
    router: false,
  });
  await expect.element(screen.getByText("Live fixture response")).toBeVisible();
  await expect
    .element(screen.getByRole("button", { name: /Source not yet confirmed/ }))
    .toBeVisible();
  activity = reduceModelActivity(activity, {
    kind: "event",
    epoch: "fixture",
    event: event(2, "observer.source", {
      state: "identified",
      querySource: "repl_main_thread",
      agentId: null,
      parentAgentId: null,
    }),
  });
  await screen.rerender(<ModelActivityView activity={activity} failed={false} />);
  await expect.element(screen.getByRole("button", { name: /Main session/ })).toBeVisible();
  activity = reduceModelActivity(activity, {
    kind: "event",
    epoch: "fixture",
    event: event(3, "observer.source", { state: "conflict" }),
  });
  await screen.rerender(<ModelActivityView activity={activity} failed={false} />);
  await expect
    .element(screen.getByRole("button", { name: /Source could not be confirmed/ }))
    .toBeVisible();
  await expect.element(screen.getByText("Live fixture response")).toBeVisible();
});
