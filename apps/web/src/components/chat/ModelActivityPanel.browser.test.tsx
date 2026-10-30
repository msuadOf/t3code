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

it("keeps receiving after a partial history match and collapses only after stream completion", async () => {
  const event = (seq: number, type: string, data: Record<string, unknown>): ModelActivity => ({
    seq,
    type,
    data,
    sessionId: "partial",
    requestId: "partial",
    messageId: "msg-partial",
  });
  let activity = reduceModelActivity(emptyModelActivityState, {
    kind: "snapshot",
    available: true,
    epoch: "partial",
    seq: 1,
    oldestSeq: 1,
    after: 0,
    gap: false,
    confirmedMessageIds: [],
    events: [
      event(1, "content_block_delta", {
        index: 0,
        delta: { type: "thinking_delta", thinking: "Initial thinking" },
      }),
    ],
  });
  // No navigation is involved in native history confirmation.
  await using screen = await render(<ModelActivityView activity={activity} failed={false} />, {
    router: false,
  });
  activity = reduceModelActivity(activity, {
    kind: "confirmed",
    epoch: "partial",
    messageIds: ["msg-partial"],
  });
  activity = reduceModelActivity(activity, {
    kind: "event",
    epoch: "partial",
    event: event(2, "content_block_delta", {
      index: 1,
      delta: { type: "text_delta", text: "Text arriving after history match" },
    }),
  });
  await screen.rerender(<ModelActivityView activity={activity} failed={false} />);
  await expect.element(screen.getByText("Text arriving after history match")).toBeVisible();
  await expect
    .element(screen.getByRole("button", { name: /Source not yet confirmed/ }))
    .toHaveAttribute("aria-expanded", "true");
  activity = reduceModelActivity(activity, {
    kind: "event",
    epoch: "partial",
    event: event(3, "message_stop", {}),
  });
  await screen.rerender(<ModelActivityView activity={activity} failed={false} />);
  await expect
    .element(screen.getByRole("button", { name: /Source not yet confirmed/ }))
    .toHaveAttribute("aria-expanded", "false");
});
