import { useAtomValue } from "@effect/atom-react";
import type { EnvironmentId, ThreadId } from "@t3tools/contracts";
import type {
  ModelActivityRequest,
  ModelActivityState,
} from "@t3tools/client-runtime/state/server";
import * as Option from "effect/Option";
import { AsyncResult } from "effect/reactivity";
import { useLayoutEffect, useRef, useState } from "react";
import {
  modelActivityFollowOffset,
  modelActivityRequestKey,
  resolveModelActivityFollowTarget,
} from "@t3tools/client-runtime/state/model-activity-follow";

import { observeResize } from "../../lib/observeResize";
import { serverEnvironment } from "../../state/server";
import { Collapsible, CollapsiblePanel, CollapsibleTrigger } from "../ui/collapsible";
import { Button } from "../ui/button";

function ModelRequest({
  request,
  onToggle,
  followActive,
}: {
  readonly request: ModelActivityRequest;
  readonly onToggle: () => void;
  readonly followActive: boolean;
}) {
  const [expanded, setExpanded] = useState<{ confirmed: boolean; open: boolean } | null>(null);
  const defaultOpen =
    !request.confirmed &&
    !(request.status === "failed" && !request.blocks.some((block) => block.text.length > 0));
  const open =
    followActive || (expanded?.confirmed === request.confirmed ? expanded.open : defaultOpen);
  return (
    <Collapsible
      open={open}
      onOpenChange={(open) => {
        onToggle();
        setExpanded({ confirmed: request.confirmed, open });
      }}
    >
      <CollapsibleTrigger render={<Button variant="ghost" size="sm" />}>
        {open ? "▾" : "▸"} Model request {request.id.slice(-8)}
      </CollapsibleTrigger>
      <p className="px-2 text-xs text-muted-foreground">
        {request.confirmed
          ? "Saved in conversation"
          : request.status === "running"
            ? "Receiving · Not yet saved by Claude"
            : request.status === "failed"
              ? "Stream ended unexpectedly · Not yet saved by Claude"
              : "Response received · Not yet saved by Claude"}
      </p>
      <CollapsiblePanel animate={false}>
        <div className="space-y-2 px-2 pb-3">
          <p className="text-xs text-muted-foreground">
            {request.confirmed
              ? "Matched to Claude’s saved message. Expand only to inspect the observed response."
              : "Not yet saved by Claude"}
          </p>
          {request.messageId ? (
            <p className="break-all font-mono text-xs text-muted-foreground">
              Message {request.messageId}
            </p>
          ) : null}
          {request.blocks.length === 0 ? (
            <p className="text-xs text-muted-foreground">Waiting for response content.</p>
          ) : null}
          {request.blocks.map((block) => (
            <div key={block.index} className="space-y-1">
              <p className="text-xs text-muted-foreground">
                {block.kind === "thinking"
                  ? "Thinking"
                  : block.kind === "tool_use"
                    ? "Tool input · partial JSON"
                    : block.kind === "text"
                      ? "Text"
                      : `Raw model block · ${block.kind}`}
              </p>
              <pre className="whitespace-pre-wrap break-words font-mono text-xs">
                {block.text || "No content received yet."}
              </pre>
            </div>
          ))}
        </div>
      </CollapsiblePanel>
    </Collapsible>
  );
}

export function ModelActivityPanel({
  environmentId,
  threadId,
}: {
  readonly environmentId: EnvironmentId;
  readonly threadId: ThreadId;
}) {
  const result = useAtomValue(
    serverEnvironment.modelActivity({ environmentId, input: { threadId } }),
  );
  const activity = Option.getOrNull(AsyncResult.value(result));
  if (!activity?.available) return null;
  return (
    <ModelActivityView
      key={`${environmentId}:${threadId}:${activity.epoch}`}
      activity={activity}
      failed={AsyncResult.isFailure(result)}
    />
  );
}

function ModelActivityView({
  activity,
  failed,
}: {
  readonly activity: ModelActivityState;
  readonly failed: boolean;
}) {
  const [open, setOpen] = useState(true);
  const [following, setFollowing] = useState(true);
  const followingRef = useRef(true);
  const viewportRef = useRef<HTMLDivElement>(null);
  const anchorRef = useRef<HTMLDivElement>(null);
  const [previousTarget, setPreviousTarget] = useState<string | null>(null);
  const target = resolveModelActivityFollowTarget(activity.requests, previousTarget);
  if (target !== null && target !== previousTarget) setPreviousTarget(target);
  const pause = () => {
    followingRef.current = false;
    setFollowing(false);
  };
  const follow = () => {
    const viewport = viewportRef.current;
    const anchor = anchorRef.current;
    if (!viewport || !anchor || !target) return;
    const offset = modelActivityFollowOffset({
      following: followingRef.current,
      targetBottom:
        anchor.getBoundingClientRect().bottom -
        viewport.getBoundingClientRect().top +
        viewport.scrollTop -
        viewport.clientTop,
      viewportHeight: viewport.clientHeight,
    });
    // Set only this viewport: scrollIntoView would also move the main conversation.
    if (offset !== null) viewport.scrollTop = offset;
  };
  useLayoutEffect(() => {
    if (!open || !target) return;
    follow();
    const elements = [anchorRef.current, viewportRef.current].filter((element) => element !== null);
    return observeResize(elements, follow);
  });
  const disconnected = activity.connectionState === "disconnected" || failed;
  const unsaved = activity.requests.filter((request) => !request.confirmed).length;
  return (
    <aside
      aria-label="Model activity"
      className="shrink-0 border-b border-border px-3 py-1 sm:px-5"
    >
      <Collapsible
        open={open}
        onOpenChange={(value) => {
          pause();
          setOpen(value);
        }}
      >
        <CollapsibleTrigger render={<Button variant="ghost" size="sm" />}>
          {open ? "▾" : "▸"} Model activity
          {disconnected ? " · reconnecting" : unsaved > 0 ? ` · ${unsaved} not yet saved` : ""}
        </CollapsibleTrigger>
        <CollapsiblePanel animate={false}>
          {target ? (
            <Button
              variant="ghost"
              size="sm"
              disabled={following}
              onClick={() => {
                followingRef.current = true;
                setFollowing(true);
                follow();
              }}
            >
              {following ? "Following live" : "Follow live"}
            </Button>
          ) : null}
          <div
            ref={viewportRef}
            tabIndex={0}
            aria-label="Observed model responses"
            className="max-h-60 space-y-2 overflow-y-auto overscroll-contain py-2 [overflow-anchor:none]"
            onWheel={pause}
            onTouchMove={pause}
            onPointerDown={pause}
            onKeyDown={(event) => {
              if (
                ["ArrowUp", "ArrowDown", "PageUp", "PageDown", "Home", "End", " "].includes(
                  event.key,
                )
              )
                pause();
            }}
          >
            <p className="text-xs text-muted-foreground">
              Observed model responses, separate from the saved conversation. Requests are not
              classified as main or auxiliary.
            </p>
            {disconnected ? (
              <p role="status" className="text-xs text-muted-foreground">
                Model activity disconnected. Reconnecting automatically; received activity stays
                visible. The saved conversation is unaffected.
              </p>
            ) : null}
            {activity.gap || activity.truncated ? (
              <p role="status" className="text-xs text-muted-foreground">
                Some observed activity is missing or was trimmed. Check the saved conversation for
                persisted messages.
              </p>
            ) : null}
            {activity.requests.length === 0 ? (
              <p className="text-xs text-muted-foreground">Waiting for model activity.</p>
            ) : null}
            {activity.requests.toReversed().map((request) => (
              <div
                key={modelActivityRequestKey(request)}
                ref={modelActivityRequestKey(request) === target ? anchorRef : undefined}
              >
                <ModelRequest
                  request={request}
                  onToggle={pause}
                  followActive={following && modelActivityRequestKey(request) === target}
                />
              </div>
            ))}
          </div>
        </CollapsiblePanel>
      </Collapsible>
    </aside>
  );
}
