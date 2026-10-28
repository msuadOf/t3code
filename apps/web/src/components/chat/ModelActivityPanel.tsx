import { useAtomValue } from "@effect/atom-react";
import type { EnvironmentId, ThreadId } from "@t3tools/contracts";
import type { ModelActivityRequest } from "@t3tools/client-runtime/state/server";
import * as Option from "effect/Option";
import { AsyncResult } from "effect/reactivity";
import { useState } from "react";

import { serverEnvironment } from "../../state/server";
import { Collapsible, CollapsiblePanel, CollapsibleTrigger } from "../ui/collapsible";
import { Button } from "../ui/button";

function ModelRequest({ request }: { readonly request: ModelActivityRequest }) {
  const [expanded, setExpanded] = useState<{ confirmed: boolean; open: boolean } | null>(null);
  const defaultOpen =
    !request.confirmed &&
    !(request.status === "failed" && !request.blocks.some((block) => block.text.length > 0));
  const open = expanded?.confirmed === request.confirmed ? expanded.open : defaultOpen;
  return (
    <Collapsible
      open={open}
      onOpenChange={(open) => setExpanded({ confirmed: request.confirmed, open })}
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
  const [open, setOpen] = useState(true);
  if (!activity?.available) return null;
  const unsaved = activity.requests.filter((request) => !request.confirmed).length;
  return (
    <aside
      aria-label="Model activity"
      className="shrink-0 border-b border-border px-3 py-1 sm:px-5"
    >
      <Collapsible open={open} onOpenChange={setOpen}>
        <CollapsibleTrigger render={<Button variant="ghost" size="sm" />}>
          {open ? "▾" : "▸"} Model activity{unsaved > 0 ? ` · ${unsaved} not yet saved` : ""}
        </CollapsibleTrigger>
        <CollapsiblePanel animate={false}>
          <div className="max-h-60 space-y-2 overflow-y-auto overscroll-contain py-2">
            <p className="text-xs text-muted-foreground">
              Observed model responses, separate from the saved conversation. Requests are not
              classified as main or auxiliary.
            </p>
            {AsyncResult.isFailure(result) ? (
              <p role="status" className="text-xs text-muted-foreground">
                Model activity disconnected. The saved conversation remains available.
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
              <ModelRequest key={`${request.sessionId}:${request.id}`} request={request} />
            ))}
          </div>
        </CollapsiblePanel>
      </Collapsible>
    </aside>
  );
}
