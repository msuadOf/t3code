import { useAtomValue } from "@effect/atom-react";
import type { EnvironmentId, ThreadId } from "@t3tools/contracts";
import type { ModelActivityRequest } from "@t3tools/client-runtime/state/server";
import * as Option from "effect/Option";
import { AsyncResult } from "effect/reactivity";
import { useState } from "react";
import { ScrollView, View } from "react-native";

import { AppText as Text } from "../../components/ui/AppText";
import { RowPressable } from "../../components/ui/RowPressable";
import { serverEnvironment } from "../../state/server";

function ModelRequest({ request }: { readonly request: ModelActivityRequest }) {
  const [expanded, setExpanded] = useState<{ confirmed: boolean; open: boolean } | null>(null);
  const defaultOpen =
    !request.confirmed &&
    !(request.status === "failed" && !request.blocks.some((block) => block.text.length > 0));
  const open = expanded?.confirmed === request.confirmed ? expanded.open : defaultOpen;
  return (
    <View className="gap-2">
      <RowPressable
        accessibilityRole="button"
        accessibilityState={{ expanded: open }}
        accessibilityLabel={`Model request ${request.id.slice(-8)}, ${request.confirmed ? "Saved in conversation" : "Not yet saved by Claude"}`}
        onPress={() => setExpanded({ confirmed: request.confirmed, open: !open })}
      >
        <Text className="py-2 text-sm text-foreground">
          {open ? "▾" : "▸"} Model request {request.id.slice(-8)} ·{" "}
          {request.confirmed
            ? "Saved in conversation"
            : request.status === "running"
              ? "Receiving"
              : request.status === "failed"
                ? "Stream ended unexpectedly"
                : "Response received"}
        </Text>
      </RowPressable>
      {open ? (
        <View className="gap-2 pb-3">
          <Text className="text-xs text-foreground-muted">
            {request.confirmed
              ? "Matched to Claude’s saved message. Expand only to inspect the observed response."
              : "Not yet saved by Claude"}
          </Text>
          {request.messageId ? (
            <Text selectable className="font-mono text-xs text-foreground-muted">
              Message {request.messageId}
            </Text>
          ) : null}
          {request.blocks.length === 0 ? (
            <Text className="text-xs text-foreground-muted">Waiting for response content.</Text>
          ) : null}
          {request.blocks.map((block) => (
            <View key={block.index} className="gap-1">
              <Text className="text-xs text-foreground-muted">
                {block.kind === "thinking"
                  ? "Thinking"
                  : block.kind === "tool_use"
                    ? "Tool input · partial JSON"
                    : block.kind === "text"
                      ? "Text"
                      : `Raw model block · ${block.kind}`}
              </Text>
              <Text selectable className="font-mono text-xs text-foreground">
                {block.text || "No content received yet."}
              </Text>
            </View>
          ))}
        </View>
      ) : null}
    </View>
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
  // oxlint-disable-next-line unicorn/no-array-reverse -- Hermes lacks toReversed; reverse only this copy.
  const newestFirst = [...activity.requests].reverse();
  const disconnected = activity.connectionState === "disconnected" || AsyncResult.isFailure(result);
  const unsaved = activity.requests.filter((request) => !request.confirmed).length;
  return (
    <View
      accessibilityLabel="Model activity"
      className="shrink-0 border-b border-border-subtle px-4 py-1"
    >
      <RowPressable
        accessibilityRole="button"
        accessibilityState={{ expanded: open }}
        accessibilityLabel={disconnected ? "Model activity, reconnecting" : "Model activity"}
        onPress={() => setOpen(!open)}
      >
        <Text className="py-2 text-sm text-foreground">
          {open ? "▾" : "▸"} Model activity
          {disconnected ? " · reconnecting" : unsaved > 0 ? ` · ${unsaved} not yet saved` : ""}
        </Text>
      </RowPressable>
      {open ? (
        <ScrollView nestedScrollEnabled className="max-h-60" contentContainerClassName="gap-2 py-2">
          <Text className="text-xs text-foreground-muted">
            Observed model responses, separate from the saved conversation. Requests are not
            classified as main or auxiliary.
          </Text>
          {disconnected ? (
            <Text accessibilityRole="alert" className="text-xs text-foreground-muted">
              Model activity disconnected. Reconnecting automatically; received activity stays
              visible. The saved conversation is unaffected.
            </Text>
          ) : null}
          {activity.gap || activity.truncated ? (
            <Text className="text-xs text-foreground-muted">
              Some observed activity is missing or was trimmed. Check the saved conversation for
              persisted messages.
            </Text>
          ) : null}
          {activity.requests.length === 0 ? (
            <Text className="text-xs text-foreground-muted">Waiting for model activity.</Text>
          ) : null}
          {newestFirst.map((request) => (
            <ModelRequest key={`${request.sessionId}:${request.id}`} request={request} />
          ))}
        </ScrollView>
      ) : null}
    </View>
  );
}
