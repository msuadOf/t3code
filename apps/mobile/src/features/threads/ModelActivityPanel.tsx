import { useAtomValue } from "@effect/atom-react";
import type { EnvironmentId, ThreadId } from "@t3tools/contracts";
import { modelActivityBlockLabel } from "@t3tools/client-runtime/state/model-activity";
import type {
  ModelActivityRequest,
  ModelActivityState,
} from "@t3tools/client-runtime/state/server";
import * as Option from "effect/Option";
import { AsyncResult } from "effect/reactivity";
import { type ComponentRef, useLayoutEffect, useRef, useState } from "react";
import {
  modelActivityFollowOffset,
  modelActivityRequestKey,
  resolveModelActivityFollowTarget,
} from "@t3tools/client-runtime/state/model-activity-follow";
import { ScrollView, View } from "react-native";

import { AppText as Text } from "../../components/ui/AppText";
import { RowPressable } from "../../components/ui/RowPressable";
import { serverEnvironment } from "../../state/server";

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
    <View className="gap-2">
      <RowPressable
        accessibilityRole="button"
        accessibilityState={{ expanded: open }}
        accessibilityLabel={`Model request ${request.id.slice(-8)}, ${request.confirmed ? "Saved in conversation" : "Not yet saved by Claude"}`}
        onPress={() => {
          onToggle();
          setExpanded({ confirmed: request.confirmed, open: !open });
        }}
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
                {modelActivityBlockLabel(block, request.status)}
              </Text>
              {block.initialInput ? (
                <>
                  <Text className="text-xs text-foreground-muted">Initial arguments</Text>
                  <Text selectable className="font-mono text-xs text-foreground">
                    {block.initialInput}
                  </Text>
                  {block.text ? (
                    <Text className="text-xs text-foreground-muted">Argument stream</Text>
                  ) : null}
                </>
              ) : null}
              {block.text || !block.initialInput ? (
                <Text selectable className="font-mono text-xs text-foreground">
                  {block.text || "No content observed."}
                </Text>
              ) : null}
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
  const viewportRef = useRef<ComponentRef<typeof ScrollView>>(null);
  const viewportHeight = useRef(0);
  const anchors = useRef(new Map<string, number>());
  const [previousTarget, setPreviousTarget] = useState<string | null>(null);
  const target = resolveModelActivityFollowTarget(activity.requests, previousTarget);
  const targetRef = useRef(target);
  if (target !== null && target !== previousTarget) setPreviousTarget(target);
  const pause = () => {
    followingRef.current = false;
    setFollowing(false);
  };
  const follow = () => {
    const offset = modelActivityFollowOffset({
      following: followingRef.current,
      targetBottom: targetRef.current ? (anchors.current.get(targetRef.current) ?? null) : null,
      viewportHeight: viewportHeight.current,
    });
    if (offset !== null) viewportRef.current?.scrollTo({ y: offset, animated: false });
  };
  useLayoutEffect(() => {
    targetRef.current = target;
    const keys = new Set(activity.requests.map(modelActivityRequestKey));
    for (const key of anchors.current.keys()) {
      if (!keys.has(key)) anchors.current.delete(key);
    }
    follow();
  });
  // oxlint-disable-next-line unicorn/no-array-reverse -- Hermes lacks toReversed; reverse only this copy.
  const newestFirst = [...activity.requests].reverse();
  const disconnected = activity.connectionState === "disconnected" || failed;
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
        onPress={() => {
          pause();
          setOpen(!open);
        }}
      >
        <Text className="py-2 text-sm text-foreground">
          {open ? "▾" : "▸"} Model activity
          {disconnected ? " · reconnecting" : unsaved > 0 ? ` · ${unsaved} not yet saved` : ""}
        </Text>
      </RowPressable>
      {open ? (
        <View>
          {target ? (
            <RowPressable
              accessibilityRole="button"
              accessibilityState={{ disabled: following }}
              disabled={following}
              onPress={() => {
                followingRef.current = true;
                setFollowing(true);
                follow();
              }}
            >
              <Text className="py-2 text-sm text-foreground">
                {following ? "Following live" : "Follow live"}
              </Text>
            </RowPressable>
          ) : null}
          <ScrollView
            ref={viewportRef}
            nestedScrollEnabled
            className="max-h-60"
            contentContainerClassName="gap-2 py-2"
            onTouchMove={pause}
            onScrollBeginDrag={pause}
            onLayout={(event) => {
              viewportHeight.current = event.nativeEvent.layout.height;
              follow();
            }}
            onContentSizeChange={follow}
          >
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
              <View
                key={modelActivityRequestKey(request)}
                onLayout={(event) => {
                  const key = modelActivityRequestKey(request);
                  const { y, height } = event.nativeEvent.layout;
                  anchors.current.set(key, y + height);
                  follow();
                }}
              >
                <ModelRequest
                  request={request}
                  onToggle={pause}
                  followActive={following && modelActivityRequestKey(request) === target}
                />
              </View>
            ))}
          </ScrollView>
        </View>
      ) : null}
    </View>
  );
}
