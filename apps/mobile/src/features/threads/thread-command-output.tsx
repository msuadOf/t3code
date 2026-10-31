import type { EnvironmentId, ThreadId, TurnItemId } from "@t3tools/contracts";
import { TERMINAL_PALETTES, terminalOutputSpans } from "@t3tools/shared/terminalOutput";
import type React from "react";
import { useMemo, useRef } from "react";
import { ScrollView, View, type NativeScrollEvent, type NativeSyntheticEvent } from "react-native";

import { AppText as Text } from "../../components/ui/AppText";
import { useServerConfigs } from "../../state/entities";
import { orchestrationEnvironment } from "../../state/orchestration";
import { useEnvironmentQuery } from "../../state/query";

import { resolveThreadFeedLiveFollow } from "./thread-feed-live-follow";

// Within this many points of the end still counts as following the output.
const FOLLOW_SLACK = 8;

export function useCommandOutputStreaming(environmentId: EnvironmentId): boolean {
  return (
    useServerConfigs().get(environmentId)?.environment.capabilities.commandOutputStreaming === true
  );
}

/** Mount only while useCommandOutputStreaming holds; unmount to stop subscriptions for collapsed rows or other threads. */
export function ThreadCommandOutput(props: {
  readonly environmentId: EnvironmentId;
  readonly threadId: ThreadId;
  readonly itemId: TurnItemId;
  readonly themeAppearance: "light" | "dark";
}) {
  const { data, error } = useEnvironmentQuery(
    orchestrationEnvironment.v2.commandOutput({
      environmentId: props.environmentId,
      input: { threadId: props.threadId, itemId: props.itemId },
    }),
  );
  const scrollRef = useRef<React.ComponentRef<typeof ScrollView>>(null);
  const followRef = useRef(true);
  const userScrollingRef = useRef(false);
  const updateFollow = (event: NativeSyntheticEvent<NativeScrollEvent>, ending = false) => {
    const { contentOffset, contentSize, layoutMeasurement } = event.nativeEvent;
    followRef.current = resolveThreadFeedLiveFollow(followRef.current, {
      type: ending ? "user-scroll-end" : "scroll",
      isAtEnd: contentSize.height - contentOffset.y - layoutMeasurement.height <= FOLLOW_SLACK,
      userScrollSessionActive: userScrollingRef.current,
    });
    if (ending) userScrollingRef.current = false;
  };
  const beginScroll = () => {
    userScrollingRef.current = true;
    followRef.current = resolveThreadFeedLiveFollow(followRef.current, {
      type: "user-scroll-begin",
    });
  };
  const text = data?.output.text ?? "";
  const spans = useMemo(
    () => terminalOutputSpans(text, TERMINAL_PALETTES[props.themeAppearance]),
    [text, props.themeAppearance],
  );
  if (text.length === 0) {
    const message = error
      ? `Couldn't load output: ${error}`
      : data === null
        ? "Loading output…"
        : data.running
          ? null
          : "No output.";
    return message === null ? null : (
      <Text className="pt-1.5 font-mono text-2xs leading-normal text-foreground-muted">
        {message}
      </Text>
    );
  }
  return (
    <View className="pt-1.5">
      {data?.output.truncated ? (
        <Text className="pb-0.5 text-3xs text-foreground-subtle">Earlier output not shown</Text>
      ) : null}
      <ScrollView
        ref={scrollRef}
        nestedScrollEnabled
        directionalLockEnabled
        showsVerticalScrollIndicator
        className="max-h-60"
        contentContainerStyle={{ paddingRight: 8 }}
        scrollEventThrottle={100}
        onScroll={(event) => updateFollow(event)}
        onScrollBeginDrag={beginScroll}
        onScrollEndDrag={(event) => updateFollow(event, true)}
        onMomentumScrollBegin={beginScroll}
        onMomentumScrollEnd={(event) => updateFollow(event, true)}
        onContentSizeChange={() => {
          if (followRef.current) scrollRef.current?.scrollToEnd({ animated: false });
        }}
      >
        <Text selectable className="font-mono text-2xs leading-normal text-foreground-muted">
          {spans.map((span, index) =>
            span.style === null ? (
              span.text
            ) : (
              // Spans are rebuilt from scratch on every update and never reorder.
              // oxlint-disable-next-line react/no-array-index-key
              <Text key={index} style={span.style}>
                {span.text}
              </Text>
            ),
          )}
        </Text>
      </ScrollView>
    </View>
  );
}
