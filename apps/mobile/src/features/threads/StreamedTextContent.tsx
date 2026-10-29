import type { ReactNode } from "react";
import { View } from "react-native";
import { streamedTextNotice } from "@t3tools/client-runtime/work-log/presentation";
import { AppText as Text } from "../../components/ui/AppText";

export function StreamedTextContent(props: {
  readonly text: string;
  readonly streaming: boolean;
  readonly textPrefixMissing?: boolean | undefined;
  readonly children: ReactNode;
}) {
  const notice = streamedTextNotice(props);
  return (
    <View>
      {notice ? <Text className="mb-1 text-2xs text-foreground-muted">{notice}</Text> : null}
      {props.textPrefixMissing ? (
        <Text selectable className="text-sm leading-normal">
          {props.text}
        </Text>
      ) : (
        props.children
      )}
    </View>
  );
}
