import type { ReactNode } from "react";
import { streamedTextNotice } from "@t3tools/client-runtime/work-log/presentation";

export function StreamedTextContent(props: {
  readonly text: string;
  readonly streaming: boolean;
  readonly textPrefixMissing?: boolean | undefined;
  readonly children: ReactNode;
}) {
  const notice = streamedTextNotice(props);
  return (
    <>
      {notice ? (
        <div role="note" className="mb-1 text-xs text-muted-foreground">
          {notice}
        </div>
      ) : null}
      {props.textPrefixMissing ? (
        <div className="whitespace-pre-wrap break-words">{props.text}</div>
      ) : (
        props.children
      )}
    </>
  );
}
