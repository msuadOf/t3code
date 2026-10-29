import type { NativeClaudeSession } from "./NativeClaudeClient.ts";
import type { NativeHistoryItem } from "@t3tools/provider-core/server/nativeSessions";
import { nativeClaudeQuestion } from "./NativeClaudeQuestions.ts";

export interface NativeClaudeWaitingNotice {
  readonly status: "running" | "completed";
  readonly message: string;
}
export function nativeClaudeWaitingNotice(
  previous: NativeClaudeWaitingNotice | undefined,
  job: NativeClaudeSession | undefined,
  pendingQuestions: ReadonlyArray<NativeHistoryItem>,
  finished: boolean,
): NativeClaudeWaitingNotice | undefined {
  if (!job && !finished) return;
  const answerable =
    pendingQuestions.length === 1 && nativeClaudeQuestion(pendingQuestions[0]?.input) !== undefined;
  const waiting = !finished && job?.status === "waiting" && !answerable;
  let next: NativeClaudeWaitingNotice;
  if (waiting) {
    const command =
      job?.id && /^[a-f0-9]{8}$/.test(job.id) ? `claude attach ${job.id}` : "claude attach";
    next = {
      status: "running",
      message:
        pendingQuestions.length > 0
          ? `Claude is waiting for a question that T3 cannot answer. Use ${command} in the CLI to continue.`
          : `Claude is waiting for input or approval in its attached CLI. Use ${command} to continue.`,
    };
  } else {
    if (previous?.status !== "running") return;
    next = {
      status: "completed",
      message: finished
        ? "Claude stopped waiting; this turn has ended."
        : answerable
          ? "Claude is waiting for the question shown in T3."
          : "Claude continued after waiting for input.",
    };
  }
  return previous?.status === next.status && previous.message === next.message ? undefined : next;
}
