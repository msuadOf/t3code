import {
  appendTerminalOutput,
  EMPTY_TERMINAL_OUTPUT,
  terminalOutputResumeText,
  type TerminalOutputState,
} from "@t3tools/shared/terminalOutput";

/** A read-only CLI view, never a source of conversation, tool identity or status. */
export class NativeClaudeTerminalProgress {
  private state: TerminalOutputState = EMPTY_TERMINAL_OUTPUT;
  private accepted = true;
  private dropped = false;
  private readonly sessionId: string;
  constructor(sessionId: string) {
    this.sessionId = sessionId;
  }

  accept(frame: Record<string, unknown>): void {
    if (frame.type === "snapshot") {
      this.state = EMPTY_TERMINAL_OUTPUT;
      this.dropped = false;
      const record = frame.record;
      this.accepted =
        typeof record === "object" &&
        record !== null &&
        "sessionId" in record &&
        record.sessionId === this.sessionId;
      // Tail replay may begin mid-redraw. Only subsequent live bytes form this view.
      return;
    }
    if (!this.accepted || frame.type !== "stream" || typeof frame.line !== "string") return;
    if (frame.line.length > 64 * 1024) {
      // Do not splice into an escape sequence; drop the whole oversized frame.
      this.dropped = true;
      return;
    }
    this.state = appendTerminalOutput(this.state, frame.line, 16 * 1024);
  }

  snapshot(): { readonly text: string; readonly truncated: boolean; readonly partial: boolean } {
    return {
      text: terminalOutputResumeText(this.state),
      truncated: this.dropped || this.state.truncated,
      partial: true,
    };
  }
}
