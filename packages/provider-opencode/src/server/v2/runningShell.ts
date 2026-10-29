import type { Shell } from "@opencode/schema/shell";

export interface RunningCommand {
  readonly id: string;
  readonly command: string;
  readonly ran: number;
}

/** Native history omits ephemeral shellID metadata. Recover only an unambiguous native owner. */
export function findRunningShell(
  tool: RunningCommand,
  tools: ReadonlyArray<RunningCommand>,
  shells: ReadonlyArray<Shell.Info>,
  sessionId: string,
  directory: string,
): string | undefined {
  const candidates = shells.filter(
    (shell) =>
      shell.status === "running" &&
      shell.metadata.sessionID === sessionId &&
      shell.cwd === directory &&
      shell.command === tool.command &&
      shell.time.started >= tool.ran,
  );
  if (candidates.length !== 1) return undefined;
  const shell = candidates[0]!;
  const owners = tools.filter(
    (candidate) => candidate.command === shell.command && candidate.ran <= shell.time.started,
  );
  return owners.length === 1 && owners[0]!.id === tool.id ? shell.id : undefined;
}
