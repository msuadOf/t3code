import { assert, it } from "@effect/vitest";
import { Shell } from "@opencode/schema/shell";
import { findRunningShell } from "./runningShell.ts";

const tool = { id: "call-native", command: "python3 native-progress.py", ran: 10 };
const shell: Shell.Info = {
  id: Shell.ID.make("sh_native"),
  status: "running",
  command: tool.command,
  cwd: "/work",
  shell: "/bin/sh",
  file: "/output",
  metadata: { sessionID: "ses_native" },
  time: { started: 11 },
};

it("recovers a running native shell whose ephemeral tool metadata was missed", () => {
  assert.equal(findRunningShell(tool, [tool], [shell], "ses_native", "/work"), shell.id);
});

it.each([
  { name: "another session", shell: { ...shell, metadata: { sessionID: "ses_other" } } },
  { name: "another directory", shell: { ...shell, cwd: "/other" } },
  { name: "another command", shell: { ...shell, command: "different" } },
  { name: "an earlier background command", shell: { ...shell, time: { started: 9 } } },
  { name: "an exited shell", shell: { ...shell, status: "exited" as const } },
])("does not subscribe to $name", ({ shell: candidate }) => {
  assert.isUndefined(findRunningShell(tool, [tool], [candidate], "ses_native", "/work"));
});

it("refuses ambiguous native commands and shells", () => {
  assert.isUndefined(findRunningShell(tool, [tool], [shell, shell], "ses_native", "/work"));
  assert.isUndefined(
    findRunningShell(tool, [tool, { ...tool, id: "call-other" }], [shell], "ses_native", "/work"),
  );
});
