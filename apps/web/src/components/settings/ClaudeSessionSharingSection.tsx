import {
  AuthOrchestrationOperateScope,
  AuthOrchestrationReadScope,
  type EnvironmentId,
  type ProviderInstanceId,
  type NativeSessionSharingCandidate,
} from "@t3tools/contracts";
import { squashAtomCommandFailure } from "@t3tools/client-runtime/state/runtime";
import { useNavigate } from "@tanstack/react-router";
import { useRef, useState } from "react";
import { serverEnvironment } from "../../state/server";
import { useEnvironmentScope } from "../../state/session";
import { useAtomCommand } from "../../state/use-atom-command";
import { Button } from "../ui/button";
import { Input } from "../ui/input";
import { SettingsSection } from "./settingsLayout";

export function ClaudeSessionSharingSection({
  environmentId,
  instanceId,
}: {
  readonly environmentId: EnvironmentId;
  readonly instanceId: ProviderInstanceId;
}) {
  const navigate = useNavigate();
  const canRead = useEnvironmentScope(environmentId, AuthOrchestrationReadScope);
  const canOpen = useEnvironmentScope(environmentId, AuthOrchestrationOperateScope);
  const list = useAtomCommand(serverEnvironment.listNativeSessions, { reportFailure: false });
  const open = useAtomCommand(serverEnvironment.openNativeSession, { reportFailure: false });
  const [query, setQuery] = useState("");
  const [loadedQuery, setLoadedQuery] = useState("");
  const [sessions, setSessions] = useState<readonly NativeSessionSharingCandidate[] | null>(null);
  const [nextOffset, setNextOffset] = useState<number | null>(null);
  const [busy, setBusy] = useState(false);
  const gate = useRef(false);
  const [error, setError] = useState<string | null>(null);
  async function load(offset?: number) {
    if (gate.current || !canRead) return;
    gate.current = true;
    setBusy(true);
    setError(null);
    const result = await list({
      environmentId,
      input: { instanceId, query, ...(offset === undefined ? {} : { offset }) },
    });
    gate.current = false;
    setBusy(false);
    if (result._tag === "Success") {
      setSessions((current) =>
        offset === undefined
          ? result.value.sessions
          : [...(current ?? []), ...result.value.sessions],
      );
      setLoadedQuery(query);
      setNextOffset(result.value.nextOffset);
    } else if (result._tag === "Failure") {
      const cause = squashAtomCommandFailure(result);
      setError(
        cause instanceof Error
          ? cause.message
          : "Could not list Claude sessions. Refresh to try again.",
      );
    }
  }
  async function openSession(
    sessionId: string,
    observeModelActivity = false,
    modelActivityAction?: "enable" | "disable",
  ) {
    if (gate.current || !canOpen) return;
    gate.current = true;
    setBusy(true);
    setError(null);
    const result = await open({
      environmentId,
      input: {
        instanceId,
        sessionId,
        ...(observeModelActivity ? { observeModelActivity: true } : {}),
        ...(modelActivityAction === undefined ? {} : { modelActivityAction }),
      },
    });
    gate.current = false;
    setBusy(false);
    if (result._tag === "Success") {
      void navigate({
        to: "/$environmentId/$threadId",
        params: { environmentId, threadId: result.value.threadId },
      });
    } else if (result._tag === "Failure") {
      const cause = squashAtomCommandFailure(result);
      setError(
        cause instanceof Error
          ? cause.message
          : "Could not open Claude session. Refresh to try again.",
      );
    }
  }
  return (
    <SettingsSection title="Claude sessions">
      <div className="space-y-3 px-3 py-3 sm:px-4">
        <p className="text-xs text-muted-foreground">
          Continue a saved Claude conversation as a shared session. Only sessions inside the shared
          session directory are shown.
        </p>
        <form
          noValidate
          className="flex flex-wrap items-center gap-2"
          onSubmit={(event) => {
            event.preventDefault();
            void load();
          }}
        >
          <div className="min-w-0 flex-1">
            <Input
              aria-label="Search Claude sessions"
              placeholder="Title, directory or session ID"
              value={query}
              maxLength={200}
              disabled={busy || !canRead}
              onChange={(event) => setQuery(event.target.value)}
            />
          </div>
          {query ? (
            <Button variant="ghost" size="sm" disabled={busy} onClick={() => setQuery("")}>
              Clear
            </Button>
          ) : null}
          <Button type="submit" variant="outline" size="sm" disabled={busy || !canRead}>
            {busy ? "Working…" : sessions === null ? "Browse sessions" : "Refresh"}
          </Button>
        </form>
        {error ? (
          <p role="alert" className="text-xs text-destructive">
            {error}
          </p>
        ) : null}
        {sessions?.length === 0 ? (
          <p role="status" className="text-xs text-muted-foreground">
            No matching Claude sessions. Check the search and shared session directory, then
            refresh.
          </p>
        ) : null}
        <div className="max-h-96 space-y-3 overflow-y-auto">
          {sessions?.map((session) => (
            <div key={session.sessionId} className="space-y-2 border-t border-border pt-3">
              <div className="min-w-0 space-y-1">
                <p className="break-words text-sm">{session.title || "Claude session"}</p>
                <p className="break-all text-xs text-muted-foreground">{session.cwd}</p>
                <p className="text-xs text-muted-foreground">
                  {session.state === "foreground"
                    ? "Open in terminal"
                    : session.state === "shared"
                      ? "Shared session"
                      : "Saved history"}
                </p>
              </div>
              {!session.historyAvailable ? (
                <p className="text-xs text-muted-foreground">
                  Claude has not saved this conversation yet. History will appear after its next
                  reply.
                </p>
              ) : null}
              {session.state === "foreground" ? (
                <p className="text-xs text-muted-foreground">
                  Run <code>/bg</code> in the original terminal, then refresh to open it here.
                </p>
              ) : (
                <div className="space-y-2">
                  <div className="flex flex-wrap gap-2">
                    <Button
                      variant="outline"
                      size="sm"
                      disabled={busy || !canOpen || query !== loadedQuery}
                      onClick={() => void openSession(session.sessionId)}
                    >
                      {session.state === "shared"
                        ? "Open shared session"
                        : "Continue as shared session"}
                    </Button>
                    {session.state === "history" && session.modelActivityAvailable === true ? (
                      <Button
                        variant="outline"
                        size="sm"
                        disabled={busy || !canOpen || query !== loadedQuery}
                        onClick={() => void openSession(session.sessionId, true)}
                      >
                        Continue with model activity
                      </Button>
                    ) : null}
                    {session.modelActivityEnabled !== undefined && session.active === false ? (
                      <Button
                        variant="outline"
                        size="sm"
                        disabled={busy || !canOpen || query !== loadedQuery}
                        onClick={() =>
                          void openSession(
                            session.sessionId,
                            false,
                            session.modelActivityEnabled ? "disable" : "enable",
                          )
                        }
                      >
                        {session.modelActivityEnabled
                          ? "Restart without model activity"
                          : "Restart with model activity"}
                      </Button>
                    ) : null}
                  </div>
                  {session.modelActivityEnabled !== undefined ? (
                    <p className="text-xs text-muted-foreground">
                      Model activity is {session.modelActivityEnabled ? "enabled" : "disabled"}.
                      Changing it restarts this native worker and disconnects terminal attachments.
                      The session UUID stays the same. Restart is available only while Claude is
                      idle.
                    </p>
                  ) : null}
                  {session.state === "history" && session.modelActivityAvailable === true ? (
                    <p className="text-xs text-muted-foreground">
                      Model activity is off by default. Enable it for this inactive session to see
                      temporary responses before Claude saves them to the conversation.
                    </p>
                  ) : null}
                </div>
              )}
            </div>
          ))}
        </div>
        {nextOffset !== null ? (
          <Button
            variant="outline"
            size="sm"
            disabled={busy || !canRead || query !== loadedQuery}
            onClick={() => void load(nextOffset)}
          >
            Load more
          </Button>
        ) : null}
      </div>
    </SettingsSection>
  );
}
