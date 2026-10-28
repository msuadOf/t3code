import {
  AuthOrchestrationOperateScope,
  AuthOrchestrationReadScope,
  type EnvironmentId,
  type ProviderInstanceId,
  type NativeSessionSharingCandidate,
} from "@t3tools/contracts";
import { squashAtomCommandFailure } from "@t3tools/client-runtime/state/runtime";
import { useAdaptiveWorkspaceLayout } from "../../layout/AdaptiveWorkspaceLayout";
import { useRef, useState } from "react";
import { TextInput, View } from "react-native";
import { AppText as Text } from "../../../components/ui/AppText";
import { SettingsActionRow } from "./SettingsActionRow";
import { serverEnvironment } from "../../../state/server";
import { useEnvironmentScope } from "../../../state/session";
import { useAtomCommand } from "../../../state/use-atom-command";

export function ClaudeSessionSharing({
  environmentId,
  instanceId,
}: {
  readonly environmentId: EnvironmentId;
  readonly instanceId: ProviderInstanceId;
}) {
  const { selectThreadRoute } = useAdaptiveWorkspaceLayout();
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
  async function openSession(sessionId: string, observeModelActivity = false) {
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
      },
    });
    gate.current = false;
    setBusy(false);
    if (result._tag === "Success") {
      selectThreadRoute({ environmentId, threadId: result.value.threadId });
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
    <View>
      <View className="gap-3 px-4 py-3">
        <Text className="text-sm text-foreground">Claude sessions</Text>
        <Text className="text-sm text-foreground-muted">
          Continue a saved Claude conversation as a shared session. Only sessions inside the shared
          session directory are shown.
        </Text>
        <TextInput
          accessibilityLabel="Search Claude sessions"
          className="rounded-lg border border-border-subtle px-3 py-2 text-base text-foreground"
          placeholderTextColorClassName="accent-foreground-muted"
          placeholder="Title, directory or session ID"
          value={query}
          maxLength={200}
          editable={!busy && canRead}
          autoCapitalize="none"
          autoCorrect={false}
          onChangeText={setQuery}
          onSubmitEditing={() => void load()}
          returnKeyType="search"
        />
        {error ? (
          <Text accessibilityRole="alert" className="text-sm text-danger-foreground">
            {error}
          </Text>
        ) : null}
        {sessions?.length === 0 ? (
          <Text className="text-sm text-foreground-muted">
            No matching Claude sessions. Check the search and shared session directory, then
            refresh.
          </Text>
        ) : null}
      </View>
      {query ? (
        <SettingsActionRow label="Clear search" disabled={busy} onPress={() => setQuery("")} />
      ) : null}
      <SettingsActionRow
        label={sessions === null ? "Browse sessions" : "Refresh"}
        loading={busy}
        disabled={!canRead}
        onPress={() => void load()}
      />
      {sessions?.map((session) => (
        <View key={session.sessionId}>
          <View className="gap-1 px-4 pt-3">
            <Text className="text-sm text-foreground">{session.title || "Claude session"}</Text>
            <Text selectable className="text-sm text-foreground-muted">
              {session.cwd}
            </Text>
            <Text className="text-sm text-foreground-muted">
              {session.state === "foreground"
                ? "Open in terminal"
                : session.state === "shared"
                  ? "Shared session"
                  : "Saved history"}
            </Text>
          </View>
          {!session.historyAvailable ? (
            <Text className="px-4 py-2 text-sm text-foreground-muted">
              Claude has not saved this conversation yet. History will appear after its next reply.
            </Text>
          ) : null}
          {session.state === "foreground" ? (
            <Text selectable className="px-4 py-3 text-sm text-foreground-muted">
              Run /bg in the original terminal, then refresh to open it here.
            </Text>
          ) : (
            <SettingsActionRow
              label={
                session.state === "shared" ? "Open shared session" : "Continue as shared session"
              }
              disabled={busy || !canOpen || query !== loadedQuery}
              onPress={() => void openSession(session.sessionId)}
            />
          )}
          {session.state === "history" && session.modelActivityAvailable === true ? (
            <SettingsActionRow
              label="Continue with model activity"
              description="Off by default. Shows temporary model responses until Claude saves them. Available for inactive history only."
              disabled={busy || !canOpen || query !== loadedQuery}
              onPress={() => void openSession(session.sessionId, true)}
            />
          ) : null}
        </View>
      ))}
      {nextOffset !== null ? (
        <SettingsActionRow
          label="Load more"
          disabled={busy || !canRead || query !== loadedQuery}
          onPress={() => void load(nextOffset)}
        />
      ) : null}
    </View>
  );
}
