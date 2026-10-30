import { useAtomValue } from "@effect/atom-react";
import {
  AuthProvidersManageScope,
  type EnvironmentId,
  type ServerProvider,
} from "@t3tools/contracts";
import { squashAtomCommandFailure } from "@t3tools/client-runtime/state/runtime";
import { AsyncResult } from "effect/reactivity";
import { useRef, useState } from "react";
import { TextInput, View } from "react-native";

import { AppText as Text } from "../../../components/ui/AppText";
import { appAtomRegistry } from "../../../state/atom-registry";
import { serverEnvironment } from "../../../state/server";
import { readEnvironmentScope, useEnvironmentScope } from "../../../state/session";
import { useAtomCommand } from "../../../state/use-atom-command";
import { SettingsSwitchRow } from "./SettingsSwitchRow";
import { SettingsActionRow } from "./SettingsActionRow";

function configRecord(config: unknown): Record<string, unknown> {
  return typeof config === "object" && config !== null && !Array.isArray(config)
    ? (config as Record<string, unknown>)
    : {};
}

export function NativeSharedSessionSettings({
  environmentId,
  provider,
  disabled,
}: {
  readonly environmentId: EnvironmentId;
  readonly provider: ServerProvider;
  readonly disabled: boolean;
}) {
  const snapshot = useAtomValue(serverEnvironment.configValueAtom(environmentId));
  const instance = snapshot?.settings.providerInstances[provider.instanceId];
  const saved = configRecord(instance?.config);
  const isCodex = provider.driver === "codex";
  const isClaude = provider.driver === "claudeAgent";
  const endpointKey = isCodex ? "daemonEndpoint" : "serverUrl";
  const savedEndpoint = saved[endpointKey];
  const endpoint = typeof savedEndpoint === "string" ? savedEndpoint : "";
  const name = provider.displayName ?? (isCodex ? "Codex" : isClaude ? "Claude" : "OpenCode");
  const root = typeof saved.sharedSessionRoot === "string" ? saved.sharedSessionRoot : "";
  const upstream =
    typeof saved.modelActivityUpstreamUrl === "string" ? saved.modelActivityUpstreamUrl : "";
  const observeNew = saved.modelActivityForNewSessions === true;
  const [draft, setDraft] = useState<{
    endpoint: string;
    root: string;
    upstream: string;
    observeNew: boolean;
  } | null>(null);
  const [saving, setSaving] = useState(false);
  const savingRef = useRef(false);
  const [error, setError] = useState<string | null>(null);
  const canManage = useEnvironmentScope(environmentId, AuthProvidersManageScope);
  const updateSettings = useAtomCommand(serverEnvironment.mutateProviderInstance);
  const value = draft ?? { endpoint, root, upstream, observeNew };
  const needsCliLogin = isCodex && saved.setupMode === "managed";
  const dirty =
    needsCliLogin ||
    value.endpoint.trim() !== endpoint ||
    value.root.trim() !== root ||
    (isClaude && (value.upstream.trim() !== upstream || value.observeNew !== observeNew));
  const unavailable = disabled || !canManage || saving;

  async function save() {
    if (unavailable || savingRef.current || !dirty) return;
    if (isClaude && value.observeNew && !value.upstream.trim()) {
      setError("Enter the model activity upstream URL before enabling observation.");
      return;
    }
    if (!readEnvironmentScope(environmentId, AuthProvidersManageScope)) return;
    const latest = appAtomRegistry.get(serverEnvironment.configValueAtom(environmentId));
    const current = latest?.settings.providerInstances[provider.instanceId];
    if (!current) {
      setError("This provider is no longer available. Refresh providers and try again.");
      return;
    }
    savingRef.current = true;
    setSaving(true);
    setError(null);
    const result = await updateSettings({
      environmentId,
      input: {
        patch: {},
        providerInstanceMutation: {
          operation: "upsert",
          instanceId: provider.instanceId,
          instance: {
            ...current,
            config: {
              ...configRecord(current.config),
              ...(isCodex ? { setupMode: "existing", shadowHomePath: "", launchArgs: "" } : {}),
              ...(!isClaude ? { [endpointKey]: value.endpoint.trim() } : {}),
              sharedSessionRoot: value.root.trim(),
              ...(isClaude
                ? {
                    modelActivityUpstreamUrl: value.upstream.trim(),
                    modelActivityForNewSessions: value.observeNew,
                  }
                : {}),
            },
          },
        },
      },
    });
    savingRef.current = false;
    setSaving(false);
    if (AsyncResult.isFailure(result)) {
      const cause = squashAtomCommandFailure(result);
      setError(cause instanceof Error ? cause.message : `Could not save ${name} settings.`);
    } else {
      setDraft(null);
    }
  }

  return (
    <View>
      <View className="gap-3 px-4 pb-4">
        <Text className="text-sm text-foreground-muted">
          {isClaude
            ? "Live control uses Claude background sessions. Existing foreground sessions must be moved to the background first."
            : `Connect CLI clients to the same ${name} server to share live sessions.`}
        </Text>
        {!isClaude ? (
          <>
            <Text className="text-sm text-foreground">Shared {name} server</Text>
            <TextInput
              accessibilityLabel={`Shared ${name} server`}
              className="rounded-lg border border-border-subtle px-3 py-2 text-base text-foreground"
              placeholderTextColorClassName="accent-foreground-muted"
              placeholder={isCodex ? "Default daemon for CODEX_HOME" : "Native OpenCode service"}
              value={value.endpoint}
              editable={!unavailable}
              autoCapitalize="none"
              autoCorrect={false}
              onChangeText={(endpoint) => setDraft({ ...value, endpoint })}
            />
          </>
        ) : null}
        <Text className="text-sm text-foreground">Shared session directory</Text>
        <TextInput
          accessibilityLabel="Shared session directory"
          className="rounded-lg border border-border-subtle px-3 py-2 text-base text-foreground"
          placeholderTextColorClassName="accent-foreground-muted"
          placeholder="All directories"
          value={value.root}
          editable={!unavailable}
          autoCapitalize="none"
          autoCorrect={false}
          onChangeText={(root) => setDraft({ ...value, root })}
        />
        <Text className="text-sm text-foreground-muted">
          Includes subdirectories on the environment’s machine. Leave empty to share all sessions.
        </Text>
        {isClaude ? (
          <>
            <Text className="text-sm text-foreground">Model activity upstream URL</Text>
            <TextInput
              accessibilityLabel="Model activity upstream URL"
              className="rounded-lg border border-border-subtle px-3 py-2 text-base text-foreground"
              placeholderTextColorClassName="accent-foreground-muted"
              placeholder="Your API gateway URL"
              value={value.upstream}
              editable={!unavailable}
              autoCapitalize="none"
              autoCorrect={false}
              keyboardType="url"
              onChangeText={(upstream) => setDraft({ ...value, upstream })}
            />
            <Text className="text-sm text-foreground-muted">
              Optional local observation of model responses. API key or API gateway configurations
              only; not Claude subscription / OAuth login. Use the same upstream your CLI already
              uses. Do not put API keys in the URL.
            </Text>
            <Text className="text-sm text-foreground-muted">
              Off by default. Enabling this applies to new Claude sessions and routes their model
              requests through this environment’s local observer. Observed responses are not the
              saved conversation.
            </Text>
          </>
        ) : null}
        {error ? (
          <Text accessibilityRole="alert" className="text-sm text-danger-foreground">
            {error}
          </Text>
        ) : null}
      </View>
      {isClaude ? (
        <SettingsSwitchRow
          label="Observe model activity for new sessions"
          subtitle="API key / gateway only. Existing sessions are unchanged."
          value={value.observeNew}
          disabled={unavailable}
          onValueChange={(observeNew) => setDraft({ ...value, observeNew })}
        />
      ) : null}
      <SettingsActionRow
        icon="checkmark"
        label={needsCliLogin ? "Use CLI login" : `Save ${name} sharing settings`}
        disabled={unavailable || !dirty}
        loading={saving}
        onPress={() => void save()}
      />
      {dirty ? (
        <SettingsActionRow
          icon="xmark"
          label="Discard changes"
          disabled={saving}
          onPress={() => setDraft(null)}
        />
      ) : null}
    </View>
  );
}
