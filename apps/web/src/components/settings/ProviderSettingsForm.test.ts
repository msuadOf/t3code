import { describe, expect, it } from "vite-plus/test";
import { makeProviderSettingsSchema, ProviderDriverKind, TrimmedString } from "@t3tools/contracts";
import * as Schema from "effect/Schema";

import { providerClients } from "./providerDriverMeta";
import {
  deriveProviderSettingsFields,
  nextProviderConfigWithFieldValue,
} from "./ProviderSettingsForm";

describe("ProviderSettingsForm helpers", () => {
  it("keeps every masked field as a stored secret", () => {
    const settingsSchema = makeProviderSettingsSchema({
      token: TrimmedString.pipe(
        Schema.annotateKey({ providerSettingsForm: { control: "password" } }),
      ),
    });

    expect(deriveProviderSettingsFields({ settingsSchema })).toMatchObject([
      { key: "token", secret: true },
    ]);
  });

  it("offers Claude model observation as a boolean opt-in rather than a text input", () => {
    const claude = providerClients.get(ProviderDriverKind.make("claudeAgent"));
    expect(claude).toBeDefined();
    const field = deriveProviderSettingsFields(claude!).find(
      (item) => item.key === "modelActivityForNewSessions",
    )!;
    expect(field.control).toBe("switch");
    const enabled = nextProviderConfigWithFieldValue(
      { modelActivityUpstreamUrl: "http://localhost:4000" },
      field,
      true,
    );
    expect(enabled).toEqual({
      modelActivityUpstreamUrl: "http://localhost:4000",
      modelActivityForNewSessions: true,
    });
    expect(nextProviderConfigWithFieldValue(enabled, field, false)).toEqual({
      modelActivityUpstreamUrl: "http://localhost:4000",
    });
  });

  it("preserves unknown config keys while omitting empty configurable fields", () => {
    const opencode = providerClients.get(ProviderDriverKind.make("opencode"));
    expect(opencode).toBeDefined();

    const serverUrl = deriveProviderSettingsFields(opencode!).find(
      (field) => field.key === "serverUrl",
    );
    expect(serverUrl).toBeDefined();

    const next = nextProviderConfigWithFieldValue(
      { forkOwned: 1, serverUrl: "http://127.0.0.1:4096" },
      serverUrl!,
      "",
    );

    expect(next).toEqual({ forkOwned: 1 });
  });

  it("can scope shared Codex sessions and reset sharing to the home daemon", () => {
    const codex = providerClients.get(ProviderDriverKind.make("codex"));
    expect(codex).toBeDefined();
    const fields = deriveProviderSettingsFields(codex!);
    const endpoint = fields.find((field) => field.key === "daemonEndpoint")!;
    const root = fields.find((field) => field.key === "sharedSessionRoot")!;
    expect(endpoint).toBeDefined();
    expect(root).toBeDefined();
    expect(
      fields.some((field) => field.key === "shadowHomePath" || field.key === "launchArgs"),
    ).toBe(false);
    const scoped = nextProviderConfigWithFieldValue(
      { homePath: "/home/codex" },
      root,
      "/work/project",
    );
    const connected = nextProviderConfigWithFieldValue(scoped, endpoint, "unix:///tmp/codex.sock");
    expect(connected).toEqual({
      homePath: "/home/codex",
      sharedSessionRoot: "/work/project",
      daemonEndpoint: "unix:///tmp/codex.sock",
    });
    expect(
      nextProviderConfigWithFieldValue(
        nextProviderConfigWithFieldValue(connected, root, ""),
        endpoint,
        "",
      ),
    ).toEqual({ homePath: "/home/codex" });
  });

  it("stores false boolean fields when true is the default", () => {
    const next = nextProviderConfigWithFieldValue(
      undefined,
      {
        key: "experimental",
        control: "switch",
        label: "Experimental",
        clearWhenEmpty: "omit",
        defaultBooleanValue: true,
      },
      false,
    );

    expect(next).toEqual({ experimental: false });
  });
});

it("offers native command backgrounding as an explicit off-by-default Claude switch", () => {
  const claude = providerClients.get(ProviderDriverKind.make("claudeAgent"));
  const field = deriveProviderSettingsFields(claude!).find(
    (item) => item.key === "autoBackgroundCommands",
  )!;
  expect(field.control).toBe("switch");
  const enabled = nextProviderConfigWithFieldValue({}, field, true);
  expect(enabled).toEqual({ autoBackgroundCommands: true });
  expect(nextProviderConfigWithFieldValue(enabled, field, false)).toBeUndefined();
});
