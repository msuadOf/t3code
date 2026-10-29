// @effect-diagnostics nodeBuiltinImport:off -- isolated ownership metadata filesystem fixture.
import * as NodeFSP from "node:fs/promises";
import * as NodeOS from "node:os";
import * as NodePath from "node:path";
import { expect, it } from "@effect/vitest";
import {
  associateNativeClaudeFork,
  nativeClaudeForkOwner,
  prepareNativeClaudeFork,
} from "./NativeClaudeForks.ts";

it("persists private fork provenance before launch and reads it after reopening the registry", async () => {
  const root = await NodeFSP.mkdtemp(NodePath.join(NodeOS.tmpdir(), "t3-native-fork-"));
  try {
    const owner = {
      sourceSessionId: "1234abcd-1234-1234-1234-123456789abc",
      targetThreadId: "target",
    };
    const settings = await prepareNativeClaudeFork(root, owner);
    expect(await nativeClaudeForkOwner(root, ["--settings", settings])).toEqual(owner);
    expect(await nativeClaudeForkOwner(root, ["--settings", settings])).toEqual(owner);
    expect(
      await nativeClaudeForkOwner(root, ["--settings", NodePath.join(root, "arbitrary.json")]),
    ).toBeUndefined();
    expect((await NodeFSP.stat(settings)).mode & 0o777).toBe(0o600);
    const sessionId = "fedcba98-1234-1234-1234-123456789abc";
    await associateNativeClaudeFork(root, settings, sessionId);
    expect(await nativeClaudeForkOwner(root, undefined, sessionId)).toEqual(owner);
    await NodeFSP.chmod(settings, 0o644);
    await expect(nativeClaudeForkOwner(root, ["--settings", settings])).rejects.toThrow("private");
  } finally {
    await NodeFSP.rm(root, { recursive: true, force: true });
  }
});

it("does not trust a symlinked T3 registry or an unregistered CLI settings flag", async () => {
  const root = await NodeFSP.mkdtemp(NodePath.join(NodeOS.tmpdir(), "t3-native-fork-"));
  try {
    const outside = NodePath.join(root, "outside");
    await NodeFSP.mkdir(outside, { mode: 0o700 });
    await NodeFSP.symlink(outside, NodePath.join(root, "t3-native-forks"));
    await expect(
      prepareNativeClaudeFork(root, {
        sourceSessionId: "1234abcd-1234-1234-1234-123456789abc",
        targetThreadId: "target",
      }),
    ).rejects.toThrow("private");
  } finally {
    await NodeFSP.rm(root, { recursive: true, force: true });
  }
});
