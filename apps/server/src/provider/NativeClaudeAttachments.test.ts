// @effect-diagnostics nodeBuiltinImport:off - Isolated persisted attachment fixtures.
import * as NodeFSP from "node:fs/promises";
import * as NodeOS from "node:os";
import * as NodePath from "node:path";
import { afterEach, expect, it } from "@effect/vitest";
import type { ChatAttachment } from "@t3tools/contracts";
import { nativeClaudeAttachmentPrompt } from "./NativeClaudeAttachments.ts";
const roots: string[] = [];
afterEach(async () => {
  await Promise.all(
    roots.splice(0).map((root) => NodeFSP.rm(root, { recursive: true, force: true })),
  );
});
const fixture = async () => {
  const root = await NodeFSP.mkdtemp(NodePath.join(NodeOS.tmpdir(), "native-attachment-"));
  roots.push(root);
  const path = NodePath.join(root, "notes (中文).txt");
  await NodeFSP.writeFile(path, "native");
  const attachment: ChatAttachment = {
    id: "stored-attachment",
    type: "file",
    name: "notes.txt",
    mimeType: "text/plain",
    sizeBytes: 6,
  };
  return { path, attachment };
};
it("uses native quoted @ references for owned stored files and preserves text exactly", async () => {
  const { path, attachment } = await fixture();
  expect(
    await nativeClaudeAttachmentPrompt({
      text: "Inspect",
      attachments: [attachment],
      resolveAttachmentPath: () => path,
    }),
  ).toBe(`Inspect\n\n@"${path}"`);
  expect(
    await nativeClaudeAttachmentPrompt({
      text: "",
      attachments: [attachment],
      resolveAttachmentPath: () => path,
    }),
  ).toBe(`@"${path}"`);
});
it("does not require a resolver or alter text without attachments", async () => {
  expect(await nativeClaudeAttachmentPrompt({ text: " exact\n", attachments: [] })).toBe(
    " exact\n",
  );
});
it("rejects absent, unowned, remote, unsafe, changed and symlink paths before sending", async () => {
  const { path, attachment } = await fixture();
  for (const value of [
    null,
    "relative.txt",
    "https://example.com/file",
    path + '"',
    path + "\\x",
    path + "\n",
    path + ".missing",
  ]) {
    await expect(
      nativeClaudeAttachmentPrompt({
        text: "Inspect",
        attachments: [attachment],
        resolveAttachmentPath: () => value,
      }),
    ).rejects.toThrow();
  }
  await expect(
    nativeClaudeAttachmentPrompt({ text: "", attachments: [attachment] }),
  ).rejects.toThrow("resolver");
  await expect(
    nativeClaudeAttachmentPrompt({
      text: "",
      attachments: [{ ...attachment, sizeBytes: 7 }],
      resolveAttachmentPath: () => path,
    }),
  ).rejects.toThrow("changed");
  await NodeFSP.symlink(path, path + ".link");
  await expect(
    nativeClaudeAttachmentPrompt({
      text: "",
      attachments: [attachment],
      resolveAttachmentPath: () => path + ".link",
    }),
  ).rejects.toThrow("changed");
});
it("rejects unknown MIME/types and mismatched image extensions", async () => {
  const { path, attachment } = await fixture();
  for (const value of [
    { ...attachment, mimeType: "application/octet-stream" },
    { ...attachment, type: "video" },
    { ...attachment, type: "image", mimeType: "image/svg+xml" },
    { ...attachment, type: "image", mimeType: "image/png" },
  ]) {
    await expect(
      nativeClaudeAttachmentPrompt({
        text: "",
        attachments: [value],
        resolveAttachmentPath: () => path,
      }),
    ).rejects.toThrow();
  }
});
