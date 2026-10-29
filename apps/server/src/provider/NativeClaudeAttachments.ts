// @effect-diagnostics nodeBuiltinImport:off - Native CLI attachment references require local file validation.
import type { ChatAttachment } from "@t3tools/contracts";
import * as NodeFSP from "node:fs/promises";
import * as NodePath from "node:path";

const imageExtensions: Readonly<Record<string, readonly string[]>> = {
  "image/png": [".png"],
  "image/jpeg": [".jpg", ".jpeg"],
  "image/gif": [".gif"],
  "image/webp": [".webp"],
};
const textMimeTypes = new Set([
  "application/json",
  "application/ld+json",
  "application/javascript",
  "application/typescript",
  "application/xml",
  "application/yaml",
  "application/x-yaml",
]);

/** Native @ references are expanded by the existing worker, not by a second SDK process. */
export async function nativeClaudeAttachmentPrompt(input: {
  readonly text: string;
  readonly attachments: readonly ChatAttachment[];
  readonly resolveAttachmentPath?: ((attachment: ChatAttachment) => string | null) | undefined;
}): Promise<string> {
  if (input.attachments.length === 0) return input.text;
  if (!input.resolveAttachmentPath)
    throw new Error("Claude attachments require the server's stored attachment resolver.");
  const references: string[] = [];
  for (const attachment of input.attachments) {
    const mime = attachment.mimeType.toLowerCase();
    const image = attachment.type === "image" ? imageExtensions[mime] : undefined;
    if (
      (attachment.type === "image" && !image) ||
      (attachment.type === "file" && !mime.startsWith("text/") && !textMimeTypes.has(mime)) ||
      (attachment.type !== "image" && attachment.type !== "file")
    )
      throw new Error(`Claude native attachments do not support MIME type '${mime}'.`);
    const path = input.resolveAttachmentPath(attachment);
    // Native quoted @ references have no verified escape syntax for quotes/backslashes.
    if (
      !path ||
      !NodePath.isAbsolute(path) ||
      [...path].some(
        (char) =>
          char === '"' || char === "\\" || char.charCodeAt(0) < 32 || char.charCodeAt(0) === 127,
      )
    )
      throw new Error("Claude attachment must resolve to a supported server-owned local path.");
    if (image && !image.includes(NodePath.extname(path).toLowerCase()))
      throw new Error("Claude attachment image extension does not match its MIME type.");
    const stat = await NodeFSP.lstat(path).catch(() => undefined);
    if (!stat?.isFile() || stat.isSymbolicLink() || stat.size !== attachment.sizeBytes)
      throw new Error("Claude attachment is unavailable or changed; attach it again.");
    // Stock 2.1.312 preserves this exact text in its native user JSONL record.
    references.push(`@"${path}"`);
  }
  return [input.text, references.join("\n")].filter((part) => part.length > 0).join("\n\n");
}
