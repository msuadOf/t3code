type Entry = Readonly<Record<string, unknown>>;
const object = (value: unknown): value is Entry =>
  value !== null && typeof value === "object" && !Array.isArray(value);

/** Repeated blocks share a model ID; IDs reused by distinct native owners are ambiguous. */
export function nativeClaudeConfirmedModelIds(
  sessionId: string,
  main: readonly Entry[],
  children: ReadonlyArray<{ readonly agentId: string; readonly entries: readonly Entry[] }>,
): readonly string[] {
  const messages = new Map<string, { owner: string; at: number } | null>();
  const collect = (entries: readonly Entry[], owner: string) => {
    const recent = new Set<string>();
    for (let index = entries.length - 1; index >= 0 && recent.size < 256; index--) {
      const entry = entries[index]!;
      if (entry.type !== "assistant" || entry.sessionId !== sessionId || !object(entry.message))
        continue;
      const id = entry.message.id;
      if (typeof id !== "string" || !id || id.length > 1024) continue;
      recent.add(id);
      const at = typeof entry.timestamp === "string" ? Date.parse(entry.timestamp) : NaN;
      const previous = messages.get(id);
      if (previous === null || (previous && previous.owner !== owner)) messages.set(id, null);
      else if (!previous) messages.set(id, { owner, at: Number.isFinite(at) ? at : 0 });
    }
  };
  collect(main, "main");
  for (const child of children) collect(child.entries, child.agentId);
  return [...messages]
    .filter((entry): entry is [string, { owner: string; at: number }] => entry[1] !== null)
    .sort((a, b) => b[1].at - a[1].at)
    .slice(0, 256)
    .map(([id]) => id);
}
