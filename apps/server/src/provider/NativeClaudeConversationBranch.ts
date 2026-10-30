type Entry = Record<string, unknown>;
const object = (value: unknown): value is Entry =>
  value !== null && typeof value === "object" && !Array.isArray(value);

const resultBlocks = (entry: Entry): readonly Entry[] =>
  object(entry.message) && Array.isArray(entry.message.content)
    ? entry.message.content.filter(object)
    : [];

/** Follow the last persisted conversation leaf so abandoned /rewind branches are not replayed. */
export function nativeClaudeConversationBranch(entries: readonly Entry[]): readonly Entry[] {
  const nodes = entries.filter(
    (entry) => typeof entry.uuid === "string" && entry.isSidechain !== true,
  );
  const parents = new Set(nodes.map((entry) => entry.parentUuid));
  const byId = new Map(
    entries
      .filter((entry) => typeof entry.uuid === "string")
      .map((entry) => [entry.uuid as string, entry]),
  );
  // Native persistence can flush an assistant before its buffered user/attachment ancestors.
  let cursor = nodes.findLast((entry) => !parents.has(entry.uuid));
  const selected: Entry[] = [];
  const seen = new Set<string>();
  while (cursor && typeof cursor.uuid === "string" && !seen.has(cursor.uuid)) {
    seen.add(cursor.uuid);
    if (
      (cursor.type === "user" || cursor.type === "assistant" || cursor.type === "attachment") &&
      cursor.isSidechain !== true
    )
      selected.push(cursor);
    cursor = typeof cursor.parentUuid === "string" ? byId.get(cursor.parentUuid) : undefined;
  }
  const ordered = selected.toReversed();
  const selectedIds = new Set(ordered.map((entry) => entry.uuid));
  const resultIds = new Set<string>();
  for (const entry of ordered)
    if (entry.type === "user" && object(entry.message) && Array.isArray(entry.message.content))
      for (const block of entry.message.content)
        if (object(block) && block.type === "tool_result" && typeof block.tool_use_id === "string")
          resultIds.add(block.tool_use_id);
  const siblings = new Map<string, Entry[]>();
  for (const entry of entries) {
    if (
      entry.type !== "user" ||
      entry.isSidechain === true ||
      typeof entry.uuid !== "string" ||
      selectedIds.has(entry.uuid) ||
      typeof entry.parentUuid !== "string" ||
      !object(entry.message) ||
      !Array.isArray(entry.message.content) ||
      entry.message.content.length === 0
    )
      continue;
    const parent = byId.get(entry.parentUuid);
    if (
      !parent ||
      !selectedIds.has(parent.uuid) ||
      parent.type !== "assistant" ||
      !object(parent.message) ||
      !Array.isArray(parent.message.content)
    )
      continue;
    const calls = new Set(
      parent.message.content
        .filter(object)
        .filter((block) => block.type === "tool_use")
        .map((block) => block.id),
    );
    const content = entry.message.content;
    if (
      !content.every(
        (block) =>
          object(block) &&
          block.type === "tool_result" &&
          typeof block.tool_use_id === "string" &&
          calls.has(block.tool_use_id) &&
          !resultIds.has(block.tool_use_id),
      )
    )
      continue;
    const list = siblings.get(entry.parentUuid) ?? [];
    list.push(entry);
    siblings.set(entry.parentUuid, list);
  }
  // Parallel tool receipts may be siblings rather than ancestors of the current leaf.
  return ordered.flatMap((entry) => {
    const receipts = typeof entry.uuid === "string" ? (siblings.get(entry.uuid) ?? []) : [];
    const counts = new Map<string, number>();
    for (const receipt of receipts)
      for (const block of resultBlocks(receipt))
        counts.set(String(block.tool_use_id), (counts.get(String(block.tool_use_id)) ?? 0) + 1);
    return [
      entry,
      ...receipts.filter((receipt) =>
        resultBlocks(receipt).every((block) => counts.get(String(block.tool_use_id)) === 1),
      ),
    ];
  });
}
