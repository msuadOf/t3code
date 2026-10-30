import type { ModelActivityRequest } from "./modelActivity.ts";

export const modelActivityRequestKey = (request: Pick<ModelActivityRequest, "sessionId" | "id">) =>
  `${request.sessionId}:${request.id}`;

/** Never jump back to old, unconfirmed auxiliary requests after the followed message saves. */
export function resolveModelActivityFollowTarget(
  requests: ReadonlyArray<Pick<ModelActivityRequest, "sessionId" | "id" | "confirmed" | "status">>,
  previousTarget: string | null,
): string | null {
  const previousIndex = requests.findIndex(
    (request) => modelActivityRequestKey(request) === previousTarget,
  );
  const floor = Math.max(0, previousIndex);
  let fallback: string | null = null;
  for (let index = requests.length - 1; index >= floor; index--) {
    const request = requests[index];
    if (
      !request ||
      (request.confirmed && request.status !== "running") ||
      request.status === "failed"
    )
      continue;
    const key = modelActivityRequestKey(request);
    if (request.status === "running") return key;
    fallback ??= key;
  }
  // Keep the response's final lines in view while native persistence catches up.
  const previous = requests[previousIndex];
  if (previous && !previous.confirmed && previous.status === "completed") return previousTarget;
  return fallback;
}

/** Scroll only the observed request's end, never the end of the newest-first list. */
export function modelActivityFollowOffset(input: {
  readonly following: boolean;
  readonly targetBottom: number | null;
  readonly viewportHeight: number;
}): number | null {
  if (
    !input.following ||
    input.targetBottom === null ||
    !Number.isFinite(input.targetBottom) ||
    !Number.isFinite(input.viewportHeight) ||
    input.viewportHeight <= 0
  )
    return null;
  return Math.max(0, input.targetBottom - input.viewportHeight);
}
