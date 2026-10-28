import * as Schema from "effect/Schema";
import { ThreadId, TrimmedNonEmptyString, NonNegativeInt } from "./baseSchemas.ts";
import { ProviderInstanceId } from "./providerInstance.ts";

export const NativeSessionSharingListInput = Schema.Struct({
  instanceId: ProviderInstanceId,
  query: Schema.optional(Schema.String.check(Schema.isMaxLength(200))),
  offset: Schema.optional(NonNegativeInt),
});
export type NativeSessionSharingListInput = typeof NativeSessionSharingListInput.Type;
export const NativeSessionSharingCandidate = Schema.Struct({
  sessionId: TrimmedNonEmptyString,
  title: Schema.String,
  cwd: TrimmedNonEmptyString,
  updatedAt: Schema.Number,
  historyAvailable: Schema.Boolean,
  state: Schema.Literals(["history", "foreground", "shared"]),
});
export type NativeSessionSharingCandidate = typeof NativeSessionSharingCandidate.Type;
export const NativeSessionSharingListResult = Schema.Struct({
  sessions: Schema.Array(NativeSessionSharingCandidate),
  nextOffset: Schema.NullOr(NonNegativeInt),
});
export type NativeSessionSharingListResult = typeof NativeSessionSharingListResult.Type;
export const NativeSessionSharingOpenInput = Schema.Struct({
  instanceId: ProviderInstanceId,
  sessionId: Schema.String.check(
    Schema.isPattern(/^[a-f0-9]{8}-(?:[a-f0-9]{4}-){3}[a-f0-9]{12}$/i),
  ),
});
export type NativeSessionSharingOpenInput = typeof NativeSessionSharingOpenInput.Type;
export const NativeSessionSharingOpenResult = Schema.Struct({ threadId: ThreadId });
export type NativeSessionSharingOpenResult = typeof NativeSessionSharingOpenResult.Type;
export class NativeSessionSharingError extends Schema.TaggedError<NativeSessionSharingError>()(
  "NativeSessionSharingError",
  {
    reason: Schema.Literals([
      "unavailable",
      "outside-root",
      "foreground",
      "open-failed",
      "list-failed",
    ]),
  },
) {
  override get message(): string {
    switch (this.reason) {
      case "foreground":
        return "This session is open in a terminal. Run /bg in that terminal, then refresh sessions.";
      case "outside-root":
        return "This session is outside the shared session directory. Refresh sessions or update the directory setting.";
      case "unavailable":
        return "Native session sharing is unavailable for this provider.";
      case "list-failed":
        return "Could not read Claude history. Check the CLI and configured Claude directory, then refresh.";
      case "open-failed":
        return "Could not open the original Claude session. It may be in use or require terminal setup. Refresh sessions and try again.";
    }
  }
}
