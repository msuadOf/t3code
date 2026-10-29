import * as Schema from "effect/Schema";
import { NonNegativeInt, ThreadId } from "./baseSchemas.ts";

/** Transient model responses are not native conversation records. */
export const ModelActivity = Schema.Struct({
  seq: NonNegativeInt,
  sessionId: Schema.String,
  requestId: Schema.String,
  messageId: Schema.NullOr(Schema.String),
  type: Schema.String,
  data: Schema.Record(Schema.String, Schema.Unknown),
});
export type ModelActivity = typeof ModelActivity.Type;
export const ModelActivitySubscribeInput = Schema.Struct({
  threadId: ThreadId,
  after: Schema.optional(NonNegativeInt),
});
export type ModelActivitySubscribeInput = typeof ModelActivitySubscribeInput.Type;
export const ModelActivityFrame = Schema.Union([
  Schema.Struct({
    kind: Schema.Literal("snapshot"),
    available: Schema.Boolean,
    connectionState: Schema.optional(Schema.Literals(["connected", "disconnected"])),
    epoch: Schema.String,
    seq: NonNegativeInt,
    oldestSeq: NonNegativeInt,
    after: NonNegativeInt,
    gap: Schema.Boolean,
    events: Schema.Array(ModelActivity),
    confirmedMessageIds: Schema.Array(Schema.String),
  }),
  Schema.Struct({ kind: Schema.Literal("disconnected"), epoch: Schema.String }),
  Schema.Struct({ kind: Schema.Literal("event"), epoch: Schema.String, event: ModelActivity }),
  Schema.Struct({
    kind: Schema.Literal("confirmed"),
    epoch: Schema.String,
    messageIds: Schema.Array(Schema.String),
  }),
]);
export type ModelActivityFrame = typeof ModelActivityFrame.Type;
export class ModelActivityError extends Schema.TaggedError<ModelActivityError>()(
  "ModelActivityError",
  {
    reason: Schema.Literals(["unavailable", "outside-root", "read-failed"]),
  },
) {
  override get message(): string {
    return "Could not read this session's local model activity.";
  }
}
