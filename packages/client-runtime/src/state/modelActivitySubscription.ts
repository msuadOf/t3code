import { ModelActivityError, type ModelActivityFrame } from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";
import * as Stream from "effect/Stream";

const isModelActivityError = Schema.is(ModelActivityError);

/** Reopen a failed read from its retained snapshot without dropping the visible partial response. */
export const recoverModelActivity = <E, R>(stream: Stream.Stream<ModelActivityFrame, E, R>) =>
  Stream.suspend(() => {
    let epoch = "";
    let delay = 250;
    const follow = (): Stream.Stream<ModelActivityFrame, E, R> =>
      stream.pipe(
        Stream.tap((frame) =>
          Effect.sync(() => {
            epoch = frame.epoch;
            if (frame.kind === "snapshot" && frame.available) delay = 250;
          }),
        ),
        Stream.catch((error) => {
          if (!isModelActivityError(error) || error.reason !== "read-failed")
            return Stream.fail(error);
          const wait = delay;
          delay = Math.min(delay * 2, 5000);
          const disconnected: ModelActivityFrame = { kind: "disconnected", epoch };
          return Stream.make(disconnected).pipe(
            Stream.concat(Stream.fromEffect(Effect.sleep(wait)).pipe(Stream.drain)),
            Stream.concat(Stream.suspend(follow)),
          );
        }),
      );
    return follow();
  });
