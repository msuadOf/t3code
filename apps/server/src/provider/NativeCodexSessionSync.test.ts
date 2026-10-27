import { describe, expect, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as Path from "effect/Path";
import { collectPages, isWithinSharedRoot } from "./NativeCodexSessionSync.ts";

describe("NativeCodexSessionSync", () => {
  it.effect("discovers every page without a session or history cap", () =>
    Effect.gen(function* () {
      const cursors: Array<string | undefined> = [];
      const values = yield* collectPages((cursor) =>
        Effect.sync(() => {
          cursors.push(cursor);
          const page = cursor === undefined ? 0 : Number(cursor);
          return {
            data: Array.from({ length: 100 }, (_, i) => page * 100 + i),
            nextCursor: page === 4 ? null : String(page + 1),
          };
        }),
      );
      expect(cursors).toEqual([undefined, "1", "2", "3", "4"]);
      expect(values).toHaveLength(500);
      expect(values.at(-1)).toBe(499);
    }),
  );
  it.effect("rejects a repeated cursor without leaving discovery in an infinite loop", () =>
    Effect.gen(function* () {
      const result = yield* collectPages(() =>
        Effect.succeed({ data: [1], nextCursor: "repeated" }),
      ).pipe(Effect.result);
      expect(result._tag).toBe("Failure");
      if (result._tag === "Failure") expect(result.failure._tag).toBe("NativeCodexPaginationError");
    }),
  );
  it.effect("matches a directory and descendants while excluding prefix siblings", () =>
    Effect.gen(function* () {
      const path = yield* Path.Path;
      expect(isWithinSharedRoot(path, "/projects/app", "/projects/app")).toBe(true);
      expect(isWithinSharedRoot(path, "/projects/app/packages/ui", "/projects/app/")).toBe(true);
      expect(isWithinSharedRoot(path, "/projects/app-other", "/projects/app")).toBe(false);
      expect(isWithinSharedRoot(path, "/projects/app/../secret", "/projects/app")).toBe(false);
      expect(isWithinSharedRoot(path, "/elsewhere", "")).toBe(true);
    }).pipe(Effect.provide(Path.layer)),
  );
});
