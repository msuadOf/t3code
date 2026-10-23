import { Session, type SessionMessage } from "@opencode/client/effect";
import type { ProviderInstanceId } from "@t3tools/contracts";
import { ProviderDriverKind } from "@t3tools/contracts";
import { ProviderDriverError } from "@t3tools/provider-core/server/errors";
import type {
  NativeHistoryItem,
  NativeHistoryTurn,
  NativeSessionSource,
} from "@t3tools/provider-core/server/nativeSessions";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Stream from "effect/Stream";
import * as OpenCode2Client from "./OpenCode2Client.ts";
import type { OpenCode2Server } from "./OpenCode2Server.ts";

const textOf = (parts: ReadonlyArray<{ readonly type: string; readonly text?: string }>) =>
  parts.flatMap((part) => (part.type === "text" ? [part.text ?? ""] : [])).join("\n");

/** Use native user message IDs as execution boundaries shared with live adoption. */
export const mapNativeHistory = (
  messages: ReadonlyArray<SessionMessage.Info>,
  active: boolean,
): ReadonlyArray<NativeHistoryTurn> => {
  const turns: Array<{
    id: string;
    status: NativeHistoryTurn["status"];
    startedAt: number;
    completedAt: number | null;
    items: NativeHistoryItem[];
  }> = [];
  for (const message of messages) {
    if (message.type === "user") {
      turns.push({
        id: message.id,
        status: "completed",
        startedAt: DateTime.toEpochMillis(message.time.created),
        completedAt: null,
        items: [{ id: message.id, type: "user", text: message.text, status: "completed" }],
      });
      continue;
    }
    const turn = turns.at(-1);
    if (turn === undefined) continue;
    if (message.type === "idle") {
      turn.status =
        message.outcome === "failed"
          ? "failed"
          : message.outcome === "interrupted"
            ? "interrupted"
            : "completed";
      turn.completedAt = DateTime.toEpochMillis(message.time.created);
    }
    if (message.type !== "assistant") continue;
    if (message.error !== undefined) turn.status = "failed";
    if (message.time.completed !== undefined)
      turn.completedAt = DateTime.toEpochMillis(message.time.completed);
    const ordinals = { text: 0, reasoning: 0 };
    for (const part of message.content) {
      if (part.type === "text" || part.type === "reasoning") {
        turn.items.push({
          id: `${message.id}:${part.type}:${ordinals[part.type]++}`,
          type: part.type === "text" ? "assistant" : "reasoning",
          text: part.text,
          status: message.time.completed === undefined && active ? "running" : "completed",
        });
      } else if (part.type === "tool") {
        const state = part.state;
        turn.items.push({
          id: part.id,
          type: "tool",
          title: part.name,
          text:
            state.status === "completed"
              ? textOf(state.content)
              : state.status === "error"
                ? state.error.message
                : typeof state.input === "string"
                  ? state.input
                  : JSON.stringify(state.input),
          status:
            state.status === "error"
              ? "failed"
              : state.status === "completed"
                ? "completed"
                : "running",
        });
      }
    }
  }
  const current = turns.at(-1);
  if (active && current !== undefined) {
    current.status = "running";
    current.completedAt = null;
  }
  return turns;
};

export const makeNativeSessions = (
  server: OpenCode2Server["Service"],
  instanceId: ProviderInstanceId,
): NativeSessionSource => {
  const error = (cause: unknown) =>
    new ProviderDriverError({
      driver: ProviderDriverKind.make("opencode"),
      instanceId,
      detail: "Could not read shared OpenCode sessions.",
      cause,
    });
  return {
    list: server
      .withConnection(({ client }) =>
        Effect.gen(function* () {
          const request: NonNullable<Parameters<typeof client.session.list>[0]> = {
            limit: 100,
            order: "desc",
            parentID: null,
          };
          const sessions = yield* OpenCode2Client.paginate(request, (input) =>
            client.session.list(input),
          ).pipe(Stream.runCollect);
          const active = yield* client.session.active();
          return sessions.map((session) => ({
            id: session.id,
            cwd: session.location.directory,
            title: session.title ?? "OpenCode session",
            ...(session.model === undefined
              ? {}
              : { model: `${session.model.providerID}/${session.model.id}` }),
            createdAt: DateTime.toEpochMillis(session.time.created),
            updatedAt: DateTime.toEpochMillis(session.time.updated),
            archived: session.time.archived !== undefined,
            active: session.id in active,
          }));
        }),
      )
      .pipe(Effect.mapError(error)),
    history: (id) =>
      server
        .withConnection(({ client }) =>
          Effect.gen(function* () {
            const messages = yield* OpenCode2Client.paginate(
              { sessionID: Session.ID.make(id), limit: 100, order: "asc" as const },
              client.message.list,
            ).pipe(Stream.runCollect);
            const active = yield* client.session.active();
            return mapNativeHistory(messages, id in active);
          }),
        )
        .pipe(Effect.mapError(error)),
  };
};
