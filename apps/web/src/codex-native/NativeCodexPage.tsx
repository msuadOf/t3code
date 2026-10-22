import { useEffect, useEffectEvent, useLayoutEffect, useRef, useState } from "react";
import { Link } from "@tanstack/react-router";

import { APP_VERSION } from "../branding";
import { randomUUID } from "../lib/utils";
import ChatMarkdown from "../components/ChatMarkdown";
import { Button } from "../components/ui/button";
import { Input } from "../components/ui/input";
import { Textarea } from "../components/ui/textarea";
import {
  NativeCodexClient,
  applyThreadNotification,
  type NativeItem,
  type NativeServerRequest,
  type NativeThread,
  type NativeTurn,
} from "./protocol";

const record = (value: unknown): Record<string, unknown> =>
  typeof value === "object" && value !== null ? (value as Record<string, unknown>) : {};
const text = (value: unknown) => (typeof value === "string" ? value : "");
const errorText = (error: unknown) =>
  error instanceof Error ? error.message : "The Codex operation failed.";

interface ThreadPage {
  readonly data: NativeThread[];
  readonly nextCursor: string | null;
}
interface TurnPage {
  readonly data: NativeTurn[];
  readonly nextCursor: string | null;
}

export function NativeCodexPage() {
  const [connection, setConnection] = useState<"connecting" | "connected" | "disconnected">(
    "connecting",
  );
  const [generation, setGeneration] = useState(0);
  const [threads, setThreads] = useState<NativeThread[]>([]);
  const [listCursor, setListCursor] = useState<string | null>(null);
  const [thread, setThread] = useState<NativeThread>();
  const [turnCursor, setTurnCursor] = useState<string | null>(null);
  const [requests, setRequests] = useState<NativeServerRequest[]>([]);
  const [error, setError] = useState<string | null>(null);
  const [busy, setBusy] = useState(false);
  const [draft, setDraft] = useState("");
  const [nativeId, setNativeId] = useState("");
  const clientRef = useRef<NativeCodexClient | null>(null);
  const selectedRef = useRef<string | null>(null);
  const snapshotReady = useRef(false);
  const selectionVersion = useRef(0);
  const submitting = useRef(false);
  const drafts = useRef(new Map<string, string>());
  const conversation = useRef<HTMLElement | null>(null);
  const following = useRef(true);

  useLayoutEffect(() => {
    if (following.current && conversation.current) {
      conversation.current.scrollTop = conversation.current.scrollHeight;
    }
  });

  const listThreads = async (client: NativeCodexClient, cursor?: string) => {
    const page = await client.request<ThreadPage>("thread/list", {
      limit: 30,
      sortKey: "updated_at",
      sourceKinds: ["cli", "vscode", "appServer"],
      ...(cursor ? { cursor } : {}),
    });
    if (clientRef.current !== client) return;
    setThreads((current) =>
      cursor
        ? [...current, ...page.data.filter((item) => !current.some((old) => old.id === item.id))]
        : page.data,
    );
    setListCursor(page.nextCursor);
  };
  const initializeList = useEffectEvent(listThreads);

  useEffect(() => {
    let disposed = false;
    const url = new URL("/api/codex-native/ws", window.location.href);
    url.protocol = url.protocol === "https:" ? "wss:" : "ws:";
    const client = new NativeCodexClient(url.href, {
      onNotification: (notification) => {
        if (disposed) return;
        const params = record(notification.params);
        if (notification.method === "serverRequest/resolved") {
          setRequests((current) => current.filter((request) => request.id !== params.requestId));
        }
        if (notification.method === "thread/started") {
          const started = record(params.thread);
          if (typeof started.id === "string") {
            setThreads((current) => [
              started as unknown as NativeThread,
              ...current.filter((item) => item.id !== started.id),
            ]);
          }
        }
        if (params.threadId !== selectedRef.current) return;
        if (snapshotReady.current) {
          setThread((current) =>
            current ? applyThreadNotification(current, notification) : current,
          );
        }
      },
      onServerRequest: (request) => {
        if (disposed || record(request.params).threadId !== selectedRef.current) return;
        setRequests((current) => [...current.filter((old) => old.id !== request.id), request]);
      },
      onClose: (cause) => {
        if (disposed) return;
        setConnection("disconnected");
        setRequests([]);
        setError(cause.message);
      },
    });
    clientRef.current = client;
    void client
      .connect()
      .then(async () => {
        await client.request("initialize", {
          clientInfo: { name: "t3_native_codex", title: "T3 Native Codex", version: APP_VERSION },
          capabilities: { experimentalApi: true },
        });
        client.notify("initialized");
        if (disposed) return;
        await initializeList(client);
        if (disposed) return;
        setThread(undefined);
        selectedRef.current = null;
        setConnection("connected");
      })
      .catch((cause: unknown) => {
        if (!disposed) {
          setConnection("disconnected");
          setError(errorText(cause));
        }
      });
    return () => {
      disposed = true;
      selectionVersion.current += 1;
      selectedRef.current = null;
      snapshotReady.current = false;
      clientRef.current = null;
      client.close();
    };
    // eslint-disable-next-line react/exhaustive-effect-dependencies -- Explicit retries replace the socket; failed actions are never replayed.
  }, [generation]);

  const openThread = async (id: string) => {
    const client = clientRef.current;
    if (!client || connection !== "connected" || busy) return;
    const version = ++selectionVersion.current;
    const previous = selectedRef.current;
    if (previous) drafts.current.set(previous, draft);
    setDraft(drafts.current.get(id) ?? "");
    following.current = true;
    selectedRef.current = id;
    snapshotReady.current = false;
    setBusy(true);
    setError(null);
    setThread(undefined);
    setRequests([]);
    try {
      if (previous && previous !== id)
        await client.request("thread/unsubscribe", { threadId: previous });
      await client.request<{ thread: NativeThread; initialTurnsPage: TurnPage }>(
        "thread/resume",
        {
          threadId: id,
          excludeTurns: true,
          initialTurnsPage: { limit: 30, sortDirection: "desc", itemsView: "full" },
        },
        (resumed) => {
          if (version !== selectionVersion.current) return;
          if (!resumed.initialTurnsPage)
            throw new Error(
              "This Codex server does not support atomic session attachment. Update Codex to the latest release.",
            );
          // Resume establishes the snapshot/event boundary on the native listener.
          // Apply it in the response handler, before any subsequent notification.
          setThread({ ...resumed.thread, turns: resumed.initialTurnsPage.data.toReversed() });
          setTurnCursor(resumed.initialTurnsPage.nextCursor);
          setNativeId(id);
          snapshotReady.current = true;
        },
      );
    } catch (cause) {
      if (version === selectionVersion.current) {
        snapshotReady.current = false;
        setError(errorText(cause));
      }
    } finally {
      if (version === selectionVersion.current) setBusy(false);
    }
  };

  const runAction = async (action: () => Promise<unknown>) => {
    if (submitting.current) return;
    submitting.current = true;
    setBusy(true);
    setError(null);
    try {
      await action();
    } catch (cause) {
      setError(errorText(cause));
    } finally {
      submitting.current = false;
      setBusy(false);
    }
  };
  const activeTurn = thread?.turns.findLast((turn) => turn.status === "inProgress");
  const disabled = connection !== "connected" || busy;

  return (
    <main className="flex h-full min-h-0 flex-col bg-background pb-safe text-foreground">
      <header className="flex shrink-0 flex-wrap items-center justify-between gap-3 border-b border-border px-4 py-3">
        <div className="min-w-0">
          <h1 className="text-base font-semibold">Native Codex</h1>
          <p className="text-xs text-muted-foreground" role="status">
            {connection === "connected"
              ? "Connected to the shared Codex server"
              : connection === "connecting"
                ? "Connecting to Codex…"
                : "Disconnected"}
          </p>
        </div>
        <div className="flex gap-2">
          <Button variant="ghost" render={<Link to="/" />}>
            Back to T3
          </Button>
          {thread ? (
            <Button
              variant="outline"
              disabled={busy}
              onClick={() => {
                const id = selectedRef.current;
                if (id) drafts.current.set(id, draft);
                selectedRef.current = null;
                setThread(undefined);
                setRequests([]);
                if (id && clientRef.current)
                  void runAction(() =>
                    clientRef
                      .current!.request("thread/unsubscribe", { threadId: id })
                      .then(() => listThreads(clientRef.current!)),
                  );
              }}
            >
              Sessions
            </Button>
          ) : null}
          {connection === "disconnected" ? (
            <Button
              onClick={() => {
                setConnection("connecting");
                setBusy(false);
                setError(null);
                submitting.current = false;
                setGeneration((value) => value + 1);
              }}
            >
              Reconnect
            </Button>
          ) : (
            <Button
              variant="outline"
              disabled={disabled}
              onClick={() => void runAction(() => listThreads(clientRef.current!))}
            >
              Refresh list
            </Button>
          )}
        </div>
      </header>
      {error ? (
        <div
          className="shrink-0 border-b border-border px-4 py-3 text-sm text-destructive"
          role="alert"
        >
          {error}{" "}
          {connection === "disconnected"
            ? "Check that the shared Codex server is running on this host, then reconnect. Unconfirmed actions are never resent automatically."
            : null}
        </div>
      ) : null}
      {!thread ? (
        <section className="min-h-0 flex-1 overflow-y-auto scrollbar-gutter-both p-4">
          <div className="mx-auto max-w-3xl space-y-5">
            <p className="text-sm text-muted-foreground">
              Open the same session in your terminal and here. Both clients see the same
              conversation and running task.
            </p>
            <form
              noValidate
              className="flex items-end gap-2"
              onSubmit={(event) => {
                event.preventDefault();
                if (nativeId.trim()) void openThread(nativeId.trim());
              }}
            >
              <label className="min-w-0 flex-1 space-y-1 text-sm">
                Native session ID
                <Input
                  value={nativeId}
                  onChange={(event) => setNativeId(event.target.value)}
                  placeholder="Paste a Codex session ID"
                />
              </label>
              <Button type="submit" disabled={disabled || !nativeId.trim()}>
                {busy ? "Opening…" : "Open"}
              </Button>
            </form>
            <div className="divide-y divide-border rounded-lg border border-border">
              {threads.map((item) => (
                <button
                  type="button"
                  key={item.id}
                  disabled={disabled}
                  className="block w-full cursor-pointer space-y-1 p-4 text-left hover:bg-accent focus-visible:outline-2 focus-visible:outline-ring disabled:pointer-events-none disabled:opacity-60"
                  onClick={() => void openThread(item.id)}
                >
                  <span className="block truncate font-medium">
                    {item.name || item.title || item.preview || item.id}
                  </span>
                  <span className="block truncate text-xs text-muted-foreground">{item.cwd}</span>
                  <span className="block truncate font-mono text-xs text-muted-foreground">
                    {item.id}
                  </span>
                </button>
              ))}
              {threads.length === 0 ? (
                <p className="p-4 text-sm text-muted-foreground">
                  {connection === "connecting"
                    ? "Loading sessions…"
                    : "No sessions listed. Start a session in a terminal connected to this Codex server, then refresh."}
                </p>
              ) : null}
            </div>
            {listCursor ? (
              <Button
                variant="outline"
                disabled={disabled}
                onClick={() => void runAction(() => listThreads(clientRef.current!, listCursor))}
              >
                Load more sessions
              </Button>
            ) : null}
          </div>
        </section>
      ) : (
        <>
          <div className="shrink-0 border-b border-border px-4 py-2 text-xs text-muted-foreground">
            <p className="truncate font-medium text-foreground">
              {thread.name || thread.title || thread.preview || "Codex session"}
            </p>
            <p className="truncate font-mono">{thread.id}</p>
          </div>
          <section
            ref={conversation}
            aria-label="Conversation"
            className="min-h-0 flex-1 overflow-y-auto scrollbar-gutter-both overscroll-contain px-4 py-5"
            onScroll={(event) => {
              const element = event.currentTarget;
              following.current =
                element.scrollHeight - element.scrollTop - element.clientHeight < 80;
            }}
          >
            <div className="mx-auto max-w-3xl space-y-5">
              {turnCursor ? (
                <Button
                  variant="outline"
                  disabled={disabled}
                  onClick={() =>
                    void runAction(async () => {
                      const id = thread.id;
                      const page = await clientRef.current!.request<TurnPage>("thread/turns/list", {
                        threadId: id,
                        cursor: turnCursor,
                        limit: 30,
                        sortDirection: "desc",
                        itemsView: "full",
                      });
                      setThread((current) =>
                        !current || current.id !== id
                          ? current
                          : {
                              ...current,
                              turns: page.data
                                .toReversed()
                                .filter((old) => !current.turns.some((item) => item.id === old.id))
                                .concat(current.turns),
                            },
                      );
                      setTurnCursor(page.nextCursor);
                    })
                  }
                >
                  Load earlier turns
                </Button>
              ) : null}
              {thread.turns.map((turn) => (
                <article key={turn.id} className="space-y-4" aria-label={`Turn ${turn.status}`}>
                  {turn.items.map((item) => (
                    <NativeItemView
                      key={item.id}
                      item={item}
                      streaming={turn.status === "inProgress"}
                    />
                  ))}
                  {turn.status === "inProgress" ? (
                    <p className="text-xs text-muted-foreground" role="status">
                      Codex is working
                    </p>
                  ) : turn.status === "failed" ? (
                    <p className="text-sm text-destructive">
                      {text(record(turn.error).message) || "This turn failed."}
                    </p>
                  ) : null}
                </article>
              ))}
              {thread.turns.length === 0 ? (
                <p className="text-sm text-muted-foreground">This session has no turns yet.</p>
              ) : null}
              {requests.map((request) => (
                <NativeRequestCard
                  key={`${typeof request.id}:${request.id}`}
                  request={request}
                  disabled={disabled}
                  onRespond={(result) => {
                    try {
                      clientRef.current!.respond(request.id, result);
                      setRequests((current) => current.filter((item) => item.id !== request.id));
                    } catch (cause) {
                      setError(errorText(cause));
                    }
                  }}
                />
              ))}
            </div>
          </section>
          <form
            noValidate
            className="shrink-0 border-t border-border p-4"
            onSubmit={(event) => {
              event.preventDefault();
              if (disabled || !draft.trim()) return;
              const message = draft;
              void runAction(async () => {
                const input = [{ type: "text", text: message, text_elements: [] }];
                await clientRef.current!.request(activeTurn ? "turn/steer" : "turn/start", {
                  threadId: thread.id,
                  input,
                  clientUserMessageId: randomUUID(),
                  ...(activeTurn ? { expectedTurnId: activeTurn.id } : {}),
                });
                if (drafts.current.get(thread.id) === message) drafts.current.delete(thread.id);
                following.current = true;
                setDraft((current) => (current === message ? "" : current));
              });
            }}
          >
            <div className="mx-auto max-w-3xl space-y-2">
              <label className="block text-xs text-muted-foreground" htmlFor="native-codex-message">
                {activeTurn ? "Add instructions to the running task" : "Message Codex"}
              </label>
              <Textarea
                id="native-codex-message"
                style={{ resize: "none" }}
                disabled={busy}
                value={draft}
                onChange={(event) => {
                  setDraft(event.target.value);
                  drafts.current.set(thread.id, event.target.value);
                }}
                placeholder="Continue this session…"
              />
              <div className="flex justify-end gap-2">
                {activeTurn ? (
                  <Button
                    type="button"
                    variant="outline"
                    disabled={disabled}
                    onClick={() =>
                      void runAction(() =>
                        clientRef.current!.request("turn/interrupt", {
                          threadId: thread.id,
                          turnId: activeTurn.id,
                        }),
                      )
                    }
                  >
                    Stop task
                  </Button>
                ) : null}
                <Button type="submit" disabled={disabled || !draft.trim()}>
                  {busy ? "Sending…" : activeTurn ? "Add instructions" : "Send"}
                </Button>
              </div>
            </div>
          </form>
        </>
      )}
    </main>
  );
}

function NativeItemView({
  item,
  streaming,
}: {
  readonly item: NativeItem;
  readonly streaming: boolean;
}) {
  if (item.type === "agentMessage" || item.type === "plan") {
    return (
      <ChatMarkdown
        text={item.text ?? ""}
        cwd={undefined}
        isStreaming={streaming}
        parseRawHtml={false}
      />
    );
  }
  if (item.type === "userMessage") {
    return (
      <div className="ml-auto max-w-[95%] rounded-lg bg-muted px-4 py-3 whitespace-pre-wrap wrap-anywhere">
        {(item.content ?? []).map((part) => text(record(part).text)).join("\n")}
      </div>
    );
  }
  if (item.type === "reasoning") {
    return (
      <details className="rounded-lg border border-border px-3 py-2 text-sm text-muted-foreground">
        <summary className="cursor-pointer focus-visible:outline-2 focus-visible:outline-ring">
          Thinking
        </summary>
        <div className="pt-2">
          <ChatMarkdown
            text={[
              ...(item.summary ?? []),
              ...(item.content ?? []).filter((part): part is string => typeof part === "string"),
            ].join("\n\n")}
            cwd={undefined}
            isStreaming={streaming}
            parseRawHtml={false}
          />
        </div>
      </details>
    );
  }
  const title =
    item.type === "commandExecution"
      ? text(item.command) || "Command"
      : item.type === "fileChange"
        ? "File changes"
        : item.type === "mcpToolCall"
          ? `${text(item.server)} · ${text(item.tool)}`
          : item.type;
  return (
    <details className="rounded-lg border border-border px-3 py-2 text-sm">
      <summary className="cursor-pointer truncate focus-visible:outline-2 focus-visible:outline-ring">
        {title}
      </summary>
      <pre className="mt-2 max-h-80 overflow-auto font-mono text-xs whitespace-pre-wrap wrap-anywhere">
        {item.aggregatedOutput ?? JSON.stringify(item, null, 2)}
      </pre>
    </details>
  );
}

function NativeRequestCard({
  request,
  disabled,
  onRespond,
}: {
  readonly request: NativeServerRequest;
  readonly disabled: boolean;
  readonly onRespond: (result: unknown) => void;
}) {
  const params = record(request.params);
  const [answers, setAnswers] = useState<Record<string, string>>({});
  const approval =
    request.method === "item/commandExecution/requestApproval" ||
    request.method === "item/fileChange/requestApproval";
  const question = request.method === "item/tool/requestUserInput";
  const questions = Array.isArray(params.questions) ? params.questions.map(record) : [];
  return (
    <section className="space-y-3 rounded-lg border border-border p-4" aria-label="Codex request">
      <h2 className="font-medium">
        {approval
          ? "Codex needs approval"
          : question
            ? "Codex has a question"
            : "Continue this request in Codex CLI"}
      </h2>
      <p className="text-sm whitespace-pre-wrap wrap-anywhere">
        {text(params.reason) || text(params.command) || request.method}
      </p>
      <details className="text-sm">
        <summary className="cursor-pointer focus-visible:outline-2 focus-visible:outline-ring">
          Request details
        </summary>
        <pre className="mt-2 max-h-64 overflow-auto font-mono text-xs whitespace-pre-wrap wrap-anywhere">
          {JSON.stringify(params, null, 2)}
        </pre>
      </details>
      {approval ? (
        <div className="flex flex-wrap gap-2">
          {(
            [
              ["accept", "Approve once"],
              ["decline", "Decline"],
              ["cancel", "Cancel task"],
            ] as const
          )
            .filter(
              ([decision]) =>
                !Array.isArray(params.availableDecisions) ||
                params.availableDecisions.includes(decision),
            )
            .map(([decision, label]) => (
              <Button
                key={decision}
                variant={decision === "accept" ? "default" : "outline"}
                disabled={disabled}
                onClick={() => onRespond({ decision })}
              >
                {label}
              </Button>
            ))}
        </div>
      ) : null}
      {question ? (
        <form
          noValidate
          className="space-y-3"
          onSubmit={(event) => {
            event.preventDefault();
            if (disabled || questions.some((entry) => !answers[text(entry.id)]?.trim())) return;
            onRespond({
              answers: Object.fromEntries(
                questions.map((entry) => [text(entry.id), { answers: [answers[text(entry.id)]!] }]),
              ),
            });
          }}
        >
          {questions.map((entry) => (
            <label key={text(entry.id)} className="block space-y-1 text-sm">
              {text(entry.question)}
              {Array.isArray(entry.options) ? (
                <span className="block text-xs text-muted-foreground">
                  {entry.options.map((option) => text(record(option).label)).join(" · ")}
                </span>
              ) : null}
              <Input
                type={entry.isSecret === true ? "password" : "text"}
                value={answers[text(entry.id)] ?? ""}
                onChange={(event) =>
                  setAnswers((current) => ({ ...current, [text(entry.id)]: event.target.value }))
                }
              />
            </label>
          ))}
          <Button
            type="submit"
            disabled={
              disabled ||
              questions.length === 0 ||
              questions.some((entry) => !answers[text(entry.id)]?.trim())
            }
          >
            Answer
          </Button>
        </form>
      ) : null}
    </section>
  );
}
