# Codex

Use your ChatGPT plan or an existing Codex CLI login to code in T3 Code.

## Connect with ChatGPT

Connect during onboarding or in **Settings → Providers**. For a remote machine,
select that environment first. T3 Code handles Codex installation; sign in on
OpenAI and allow sharing of your ChatGPT plan.

Manage shared usage and credits in ChatGPT through **Manage usage** in T3 Code.
If a request uses a feature that ChatGPT sharing does not support, use another
provider for that request.

When reconnecting, choose the same account in T3 Code and on OpenAI's sign-in
page. Disconnecting stops running threads but keeps their history and lets you
reconnect later.

If remote sign-in cannot return automatically, paste the full URL from the final
localhost page into the sign-in panel, even if that page could not load.

## Use an existing Codex login

T3 Code can use your installed Codex and its existing login. Run `codex login`
on the environment's machine to sign in. [Provider setup](./install.md#providers)
covers installation and custom configuration.

## Share a live session with Codex CLI

Open **Native Codex** from the sidebar to use a session simultaneously in a terminal
and a phone browser. This mode connects directly to an existing Codex server. Its
sessions use native Codex IDs and the server's model, login, tools, and permissions.

On the remote machine, start the current Codex daemon and connect your terminal:

```bash
codex app-server daemon start
codex --remote unix://
```

Start this T3 fork on that same machine, open its authenticated web address on your
phone, and select **Native Codex**. Choose the terminal session or paste its native
ID. To resume it in another terminal, run `codex --remote unix:// resume <session-id>`.
Both clients must connect to the same server; sharing a history directory alone
does not join an already running session.

The default connection uses the daemon socket under `CODEX_HOME` (otherwise
`~/.codex`). Set `T3CODE_CODEX_NATIVE_ENDPOINT=unix:///absolute/path/to/socket`
on the T3 server for another socket. `ws://` and `wss://` endpoints are also supported.
The native connector currently targets macOS and Linux hosts; it does not configure
upstream WebSocket bearer authentication. Use the local daemon socket on the T3 host.
This mode requires current Codex support for atomic paginated session attachment;
there is no older-protocol fallback.

Replies, exposed thinking summaries, tool activity, and pending requests update
from either client. Approvals answered elsewhere disappear here. Requests without
a supported control remain available in Codex CLI. Closing the page disconnects
only that client. After a connection failure, reconnect and reopen the session;
unconfirmed messages and answers are never automatically resent.

When joining a task mid-reply, Codex may initially supply only subsequent output.
The completed reply replaces that partial text when the native item finishes.

Native Codex is served by this fork's web server, including access from a phone
over a private network. It is separate from T3's ordinary threads and does not
provide T3 checkpoints, provider switching, or native React Native app integration.

## Use multiple accounts

Add another ChatGPT account in **Settings → Providers**, then select the account
from the thread's model picker. Compatible accounts can continue the same thread.
Connecting accounts through T3 Code leaves your CLI login unchanged.

### Multiple CLI logins

A shared Codex home with a shadow home lets work and personal accounts continue
the same threads. The accounts share Codex sessions and configuration while keeping
their own login and available models.

Keep your first account in `~/.codex`. On the environment's machine, sign the
second account into a fresh directory:

```bash
mkdir -p ~/.codex_personal
CODEX_HOME=~/.codex_personal codex login
```

Then add a second Codex instance in **Settings > Providers**:

| Instance       | CODEX_HOME path | Shadow home path    |
| -------------- | --------------- | ------------------- |
| Codex Work     | `~/.codex`      | Leave empty         |
| Codex Personal | `~/.codex`      | `~/.codex_personal` |

Both instances must use the same **CODEX_HOME path**. T3 Code prepares the shared
state in the shadow directory; do not populate it by copying your whole Codex
home.

The shadow account needs its own `auth.json` file. If Codex uses an OS credential
store, configure file storage for this setup. See
[OpenAI's credential storage guide](https://learn.chatgpt.com/docs/auth#credential-storage).

Use a completely separate **CODEX_HOME path**, with no shadow home, when you want
separate Codex sessions and configuration. That instance cannot continue threads
from the other home.

## Switch accounts in an existing thread

Choose the other account from the thread's model picker. T3 Code offers compatible
Codex instances that share the thread's **CODEX_HOME path**. Changing accounts does
not move the conversation into a separate Codex home.

If the account is missing from the picker, compare the home paths in provider
settings. If two instances show the same unexpected account or models, check their
reported accounts, refresh provider status, and confirm the second instance has
its own shadow path and login. A shadow-home conflict usually means the directory
contains a copied Codex setup. Use a fresh shadow directory and sign in again.

## Answer questions while Codex works

Codex can ask a question and keep working. Answer it in the thread's question
panel. The answer becomes a new message: it reaches the active turn, or starts
another turn if Codex has finished. Unanswered questions survive reconnects.
If you do not want to answer, dismiss the question from its panel. Dismissing
closes it without sending anything to Codex. This requires a Codex version that
supports async questions.

## Approve app access

Codex tools can request access to another app. Respond to the named app's request
in the thread on web, desktop, or mobile. Some tools offer access for one request,
the current session, or permanently. See [Permission modes](./permission-modes.md)
for command and file approvals.

## Codex says I hit a usage limit

When Codex stops on a usage limit, the thread names the window that ran out and
when it resets, when Codex reports them. Send the message again after the reset. On a workspace plan the
message also says whether your workspace owner needs to add credits or raise the
spend limit to continue sooner.

## Send feedback to OpenAI

In an existing Codex thread, send `/feedback` with an optional description, for
example `/feedback The agent stopped before finishing the tests`. This uploads
the conversation and Codex logs to OpenAI. The returned thread ID can be shared
with OpenAI support.
