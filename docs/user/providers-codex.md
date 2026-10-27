# Codex

T3 Code connects to the shared Codex server on your environment’s machine. Use the
same server from Codex CLI to open and control the same live sessions in T3 Code
on web, desktop, iOS, and Android.

## Share sessions with Codex CLI

Install a current Codex CLI that supports `app-server daemon` and `--remote`, then
sign in on the environment’s machine:

```bash
codex login
codex app-server daemon start
codex --remote unix://
```

In T3 Code, select your environment and configure Codex to use the existing CLI
installation. Leave **Shared Codex server** empty to use the daemon for that
instance’s **CODEX_HOME path**, which defaults to `~/.codex`. T3 Code can start the
default daemon if it is not running.

Codex sessions appear in the normal project and thread lists. Opening one keeps
its original Codex session ID; it does not create a copy. CLI and T3 Code can stay
open together. Messages, ongoing replies, tool activity, and supported approvals
use the same running session. Closing T3 Code leaves the daemon and CLI running.

When joining a turn already in progress, T3 restores the items the native server
exposes, including available reasoning summaries. Codex may withhold an unfinished
assistant reply from its snapshot; that prefix becomes available when Codex emits
the completed item. Reasoning shows the summaries Codex publishes.

T3 titles and archive choices organize its own list. Later CLI renames and archive
changes are not mirrored.

An independent CLI process that has not connected to this daemon cannot share its
active execution. Start it with `--remote unix://` using the same Codex home.

## Choose which sessions to share

Leave **Shared session directory** empty to show sessions from all directories.
Set it to an absolute directory on the environment’s machine to include that
directory and its subdirectories. This setting is available with the Codex
provider settings on web and desktop, and under the environment’s **Providers**
section on mobile.

Narrowing the directory hides previously shared sessions outside that directory
in T3 Code. It does not archive or delete them in Codex. Clear or widen the
setting to show them again.

## Connect to a remote machine

Use T3 Code’s existing remote environment connection, including its SSH flow,
then select that environment before configuring Codex. Both Codex CLI and the
shared daemon run on the remote machine; the clients connect through T3 Code.

**CODEX_HOME path**, **Shared Codex server**, and **Shared session directory** all
refer to that environment’s machine. For a custom daemon, set **Shared Codex
server** to its `unix:///absolute/path/to/socket`, `ws://`, or `wss://` endpoint and
connect CLI clients to that same endpoint. T3 Code connects to an explicit
endpoint without starting another server. A Unix socket path is on the T3
server’s machine, not on the phone or desktop viewing it.

## Accounts and configuration

Shared sessions use the login and configuration owned by the Codex daemon. Sign
in with `codex login` for its Codex home. T3-managed ChatGPT credentials and
shadow-home account switching are not used for shared sessions.

For a separate account, configure a separate **CODEX_HOME path**, sign in there,
and start its own daemon. Keep its CLI and T3 instance connected to that home’s
server. Separate homes have separate sessions; selecting another account does
not move an existing native session between homes.

## Answer questions while Codex works

Codex can ask a question and keep working. Answer it in the thread’s question
panel. The answer becomes a new message: it reaches the active turn, or starts
another turn if Codex has finished. Unanswered questions survive reconnects.
If you do not want to answer, dismiss the question from its panel. Dismissing
closes it without sending anything to Codex. This requires a Codex version that
supports async questions.

## Approve app access

Codex tools can request access to another app. Respond to the named app’s request
in the thread on web, desktop, or mobile. Some tools offer access for one request,
the current session, or permanently. An approval resolved by another connected
client is removed from T3 Code. Some requests must be answered by the client that
owns them. See [Permission modes](./permission-modes.md) for command and file
approvals.

## Codex says I hit a usage limit

When Codex stops on a usage limit, the thread names the window that ran out and
when it resets, when Codex reports them. Send the message again after the reset.
On a workspace plan the message also says whether your workspace owner needs to
add credits or raise the spend limit to continue sooner.

## Send feedback to OpenAI

In an existing Codex thread, send `/feedback` with an optional description, for
example `/feedback The agent stopped before finishing the tests`. This uploads
the conversation and Codex logs to OpenAI. The returned thread ID can be shared
with OpenAI support.
