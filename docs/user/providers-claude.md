# Claude

T3 Code uses Claude Code's login and configuration. Start with the default provider
for one account; [provider setup](./install.md#providers) covers installation and
shared provider settings.

## Native shared sessions

This fork connects conversations to Claude Code's native background-session daemon.
Install a current Claude CLI with `agents`, `attach`, and `--bg` on the environment's
macOS or Linux host. Web, desktop, and mobile clients all read the same server-side
conversation records.

T3 discovers background sessions registered in `claude agents --all`, including
stopped jobs, when their transcript belongs to the selected Claude configuration.
Ordinary foreground conversations and unregistered historical transcripts are not
automatically added to the thread list. In the environment’s Claude provider settings,
use **Browse sessions** to search saved conversations by title, directory or session ID.
Choose **Continue as shared session** to restore inactive history through Claude’s
native background mode and open it in T3. A conversation still open in a foreground
terminal must first be moved with `/bg` in that terminal; refresh the list afterward.
Opening an already shared session reuses its existing worker. A newly backgrounded
conversation may not save its transcript until the next reply; T3 shows it as shared
and brings in its history when Claude saves it. If Claude cannot claim
the original history and creates a copy, T3 stops that copy without sending a prompt
and reports the failure. **Shared session directory** limits discovery
to a directory and its descendants; an empty value includes all directories in
that Claude configuration. Directory filtering does not stop native work.

Start a shared conversation with `claude --bg`, then open it with
`claude attach <id>` in the terminal and select the same session in T3. Creating a
new conversation in T3 also creates a native background worker. T3 sends text
when that worker is idle. Both interfaces operate on the same worker, and closing
T3 leaves native work running.

New workers started by T3 can use T3's MCP tools through a private local bridge.
This includes inactive history opened with **Continue as shared session**, when
that history has no existing daemon worker.
The bridge belongs to the original native session and follows T3's current tool
permissions when the daemon reconnects. It does not use Anthropic Remote Control.
Opening an existing worker that was started without this bridge does not install
it into the running process. Native child sessions do not inherit another
session's T3 tool authorization.

T3 can send uploaded PNG, JPEG, GIF and WebP images, plus supported text and code
files, to the same idle worker using Claude's native file-reference processing.
The files must be available on the environment hosting Claude. Unsupported formats
and paths are rejected before sending; PDF attachments are not supported here yet.

To share an ordinary foreground conversation, use `/background` in the CLI first,
then select the resulting session in T3. The CLI may assign that background session
a new UUID. T3 can wake a stopped background job through native `attach`; saved
sessions retain their own model, permission, and environment options.

The current native daemon integration has these limits:

- Saved replies, tool output, and thinking blocks appear after Claude persists them.
  Opt-in local model activity can show provisional text and thinking earlier;
  see [Local model activity](#local-model-activity).
- T3 can display and answer a native `AskUserQuestion` containing one single-select,
  multiple-select, or free-text question. Multiple-select answers use the listed
  options. It waits for Claude’s actual tool result before marking the answer
  complete; submission alone is not confirmation. Multi-question prompts and
  command/file permission approvals still require
  `claude attach`. T3 does not install hooks to forward these requests. The native
  reply API cannot atomically target a question, so avoid answering simultaneously
  from T3 and the CLI. The recorded native answer wins if another client answered.
  When Claude reports that it is waiting, T3 shows a continuation hint for requests
  it cannot answer. That hint clears when the worker continues or the turn ends.
- Native daemon state notifications refresh progress promptly, with polling as a
  reconnect fallback. These notifications are separate from body/thinking token
  streams; terminal output is not parsed into chat messages.
- Native background Bash commands with an identifiable output receipt show a live
  command output tail. Their task status comes from Claude's matching task
  notification, independently of the model reply. Earlier output may be omitted
  when joining a long-running command. Reconnecting restores pending background
  work to its original records, including completion that happened while T3 was
  offline, without reopening the parent reply. Foreground Bash output is shown when Claude
  saves its result; its internal live progress is not exposed by this connection.
- Native `TodoWrite` results appear as structured task lists, and explicit
  `ExitPlanMode` plan fields appear as proposed plans. A proposal is not an approval.
- Native `Agent` and `Task` children appear with their original parent tool and
  lifecycle. A background launch is not a completion; T3 requires a native terminal
  notification or the child's own confirmed terminal state. Unconfirmed outcomes
  remain unknown. These are observed children, not separately controllable T3 threads.
- Claude supplies automatic conversation titles. Explicit title regeneration remains
  available; T3 auxiliary model tasks use separate, explicitly identified sessions
  without saving them to Claude’s history. Old auxiliary history without trustworthy
  ownership records is not guessed or removed.
- Continuing the same native conversation does not inject failed T3 inputs or
  restart summaries into the next prompt. A failed native resume reports failure
  instead of silently creating a replacement conversation.
- T3 can interrupt an observed running background turn through the native terminal.
  It checks the current turn and worker before sending Ctrl+C. The native terminal
  protocol cannot atomically target a turn, so avoid racing an interrupt with a new
  CLI submission. The temporary control attachment may briefly resize the terminal;
  the daemon restores the remaining attachment's dimensions when it disconnects.
  Steering remains available through `claude attach`.
  Additional human input that Claude absorbs at a tool boundary appears as a user
  message in the original turn, after Claude saves its delivery record.
  Queued text that Claude has not consumed does not appear as confirmed input.
- Send from T3 while the worker is idle. Avoid submitting identical text from T3
  and the CLI simultaneously: the native reply acknowledgement has no request ID
  to distinguish those submissions.
- Model, permission, and environment changes for an existing worker belong to its
  native CLI configuration. Attachment preserves the worker's configuration. The
  background daemon can filter launcher environment variables; configure router
  endpoints in that configuration directory's `settings.json` under `env`.
- Local Windows native-daemon hosting has not been implemented. Clients on Windows
  can connect to a supported remote host.

## Fork a shared conversation

An idle Claude conversation can be forked with its complete saved history. The new
conversation gets its own native UUID and worker; CLI clients can open either one
independently. T3 keeps copied native message IDs separate from the source thread.

Forking from a selected historical turn, changing the model or workspace during the
fork, and unverified custom `--settings` overrides are not supported. An observed
source requires independently configured observation for the new worker. Wait for
active work to finish before forking.

## Local model activity

For an API-key connection to the Anthropic Messages API or a compatible gateway,
set **Local model activity upstream** to the base URL in Claude provider settings.
Enable **Observe model activity in new sessions** to route newly created workers
through the local observer, or choose **Continue with model activity** for inactive
history. This is experimental and off by default. OAuth subscription connections,
Bedrock and Vertex are not supported by this observer.

Web, desktop and mobile show the same **Model activity** panel. It receives real
response increments, including available text, thinking and partial tool input.
Named tools show their arguments as they arrive. **Arguments received** means the
model finished preparing the call; execution results appear in the saved conversation.
These responses can include auxiliary model requests and are marked **Not yet saved
by Claude**. Only a matching message saved by Claude changes the status to **Saved
in conversation** and collapses that response; provisional activity does not become
conversation history or future context. Missing or trimmed activity is indicated.

The activity panel follows the newest running response as it grows. Scroll or expand
a request to pause following while reading; choose **Follow live** to return to the
current output. This moves only the activity panel, not the saved conversation.

The observer runs on the environment's machine and forwards model traffic to the
upstream you configured. It does not use Anthropic Remote Control. It keeps a
bounded response buffer in memory and does not save model request bodies or headers.
Closing a client or restarting T3 leaves its native worker and observer running.
A stopped observer can be restored when T3 reopens the bound worker; an interrupted
model request is not replayed automatically.

A running worker cannot be upgraded in place. Stop it through Claude before
explicitly reopening its inactive history with observation. Changing the upstream
or disabling the new-session switch affects future launches; previously observed
workers retain their original route. For an idle shared session, use **Restart
without model activity** in the session browser to stop observing, or **Restart
with model activity** to enable it again. This restarts the native worker and
can disconnect terminal attachments, while preserving the session UUID and history.
Avoid sending CLI input during this operation: the native stop command has no atomic
idle precondition. T3 changes only that worker's private settings; global Claude
settings and other workers remain unchanged. Simply stopping and resuming a job in
Claude retains its saved launch settings and does not turn observation off.

## Separate accounts or configurations

Use a separate Claude config directory for each account. This also works for named
presets that need different Claude settings or a router connection.

Keep your existing account in the default directory. On the environment's machine,
create the second login:

```bash
mkdir -p ~/.claude_personal
CLAUDE_CONFIG_DIR=~/.claude_personal claude auth login
```

Add another Claude instance in **Settings > Providers**:

| Instance        | Binary path | CLAUDE_CONFIG_DIR path |
| --------------- | ----------- | ---------------------- |
| Claude Work     | `claude`    | Leave empty            |
| Claude Personal | `claude`    | `~/.claude_personal`   |

An empty config-directory setting uses Claude Code's normal configuration. The
custom setting changes `CLAUDE_CONFIG_DIR`, leaving `HOME` and the system keychain
location intact. Use the same variable for the login command. Setting `HOME`
instead can put credentials where this provider will not find them.

Check the account reported in provider settings after signing in. Existing
threads can switch only between Claude instances with the same config directory.
Separate account directories stay isolated, including their local conversation
state. Claude does not have Codex's shared-home and shadow-home arrangement.

For presets that differ only in API keys or endpoints, use the instance's
**Environment variables**. Variable assignments do not belong in **Launch arguments**.

Claude Code's verbose mode can stay enabled when you use Claude for text generation, including
thread titles, branch names, commit messages, and pull request descriptions. On a remote connection,
T3 Code uses the Claude configuration on the connected server.

## Native conversation controls

Use `/compact`, model selection, permission controls, and task management in the
attached Claude CLI. Its background worker owns those settings. SDK-specific
compaction prompts, structured task-list projection, and usage-limit recovery
are unavailable in the native conversation adapter.

## Skills

Claude skills come from the config directory's `skills` folder and the project's
`.claude/skills` folder. If both define the same name, the config-directory copy
wins. Skills disabled in Claude's settings do not appear in the composer.

Use `$` in the composer to select a skill. Enabled installed Claude plugins also
contribute skills under names such as `plugin:review`. Skills marked `disable-model-invocation`
can still be started by you. Invoke those one per message: Claude directly runs
only the last named skill and may try to start earlier ones through its Skill
tool, which refuses skills reserved for manual invocation.

## OpenRouter

Create a Claude instance with its own config directory, such as
`~/.claude_openrouter`, and keep **Binary path** set to `claude`. In that instance's
**Environment variables**, use:

| Variable               | Value                                     |
| ---------------------- | ----------------------------------------- |
| `ANTHROPIC_BASE_URL`   | `https://openrouter.ai/api`               |
| `ANTHROPIC_AUTH_TOKEN` | Your OpenRouter API key, marked Sensitive |
| `ANTHROPIC_API_KEY`    | An explicitly empty value                 |

If that Claude config directory has a cached Anthropic login, run `/logout` in a
Claude Code session using that directory before starting the router setup. Cached
login credentials can conflict with the router token.

Select the model you want in T3 Code. For an OpenRouter model outside the built-in
list, open that Claude instance in **Settings > Providers** and add its full model
ID with **Add custom model**. Then select it in the chat model picker.
`ANTHROPIC_DEFAULT_*_MODEL` variables map Claude Code aliases such as `sonnet`; they
do not replace the explicit model ID selected in T3 Code. Custom models may have
fewer effort, thinking, or context controls than built-in models.

Verify the model used in OpenRouter's activity dashboard. For current compatibility
requirements, use the
[OpenRouter Claude Code guide](https://openrouter.ai/docs/cookbook/coding-agents/claude-code-integration).

## Other routers

A local router uses an ordinary Claude provider instance. Give it a separate
config directory and put the router's endpoint and credential variables in that
configuration directory's `settings.json` under `env`. The router must run where the environment
can reach it. Follow the [Claude Code Router instructions](https://github.com/musistudio/claude-code-router)
for its installation and routing configuration.
