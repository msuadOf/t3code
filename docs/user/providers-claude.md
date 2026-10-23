# Claude

T3 Code uses Claude Code's login and configuration. Start with the default provider
for one account; [provider setup](./install.md#providers) covers installation and
shared provider settings.

## Native shared sessions

This fork connects conversations to Claude Code's native background-session daemon.
Install a current Claude CLI with `agents`, `attach`, and `--bg` on the environment's
macOS or Linux host. Web, desktop, and mobile clients all read the same server-side
conversation records.

Existing transcripts appear automatically. **Shared session directory** limits
this to a directory and its descendants; an empty value includes all directories
in that Claude configuration. Directory filtering does not delete transcripts or
stop native work.

For an already running foreground conversation, use `/background` in its terminal.
Then open that conversation in T3 Code and use `claude attach <id>` in the terminal.
Both clients observe the same background worker. T3 sends text when that worker is
idle. Creating a new conversation in T3 also creates a native background worker.

To reopen a historical conversation that has no running worker, start it on the
host with `claude --bg --resume <session-id>`, using the same `CLAUDE_CONFIG_DIR` as
T3. T3 then attaches to that worker. Closing T3 leaves native work running.

The current native daemon integration has these limits:

- Replies, tool output, and thinking blocks appear after Claude persists them.
  Live reasoning token streaming is unavailable.
- Answer permission dialogs and questions in `claude attach`. T3's structured
  approval buttons are unavailable for native Claude sessions.
- Interrupt and steer work from the attached CLI. T3 cannot safely target one
  native turn with the daemon's current terminal-control protocol.
- Send from T3 while the worker is idle. Avoid submitting identical text from T3
  and the CLI simultaneously: the native reply acknowledgement has no request ID
  to distinguish those submissions.
- Model, permission, and environment changes for an existing worker belong to its
  native CLI configuration. Attachment preserves the worker's configuration. The
  background daemon can filter launcher environment variables; configure router
  endpoints in that configuration directory's `settings.json` under `env`.
- Local Windows native-daemon hosting has not been implemented. Clients on Windows
  can connect to a supported remote host.

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
