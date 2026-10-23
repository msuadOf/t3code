# OpenCode

Install and authenticate OpenCode on the machine running your environment, then
enable it in **Settings > Providers**. See [provider setup](./install.md#providers).
T3 Code requires OpenCode 1.14.19 or newer, including when you connect an existing
OpenCode server.

## OpenCode 2

T3 Code supports OpenCode 2.0.18 and newer. It detects the version on its own, so
the same provider settings work for OpenCode 1.x and 2.x. OpenCode 1.x shows
**Limited support** in its provider settings.

OpenCode 2 is a separate package, `@opencode/cli`. To move from 1.x, install it
yourself, for example `npm install -g @opencode/cli`. Then refresh provider status.
T3 Code's update button updates whichever package you have installed. It never
switches a 1.x install to 2.x.

OpenCode 2 converts the shared OpenCode database to its own format the first time it
runs. Don't run OpenCode 1.x and 2.x side by side on the same machine. Threads you
started on 1.x continue on 2.x.

Plan mode uses OpenCode's `plan` agent.

## Shared CLI sessions

With current OpenCode, leave **Server URL** empty to connect to its native shared
background service. T3 Code and the OpenCode CLI use the same service registration
and credentials on the environment's machine. Opening a session in both clients
uses the same native session; closing T3 Code leaves CLI work running.

Existing native sessions appear automatically. Set **Shared session directory**
in **Settings > Providers** to limit discovery to one directory and its
subdirectories. Leave it empty to include all directories. Excluding a directory
hides discovered sessions without deleting their history or stopping CLI work.

To use another existing OpenCode server, set **Server URL** and its password in
provider settings. The CLI must attach to that same server to share live work.
T3 Code uses only the configured password for an explicit server URL. If connection
checks fail, check the URL, credentials, and OpenCode version, then refresh status.

A native background service owns its own credentials. The password field applies
to an explicit server URL; it does not replace the native service's password.

## Approvals

OpenCode follows the shared [permission modes](./permission-modes.md). **Auto** has
the same rules as **Supervised** because OpenCode has no AI approval reviewer.
Environment files such as `.env` and `.env.local` need approval in restricted
modes even though normal file reads do not; `.env.example` is allowed.

**Allow for workspace** applies to matching requests in other OpenCode sessions
using the same workspace. It is broader than the current thread, especially on a
shared external server. Use **Allow once** for a single request. Denying an action
does not stop the whole turn.

## Refresh models, commands, and skills

After changing an OpenCode login or configuration, use **Refresh provider status**
in **Settings > Providers** for that environment. On mobile, use **Refresh models**
in the thread settings. Reconnecting also refreshes the catalog; periodic provider
health checks do not.

Credential changes are read on refresh. A running native service can cache its own
configuration. Reload or restart it through OpenCode when changing native
configuration that requires a restart; T3 Code does not shut down the shared
service on disconnect.

Existing threads keep their selected model and options even when it disappears
from the catalog. If OpenCode rejects that model, select an available one and retry.
