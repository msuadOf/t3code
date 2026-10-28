---
version: alpha
name: T3 Code
description: A shared workspace for controlling native coding agents across web, desktop and mobile.
omitted:
  - section: colors
    reason: Existing runtime themes remain authoritative; this document introduces no palette values.
  - section: typography
    reason: Existing web and mobile typography remains authoritative.
  - section: rounded
    reason: Shared controls own their geometry.
  - section: spacing
    reason: Existing responsive settings and chat layouts own spacing.
components:
  select: {}
  settings: {}
  conversation: {}
---

# T3 Code design context

## Overview

T3 Code is a product interface for people steering coding agents from a desktop
or a phone. Keep the established compact workspace and familiar settings controls.
The conversation and current agent activity take priority over decorative chrome.
Settings copy follows the existing English product vocabulary; user messages retain
their original language. No market-specific behavior is introduced here.

Runtime themes remain the source of truth in `apps/web/src/index.css` and
`apps/mobile/global.css`. This file does not generate tokens or duplicate their
values. Desktop renders the web interface; mobile uses its own native controls.

## Layout

Keep the existing responsive chat layout. At narrow widths, replies, activity and
the stop action remain available without opening the sidebar or details panel.
Streaming text must not steal focus from the composer.

## Components

| Capability     | Canonical owner                                                   | Source of truth                              | Allowed variants                             | Verification                                   |
| -------------- | ----------------------------------------------------------------- | -------------------------------------------- | -------------------------------------------- | ---------------------------------------------- |
| Select/Listbox | Web `components/ui/select`; native settings choice rows           | Existing shared controls                     | Authored web popup; native choice rows       | Keyboard, popup and narrow viewport checks     |
| Form           | Scoped server settings in web and mobile                          | Contract schemas and server settings service | Project and environment scope                | Settings persistence and scoped settings tests |
| Conversation   | Shared runtime events and each client’s existing message renderer | Provider events and streaming setting        | Incremental, paragraph and full-turn display | Native CLI to browser streaming test           |

Response streaming uses one server setting across web, desktop and mobile. The
default is **Show text as it arrives**, for both replies and thinking supplied by
the provider. Finished-paragraph and full-turn display remain explicit choices.
All surfaces describe the live choice with the same label and meaning.

Claude history discovery lives in each environment’s provider settings, using the
existing web settings controls and native action rows. Search is explicit, results
are paginated, and conversion is an explicit action. Foreground sessions show the
native `/bg` handoff instruction instead of starting a second writer.

## Do's and Don'ts

- Use the existing settings and conversation components.
- Keep native provider capability limits visible and honest.
- Do not add animation, alternate controls or a second stream for this behavior.
- Do not describe a completed-history import as live output.
