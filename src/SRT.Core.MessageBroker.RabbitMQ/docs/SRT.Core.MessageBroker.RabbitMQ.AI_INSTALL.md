# SRT.Core.MessageBroker.RabbitMQ — Where to copy files for each AI

Source folder (inside this package):

`SRT.Core.MessageBroker.RabbitMQ/src/SRT.Core.MessageBroker.RabbitMQ/docs/`

Copy **with the same filenames** (keep the `SRT.Core.MessageBroker.RabbitMQ.` prefix).  
`<project>` = root of the consumer solution/repo that installed this package.

---

## Shared (all AIs) — human + full knowledge

| Copy from `docs/` | Paste into consumer |
|-------------------|---------------------|
| `SRT.Core.MessageBroker.RabbitMQ.README.md` | `<project>/docs/` |
| `SRT.Core.MessageBroker.RabbitMQ.HUMAN_GUIDE.md` | `<project>/docs/` |
| `SRT.Core.MessageBroker.RabbitMQ.API_REFERENCE.md` | `<project>/docs/` |
| `SRT.Core.MessageBroker.RabbitMQ.COOKBOOK.md` | `<project>/docs/` |
| `SRT.Core.MessageBroker.RabbitMQ.AI_RULES.md` | `<project>/docs/` |

Optional: also keep `SRT.Core.MessageBroker.RabbitMQ.AI_INSTALL.md` in `<project>/docs/`.

---

## Cursor

| Copy from `docs/` | Paste into consumer |
|-------------------|---------------------|
| `SRT.Core.MessageBroker.RabbitMQ.CURSOR_RULE.mdc` | `<project>/.cursor/rules/` |
| `SRT.Core.MessageBroker.RabbitMQ.AI_RULES.md` | `<project>/docs/` |
| `SRT.Core.MessageBroker.RabbitMQ.API_REFERENCE.md` | `<project>/docs/` |
| `SRT.Core.MessageBroker.RabbitMQ.COOKBOOK.md` | `<project>/docs/` |

---

## Claude

| Copy from `docs/` | Paste into consumer |
|-------------------|---------------------|
| `SRT.Core.MessageBroker.RabbitMQ.CLAUDE.md` | `<project>/` or `<project>/docs/` |
| Knowledge trio | `<project>/docs/` |

If a root `CLAUDE.md` already exists, add a pointer; do not overwrite it.

---

## Codex

| Copy from `docs/` | Paste into consumer |
|-------------------|---------------------|
| `SRT.Core.MessageBroker.RabbitMQ.AGENTS.md` | `<project>/` or `<project>/docs/` |
| Knowledge trio | `<project>/docs/` |

If a root `AGENTS.md` already exists, add a pointer; do not overwrite it.

---

Canonical rules for every AI: `docs/SRT.Core.MessageBroker.RabbitMQ.AI_RULES.md`
