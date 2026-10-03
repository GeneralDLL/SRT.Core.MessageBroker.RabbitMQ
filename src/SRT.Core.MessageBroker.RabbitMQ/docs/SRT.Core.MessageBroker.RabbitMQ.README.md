# SRT.Core.MessageBroker.RabbitMQ — Documentation Index

Package: **SRT.Core.MessageBroker.RabbitMQ**  
Target: .NET 10 / RabbitMQ.Client 7.1.2  
Reviewed against: RabbitMQ repo `81dae1d`, Core repo `9381db4`

All filenames in this folder start with `SRT.Core.MessageBroker.RabbitMQ.` so you can copy the whole set into any consumer project without colliding with other packages’ docs.

## Reading order

| Audience | Start here | Then |
|----------|------------|------|
| Humans | [SRT.Core.MessageBroker.RabbitMQ.HUMAN_GUIDE.md](SRT.Core.MessageBroker.RabbitMQ.HUMAN_GUIDE.md) | [COOKBOOK](SRT.Core.MessageBroker.RabbitMQ.COOKBOOK.md), [API_REFERENCE](SRT.Core.MessageBroker.RabbitMQ.API_REFERENCE.md) |
| AI (all tools) | [SRT.Core.MessageBroker.RabbitMQ.AI_RULES.md](SRT.Core.MessageBroker.RabbitMQ.AI_RULES.md) | [API_REFERENCE](SRT.Core.MessageBroker.RabbitMQ.API_REFERENCE.md), [COOKBOOK](SRT.Core.MessageBroker.RabbitMQ.COOKBOOK.md) |
| Codex | [SRT.Core.MessageBroker.RabbitMQ.AGENTS.md](SRT.Core.MessageBroker.RabbitMQ.AGENTS.md) | AI_RULES |
| Claude | [SRT.Core.MessageBroker.RabbitMQ.CLAUDE.md](SRT.Core.MessageBroker.RabbitMQ.CLAUDE.md) | AI_RULES |
| Cursor | Install [SRT.Core.MessageBroker.RabbitMQ.CURSOR_RULE.mdc](SRT.Core.MessageBroker.RabbitMQ.CURSOR_RULE.mdc) | AI_RULES |
| Copy paths per AI | [SRT.Core.MessageBroker.RabbitMQ.AI_INSTALL.md](SRT.Core.MessageBroker.RabbitMQ.AI_INSTALL.md) | — |

## Files in this set

1. `SRT.Core.MessageBroker.RabbitMQ.README.md` — this index
2. `SRT.Core.MessageBroker.RabbitMQ.HUMAN_GUIDE.md` — how the package works
3. `SRT.Core.MessageBroker.RabbitMQ.API_REFERENCE.md` — exact contracts
4. `SRT.Core.MessageBroker.RabbitMQ.COOKBOOK.md` — recipes (including `Service` in every environment appsettings)
5. `SRT.Core.MessageBroker.RabbitMQ.AI_RULES.md` — canonical MUST / MUST NOT
6. `SRT.Core.MessageBroker.RabbitMQ.AGENTS.md` — Codex entry
7. `SRT.Core.MessageBroker.RabbitMQ.CLAUDE.md` — Claude entry
8. `SRT.Core.MessageBroker.RabbitMQ.CURSOR_RULE.mdc` — Cursor rule **template**
9. `SRT.Core.MessageBroker.RabbitMQ.AI_INSTALL.md` — copy destinations

## Copy into a consumer project

Follow **[SRT.Core.MessageBroker.RabbitMQ.AI_INSTALL.md](SRT.Core.MessageBroker.RabbitMQ.AI_INSTALL.md)**.

## Session bootstrap (paste for Cursor / Claude / Codex)

```text
You are working with SRT.Core.MessageBroker.RabbitMQ.
Before changing or consuming this package:
1) Read SRT.Core.MessageBroker.RabbitMQ.AI_RULES.md
2) Use SRT.Core.MessageBroker.RabbitMQ.API_REFERENCE.md for signatures/contracts
3) Follow SRT.Core.MessageBroker.RabbitMQ.COOKBOOK.md for host/appsettings recipes
Rules: read docs first; do not invent APIs; if ambiguous, inspect related code and update these docs.
Consumers MUST add Service.Id / Service.Name to every environment appsettings file.
Enable plugin via lstRabbitMQNamespace + ProjectReference; ConnectionDB:SRTCore_RabbitMQ.host required.
Initial RabbitMQ setup: ask human for TLS yes/no and custom CA vs system trust; if TLS and path unknown use placeholder caPath (tls/ca.pem).
Shared IRabbitConnection; publishers/consumers own channels only. Publish success = confirm + no BasicReturn.
Optional RabbitMQ:Prefix in appsettings is applied to queue/exchange names (Redis-style); empty Prefix is a no-op.
```

## Loading note

Files under package `docs/` are **not** auto-loaded by Cursor. Copy the `.mdc` to the consumer `.cursor/rules/` and keep package-prefixed names.
