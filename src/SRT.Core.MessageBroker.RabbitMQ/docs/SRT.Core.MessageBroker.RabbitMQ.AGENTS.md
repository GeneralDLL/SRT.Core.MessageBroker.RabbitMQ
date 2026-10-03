# SRT.Core.MessageBroker.RabbitMQ — Codex / Agents entry

Package: **SRT.Core.MessageBroker.RabbitMQ**

Before any change that uses or modifies this package, read:

1. [SRT.Core.MessageBroker.RabbitMQ.AI_RULES.md](SRT.Core.MessageBroker.RabbitMQ.AI_RULES.md) *(canonical MUST / MUST NOT)*
2. [SRT.Core.MessageBroker.RabbitMQ.API_REFERENCE.md](SRT.Core.MessageBroker.RabbitMQ.API_REFERENCE.md)
3. [SRT.Core.MessageBroker.RabbitMQ.COOKBOOK.md](SRT.Core.MessageBroker.RabbitMQ.COOKBOOK.md) for host/appsettings (including `Service` in every environment)

Working rule: read docs first; do not invent APIs; if ambiguous, inspect related code and update the docs.

Initial RabbitMQ wiring: ask the human for TLS yes/no and custom CA vs system trust (placeholder `caPath` if path unknown) — see **Initial RabbitMQ configuration interview** in `AI_RULES.md`.

This file does not replace `AI_RULES.md`. Keep the `SRT.Core.MessageBroker.RabbitMQ.` filename prefix when copying into consumer projects.
