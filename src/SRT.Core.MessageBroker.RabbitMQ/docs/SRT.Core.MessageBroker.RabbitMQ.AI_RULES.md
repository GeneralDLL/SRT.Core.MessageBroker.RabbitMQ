# SRT.Core.MessageBroker.RabbitMQ — AI Rules (canonical)

Mandatory for Cursor, Claude, Codex, and any agent consuming or modifying **SRT.Core.MessageBroker.RabbitMQ**.

Companion docs:

- [SRT.Core.MessageBroker.RabbitMQ.API_REFERENCE.md](SRT.Core.MessageBroker.RabbitMQ.API_REFERENCE.md)
- [SRT.Core.MessageBroker.RabbitMQ.COOKBOOK.md](SRT.Core.MessageBroker.RabbitMQ.COOKBOOK.md)
- [SRT.Core.MessageBroker.RabbitMQ.HUMAN_GUIDE.md](SRT.Core.MessageBroker.RabbitMQ.HUMAN_GUIDE.md)

## Working rule

1. Read these docs first.
2. Do not invent APIs that duplicate `IPublisherMessage`, `IConsumerMessage`, or `IRabbitConnection`.
3. If docs are wrong: inspect related code and update these docs in the same change when possible.

## Initial RabbitMQ configuration interview (mandatory)

When **first wiring RabbitMQ** into a host (new consumer, new `ConnectionDB:SRTCore_RabbitMQ`, first `lstRabbitMQNamespace`, or unclear existing choice), the AI **MUST stop and ask the human** before writing appsettings / `BasicProjectConfig`. Do not guess.

### 1) Enable messaging

Ask which namespace prefix(es) to put in `lstRabbitMQNamespace` (publisher/consumer class namespaces). Empty list → plugin no-ops.

### 2) TLS / SSL

Ask: **Does this RabbitMQ endpoint use TLS/SSL?**

- **No** → set `sslEnabled` to `"false"` (or omit) and leave `caPath` empty. Do not invent SSL fields.
- **Yes** → ask: **custom CA PEM (`caPath`) or system trust only?**
  - Custom CA: if path unknown, write placeholder and say so:

```json
"sslEnabled": "true",
"port": "5671",
"caPath": "tls/ca.pem"
```

  - System trust: `"sslEnabled": "true"` and empty `caPath` (broker cert must be trusted by the OS).

After answers: set `lstRabbitMQNamespace`, configure `ConnectionDB:SRTCore_RabbitMQ`, and put `Service` in every environment appsettings file.

## Host / plugin

MUST:

- Keep `Program.cs` thin (`SRT_ConfigController` → `SRT_ConfigBuilder` → `SRT_ConfigBuilderAndBuildApp` → `SRT_ConfigApp`).
- Project-reference this package so `RabbitMqBasicConfigurations` is discovered.
- Configure `ConnectionDB:SRTCore_RabbitMQ` when `lstRabbitMQNamespace` is non-empty.
- If the host has `RabbitMQ:Prefix` in appsettings, leave it; the plugin applies it to queue/exchange names. Do not hardcode a second prefix in publisher/consumer type names. Empty/missing Prefix is valid.
- Treat Rabbit TLS as optional: `UseTls = sslEnabled || caPath`; plaintext when both absent/empty.
- When `caPath` is set, fail fast on missing/invalid PEM; never fall back to plaintext after a TLS config error.
- Prefer durable queues/exchanges for production topology; do **not** change package defaults of existing `QueueData`/`ExchangeData` if that would break declared brokers (`PRECONDITION_FAILED`).
- Add `Service.Id` and `Service.Name` to **every** environment appsettings file.

MUST NOT:

- Call `UseAuthorization` or `MapControllers` in the host.
- Register RabbitMQ DI in `Program.cs` (plugin does it).
- Dispose the shared `IConnection` from a publisher/consumer (only close their channel).
- Copy Redis Docker flag `tls-auth-clients` into RabbitMQ compose — use RabbitMQ `ssl_options` instead.
- Require SSL fields when the consumer uses plaintext AMQP.
- Assume TLS during initial setup without asking the human.

## Messaging usage

MUST:

- Subclass `PublisherMessageAbstract` / `ConsumerMessageAbstract` (or RPC variants) in a namespace listed in `lstRabbitMQNamespace`.
- Inject the **concrete** publisher type (scoped). Consumers are hosted services.
- Treat publish success as broker confirm **and** no `BasicReturn` (unroutable fails).
- Use `IntegrationDeadException` for intentional dead-letter (Nack without requeue).

MUST NOT:

- Open a second connection factory per publish when `IRabbitConnection` is available.
- Double-ack RPC reply consumers (`autoAck: true` means no manual Ack).
- Leave deliveries unacked on handler failure.

## Documentation maintenance

MUST:

- Keep filenames prefixed with `SRT.Core.MessageBroker.RabbitMQ.` when copying into consumers.
- Update `API_REFERENCE.md` when public contracts change.
- Keep `AGENTS` / `CLAUDE` / `CURSOR_RULE` as short pointers to this file.

MUST NOT:

- Rename these docs to generic `README.md` / `AGENTS.md` / `CLAUDE.md` when multiple package doc sets coexist.
