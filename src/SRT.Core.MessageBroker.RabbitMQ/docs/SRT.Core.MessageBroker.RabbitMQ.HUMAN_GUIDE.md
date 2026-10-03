# SRT.Core.MessageBroker.RabbitMQ — Human Guide

How the package works for application developers.

## What it does

Plugin for SRT hosts that registers:

1. A **shared** `IRabbitConnection` (one AMQP connection, automatic recovery).
2. **Publishers** found under `AppSetting.lstRabbitMQNamespace` as scoped concrete types.
3. **Consumers** (`IHostedService`) for the same namespaces.

Discovery: Core loads `SRT.Core*.dll` plugins; this package’s `RabbitMqBasicConfigurations` (`Order = 40`, feature `RabbitMQ`) runs when `lstRabbitMQNamespace` is non-empty and `ConnectionDB:SRTCore_RabbitMQ.host` is set.

## Connection and TLS

Config binds from `ConnectionDB:SRTCore_RabbitMQ` (`GeneralConnectionString`):

| Field | Meaning |
|-------|---------|
| `host` / `port` / `user` / `pass` | Broker endpoint |
| `virtualHost` | Empty → `/` |
| `sslEnabled` | `"true"` / `"1"` enables TLS |
| `caPath` | CA PEM path; non-empty also enables TLS (`UseTls`) |

- Plaintext: no TLS flags → AMQP, default port **5672** if `port` empty.
- TLS: `UseTls` → AMQPS, default port **5671** if `port` empty. Explicit `port` is never overridden.
- Missing/invalid `caPath` → fail fast (no plaintext fallback).
- `sslEnabled` without `caPath` → TLS with OS trust store.

Optional name prefix (like Redis `RedisCache:Prefix`) from a sibling section:

```json
"RabbitMQ": { "Prefix": "srt:test:" }
```

When non-empty, declared queue and exchange names (and RPC reply queues) are prefixed at runtime. Routing keys are not prefixed. Empty/missing Prefix is a no-op.

Publishers and consumers create **their own channels** from the shared connection. Disposing a publisher/consumer closes only its channel.

## Publish / consume patterns

| Pattern | Base type |
|---------|-----------|
| Fire-and-forget | `PublisherMessageAbstract` + `ConsumerMessageAbstract` |
| Sync RPC (wait for reply) | `PublisherMessageRPCAbstract` |
| Async RPC (named callback queue) | `PublisherMessageRPCStaticAbstract` + `ConsumerOfPublisherResultAbstract` |

Publish success requires a **publisher confirm** and **no `BasicReturn`** (unroutable messages fail). Channels are serialized with a semaphore so concurrent calls on one scoped publisher are safe.

Consumer errors:

- `IntegrationDeadException` → Nack without requeue.
- Other errors → republish with `x-retry-count` until `MaxRedeliveries` (default 10), then Nack without requeue.

## Recommended topology

Package defaults for `QueueData` / `ExchangeData` are unchanged for broker compatibility. For new durable messaging, prefer `durable: true` and `autoDelete: false` on queues/exchanges you create in your subclasses.

## Dual test hosts

| Host | Port | SSL |
|------|------|-----|
| `SRT.Test.RabbitMQ` | 5672 | No |
| `SRT.Test.RabbitMQ.Tls` | 5671 | Yes (`caPath`) |

One Docker Compose file exposes both listeners. See [COOKBOOK](SRT.Core.MessageBroker.RabbitMQ.COOKBOOK.md).
