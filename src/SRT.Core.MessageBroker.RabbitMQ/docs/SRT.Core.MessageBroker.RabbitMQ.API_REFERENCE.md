# SRT.Core.MessageBroker.RabbitMQ — API Reference

Reviewed: RabbitMQ repo `81dae1d`, Core repo `9381db4`.  
Prefer this document for consumption. If docs disagree with code, inspect the related source and update this file.

---

## 1. Plugin — `RabbitMqBasicConfigurations`

- Namespace: `SRT.Core.MessageBroker.RabbitMQ`
- Implements: `ISRTBasicConfigurations`
- `Order` → `40`
- `FeatureName` → `SRTFeatureNames.RabbitMQ` (`"RabbitMQ"`)

### `SRT_ConfigBuilder`

| Condition | Behavior |
|-----------|----------|
| `lstRabbitMQNamespace.Count == 0` | Returns `builder` unchanged |
| Otherwise | Calls `SRT_RabbitMQConfig`: binds `RabbitMQ` section to `RabbitMqOptions`; registers singleton `IRabbitConnection`; scans namespaces for `IConsumerMessage` / `IPublisherMessage` |

### `SRT_ConfigApp`

Registers async shutdown: stop `IConsumerMessage` hosted services, then dispose `IRabbitConnection` (no sync `.GetResult()`).

---

## 2. Core config — `GeneralConnectionString` (SRT.Core)

Slot: `AppConnectionString.SRTCore_RabbitMQ` from `ConnectionDB`.

| Member | Behavior |
|--------|----------|
| `UseTls` | `IsSslEnabled \|\| !string.IsNullOrWhiteSpace(caPath)` |
| `UseRedisTls` | Alias of `UseTls` |
| `GetRabbitMqPort()` | User `port` if set; else `5671` when `UseTls`, else `5672` |
| `GetConnectionString_RabbitMQ()` | `amqps`/`amqp` + redacted password; uses `GetRabbitMqPort()` |
| `GetVirtualHost()` | Empty → `"/"` |
| `GetSafeSummary()` | Includes `ssl={UseTls}` |

---

## 3. `RabbitConnectionFactoryBuilder`

```csharp
public static ConnectionFactory Create(GeneralConnectionString cfg, string? contentRoot = null)
public static X509Certificate2 LoadCaCertificate(string caPath, string? contentRoot = null)
public static string ResolveCaPath(string caPath, string? contentRoot = null)
public static bool ValidateWithCa(X509Certificate? certificate, SslPolicyErrors errors, X509Certificate2 caCert)
```

| Mode | Behavior |
|------|----------|
| `!UseTls` | Plaintext factory; `AutomaticRecoveryEnabled` / `TopologyRecoveryEnabled` = true |
| `UseTls` + empty `caPath` | SSL enabled, system trust |
| `UseTls` + `caPath` | Load PEM (fail-fast); `CustomRootTrust` validation; reject name mismatch / not available / expired |

`Ssl.ServerName = host`; `Ssl.Version = Tls12 \| Tls13`.

---

## 4. `IRabbitConnection` / `RabbitConnection`

```csharp
string Prefix { get; }
string ApplyNamePrefix(string name);
Task<IConnection> GetConnectionAsync(CancellationToken cancellationToken = default);
Task<IChannel> CreateChannelAsync(CreateChannelOptions? options = null, CancellationToken cancellationToken = default);
```

- Singleton; single-flight connect; up to 3 cancelable initial retries.
- Owns the shared `IConnection`. Publishers/consumers must not dispose it.
- `Prefix` / `ApplyNamePrefix` come from `RabbitMqOptions` (`RabbitMQ:Prefix` in appsettings). Empty Prefix leaves declared names unchanged.

### `RabbitMqOptions`

Bound from section `RabbitMQ` (same pattern as Redis `RedisCache:Prefix`).

| Member | Behavior |
|--------|----------|
| `Prefix` | Raw appsettings value; empty = no prefix |
| `NormalizedPrefix` | Trim; if missing trailing `:`, `_`, `-`, or `.`, append `:` |
| `ApplyPrefix(name)` | Prepends `NormalizedPrefix` unless name is empty or already starts with it |

Applied to declared queue names, exchange names, RPC `ReplyTo`, and `RabbitQueueNames.IntegrationExchange` at declare/publish/consume time. Routing keys from `QueueData.routingKey` are **not** prefixed.

---

## 5. Publishers — `SRT.Core.MessageBroker.RabbitMQ.Publisher`

### `IPublisherMessage`

```csharp
ExchangeData? exchangeData { get; }
List<QueueData> lstQueueData { get; }
Task SendMessageAsync(string message, string? routeKey = null);
```

### `PublisherMessageAbstract`

- Ctor: `(AppConnectionString, ILogger<PublisherMessageAbstract>, IRabbitConnection, IServiceScopeFactory? = null)`
- Own channel with publisher confirms; `SemaphoreSlim` serializes channel ops.
- Publish success = confirm + no `BasicReturn` (`mandatory: true`).
- `PublishTimeout` virtual → default 30s.
- `DisposeAsync` closes **channel only**.

### `PublisherMessageRPCAbstract`

- Waits on exclusive reply queue; `autoAck: true` (no manual Ack).
- `RpcTimeout` virtual → default 30s; cancels pending on timeout.

### `PublisherMessageRPCStaticAbstract`

- Sets `ReplyTo = CallBackQueueName`, then fire-and-forget base send.

---

## 6. Consumers — `SRT.Core.MessageBroker.RabbitMQ.Consumer`

### `IConsumerMessage` : `IHostedService`

```csharp
QueueData queueData { get; }
Task<string?> AfterReceiveData(string data, BasicDeliverEventArgs e);
```

Non-null return = RPC reply body to `ReplyTo`.

### `ConsumerMessageAbstract`

- Ctor: `(AppConnectionString, ILogger, IRabbitConnection, IServiceScopeFactory? = null)`
- Prefetch 1; `autoAck: false`.
- Lifetime `CancellationTokenSource` for deliveries (not `StartAsync` token).
- `IntegrationDeadException` → Nack no-requeue; other errors → retry then Nack no-requeue.
- `DisposeAsync` closes **channel only**.

### `ConsumerOfPublisherResultAbstract`

RPC callback consumer on durable named queue; processes when `CorrelationId` present.

---

## 7. Domain

### `ExchangeData`

Defaults: `type = Topic`, `durable = true`, `autoDelete = true`.

### `QueueData`

Defaults: `routingKey = "*.image.*"`, `durable = false`, `autoAck = true` (unused by consumer — always manual ack).

### `RabbitQueueNames`

`IntegrationExchange = "srt.integration"`; `Dlq(name) => name + ".dlq"`.

### `IntegrationDeadException`

Signals intentional dead-letter path.
