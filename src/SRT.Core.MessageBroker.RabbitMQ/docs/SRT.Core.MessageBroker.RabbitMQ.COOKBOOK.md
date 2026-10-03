# SRT.Core.MessageBroker.RabbitMQ — Cookbook

Recipes match [SRT.Core.MessageBroker.RabbitMQ.API_REFERENCE.md](SRT.Core.MessageBroker.RabbitMQ.API_REFERENCE.md).

---

## 1. Host startup (self-contained)

### `Program.cs` (keep thin)

```csharp
var dmAppSetting = args.SRT_ConfigController();
var (builder, config) = StartAppConfigurations.SRT_ConfigController(args, dmAppSetting);
var app = await builder.SRT_ConfigBuilder(config)
    .SRT_ConfigBuilderAndBuildApp(config)
    .SRT_ConfigApp(config);
app.Run();
```

Do **not** call `UseAuthorization` / `MapControllers`. Do **not** register RabbitMQ DI in `Program.cs`.

### `BasicProjectConfig`

```csharp
public static class BasicProjectConfig
{
    public static MyAppSetting SRT_ConfigController(this string[] args)
    {
        return new MyAppSetting
        {
            UseErrorLogSQL = false,
            UseSerilog = true,
            lstRabbitMQNamespace =
            {
                "SRT.Test.RabbitMQ.Messaging" // namespace prefix of publishers/consumers
            },
            SwaggerConfig = new SwaggerDomain
            {
                swaggerTitle = "SRT.Test.RabbitMQ",
                RoutePrefix = true,
                SwaggerInProduction = false,
            }
        };
    }

    public static WebApplicationBuilder SRT_ConfigBuilder(this WebApplicationBuilder builder, MyAppSetting config)
    {
        // Local DI only (e.g. in-memory last-message store for tests).
        return builder;
    }

    public static async Task<WebApplication> SRT_ConfigApp(this Task<WebApplication> appTask, MyAppSetting config)
    {
        var app = await appTask;
        return app.SRT_ConfigApp(config);
    }

    public static WebApplication SRT_ConfigApp(this WebApplication app, MyAppSetting config)
        => app;
}
```

Web project must **ProjectReference** `SRT.Core.MessageBroker.RabbitMQ`.

---

## 2. Appsettings — required in **every** environment file

For AIs wiring RabbitMQ the first time: stop and ask for TLS yes/no and custom CA vs system trust. See [AI_RULES](SRT.Core.MessageBroker.RabbitMQ.AI_RULES.md).

### Plaintext (5672)

```json
{
  "Service": {
    "Id": 9101,
    "Name": "SRT.Test.RabbitMQ"
  },
  "ConnectionDB": {
    "SRTCore_RabbitMQ": {
      "connectionType": "rabbitmq",
      "host": "localhost",
      "port": "5672",
      "user": "guest",
      "pass": "guest",
      "virtualHost": "/",
      "sslEnabled": "false"
    }
  },
  "RabbitMQ": {
    "Prefix": "srt:test:"
  }
}
```

Omit `RabbitMQ:Prefix` (or set it empty) to use declared queue/exchange names as-is. When set, the plugin prepends it (adds a trailing `:` if the value has no `:`, `_`, `-`, or `.` suffix), matching Redis `RedisCache:Prefix`.

### TLS with private CA (5671)

```json
"SRTCore_RabbitMQ": {
  "connectionType": "rabbitmq",
  "host": "localhost",
  "port": "5671",
  "user": "guest",
  "pass": "guest",
  "virtualHost": "/",
  "sslEnabled": "true",
  "caPath": "tls/ca.pem"
}
```

- `sslEnabled=true` without `caPath` → TLS + system trust.
- Non-empty `caPath` alone also enables TLS (`UseTls`).
- Missing/invalid `caPath` throws — no plaintext fallback.

Repeat `Service` in every environment appsettings file.

---

## 3. Sample publisher + consumer

```csharp
namespace SRT.Test.RabbitMQ.Messaging;

public sealed class DemoPublisher : PublisherMessageAbstract
{
    public DemoPublisher(
        AppConnectionString cs,
        ILogger<PublisherMessageAbstract> logger,
        IRabbitConnection rabbit,
        IServiceScopeFactory scopeFactory)
        : base(cs, logger, rabbit, scopeFactory) { }

    public override ExchangeData? exchangeData => null;
    public override List<QueueData> lstQueueData =>
    [
        new() { name = "srt.test.demo", durable = true, autoDelete = false, routingKey = "" }
    ];
}

public sealed class DemoConsumer : ConsumerMessageAbstract
{
    private readonly LastMessageStore _store;

    public DemoConsumer(
        AppConnectionString cs,
        ILogger<DemoConsumer> logger,
        IRabbitConnection rabbit,
        IServiceScopeFactory scopeFactory,
        LastMessageStore store)
        : base(cs, logger, rabbit, scopeFactory)
    {
        _store = store;
    }

    public override QueueData queueData =>
        new() { name = "srt.test.demo", durable = true, autoDelete = false, routingKey = "" };

    public override Task<string?> AfterReceiveData(string data, BasicDeliverEventArgs e)
    {
        _store.Last = data;
        return Task.FromResult<string?>(null);
    }
}
```

Note: consumers are created via `Activator` with `(AppConnectionString, ILogger, IRabbitConnection, IServiceScopeFactory)`. Extra ctor parameters are not supported unless you change registration — keep the 3–4 arg shape for hosted consumers, or resolve other deps from a scope inside `AfterReceiveData`.

Prefer resolving shared stores from a **singleton** registered in DI and captured via a static/service locator only if needed; for tests, register the store as singleton and use a consumer ctor that matches Activator (inject store via a static holder or wrap in a singleton accessor). The test hosts use a singleton `LastMessageStore` resolved through a small adapter registered before plugin scan where possible — or store as `ILastMessageStore` singleton accessed from consumer via constructor that Activator can fill: Activator only passes 4 args. So use a static/singleton accessor pattern for test stores:

```csharp
public sealed class LastMessageStore
{
    public string? Last { get; set; }
}

// Consumer uses IServiceScopeFactory to resolve LastMessageStore inside AfterReceiveData.
```

---

## 4. Local Docker (plaintext + TLS together)

```bash
# from SRT.Core.TestApplications/tls
powershell -File ./generate-rabbit-certs.ps1
docker compose -f docker-compose.rabbitmq.yml up -d
```

- Plaintext AMQP: `localhost:5672`
- TLS AMQPS: `localhost:5671` with `caPath: tls/ca.pem` on the TLS test host

RabbitMQ server config uses `listeners.tcp` + `listeners.ssl` and `ssl_options` (not Redis `tls-auth-clients`).

---

## 5. Dual test hosts

| Host | SSL | Port |
|------|-----|------|
| `SRT.Test.RabbitMQ` | No | 5672 |
| `SRT.Test.RabbitMQ.Tls` | Yes | 5671 |

Each exposes an HTTP endpoint to publish and poll the last consumed message (same modes, different transport).
