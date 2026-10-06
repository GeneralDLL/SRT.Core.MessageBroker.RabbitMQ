# SRT.Core.MessageBroker.RabbitMQ

RabbitMQ plugin for SRT hosts: shared connection, publishers, consumers, RPC, optional TLS.

**Target:** .NET 10

Package docs: **[src/SRT.Core.MessageBroker.RabbitMQ/docs/SRT.Core.MessageBroker.RabbitMQ.README.md](src/SRT.Core.MessageBroker.RabbitMQ/docs/SRT.Core.MessageBroker.RabbitMQ.README.md)**

## Consume

```xml
<ProjectReference Include="..\SRT.Core.MessageBroker.RabbitMQ\src\SRT.Core.MessageBroker.RabbitMQ\SRT.Core.MessageBroker.RabbitMQ.csproj" />
```

Reference **SRT.Core**. Non-empty `lstRabbitMQNamespace` makes Core require this plugin. Configure `ConnectionDB:SRTCore_RabbitMQ`. Default port is **5672** plaintext or **5671** when TLS (`sslEnabled` or `caPath`).

## Quick start

Thin Core `Program.cs`. Put publisher/consumer types in a listed namespace. Recipes: [COOKBOOK](src/SRT.Core.MessageBroker.RabbitMQ/docs/SRT.Core.MessageBroker.RabbitMQ.COOKBOOK.md).
