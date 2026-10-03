using SRT.Core.MessageBroker.RabbitMQ.Domain;
using SRT.Core.Domain;
using SRT.Core.Domain.Diagnostics;

using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using System.Text;

namespace SRT.Core.MessageBroker.RabbitMQ.Publisher
{
    public abstract class PublisherMessageAbstract : IPublisherMessage, IAsyncDisposable
    {
        protected readonly AppConnectionString appConnectionString;
        protected readonly ILogger<PublisherMessageAbstract> logger;
        protected readonly IServiceScopeFactory? _diagScopeFactory;
        protected readonly IRabbitConnection rabbitConnection;

        protected IChannel? channel;
        private bool _topologyDeclared;
        private readonly SemaphoreSlim _channelGate = new(1, 1);
        private bool _disposed;

        #region Constructors
        protected PublisherMessageAbstract(
            AppConnectionString appConnectionString,
            ILogger<PublisherMessageAbstract> logger,
            IRabbitConnection rabbitConnection,
            IServiceScopeFactory? scopeFactory = null)
        {
            this.appConnectionString = appConnectionString;
            this.logger = logger;
            this.rabbitConnection = rabbitConnection ?? throw new ArgumentNullException(nameof(rabbitConnection));
            _diagScopeFactory = scopeFactory;
        }
        #endregion

        #region Basic data For Queue & Exchange
        public abstract ExchangeData? exchangeData { get; }

        public abstract List<QueueData> lstQueueData { get; }

        public virtual BasicProperties basicProperties { get; protected set; } =
            new BasicProperties
            {
                Persistent = true
            };

        /// <summary>Timeout waiting for publisher confirm. Default 30s.</summary>
        protected virtual TimeSpan PublishTimeout => TimeSpan.FromSeconds(30);

        protected string PrefixedQueue(string name) => rabbitConnection.ApplyNamePrefix(name);

        protected string PrefixedExchange(string name) => rabbitConnection.ApplyNamePrefix(name);
        #endregion

        #region Make Ready
        protected async Task EnsureExchangeAndQueueReadyAsync(CancellationToken cancellationToken = default)
        {
            await _channelGate.WaitAsync(cancellationToken).ConfigureAwait(false);
            try
            {
                await EnsureReadyCoreAsync(cancellationToken).ConfigureAwait(false);
            }
            finally
            {
                _channelGate.Release();
            }
        }

        private async Task EnsureReadyCoreAsync(CancellationToken cancellationToken)
        {
            if (channel is null || channel.IsClosed)
            {
                if (channel is not null)
                {
                    try { await channel.DisposeAsync().ConfigureAwait(false); }
                    catch { /* ignore */ }
                    channel = null;
                }

                _topologyDeclared = false;
                var rabbitCfg = appConnectionString.SRTCore_RabbitMQ;
                try
                {
                    var channelOptions = new CreateChannelOptions(
                        publisherConfirmationsEnabled: true,
                        publisherConfirmationTrackingEnabled: true);
                    channel = await rabbitConnection
                        .CreateChannelAsync(channelOptions, cancellationToken)
                        .ConfigureAwait(false);
                }
                catch (Exception ex)
                {
                    await DiagnosticReporter.ReportAsync(
                        ex,
                        new DiagnosticEnvelope
                        {
                            Category = DiagnosticCategories.RabbitConnect,
                            Operation = "Connect",
                            Outcome = DiagnosticOutcomes.Failed,
                            Severity = "Critical",
                            Target = $"{rabbitCfg.host}:{rabbitCfg.GetRabbitMqPort()}",
                            PersistToErrorDb = true,
                            Detail = new DiagnosticDetail
                            {
                                Ssl = rabbitCfg.UseTls,
                                VirtualHost = rabbitCfg.GetVirtualHost(),
                            },
                        },
                        logger,
                        _diagScopeFactory).ConfigureAwait(false);
                    throw;
                }
            }

            if (_topologyDeclared || channel is null)
                return;

            if (exchangeData != null)
            {
                await channel
                    .ExchangeDeclareAsync(
                        exchange: PrefixedExchange(exchangeData.name),
                        autoDelete: exchangeData.autoDelete,
                        durable: exchangeData.durable,
                        type: exchangeData.type,
                        cancellationToken: cancellationToken)
                    .ConfigureAwait(false);
            }

            foreach (var queueData in lstQueueData)
            {
                var queueName = PrefixedQueue(queueData.name);
                await channel
                    .QueueDeclareAsync(
                        queue: queueName,
                        durable: queueData.durable,
                        exclusive: queueData.exclusive,
                        autoDelete: queueData.autoDelete,
                        arguments: queueData.arguments,
                        cancellationToken: cancellationToken)
                    .ConfigureAwait(false);

                if (exchangeData != null)
                {
                    await channel
                        .QueueBindAsync(
                            queue: queueName,
                            exchange: PrefixedExchange(exchangeData.name),
                            routingKey: queueData.routingKey,
                            cancellationToken: cancellationToken)
                        .ConfigureAwait(false);
                }
            }

            _topologyDeclared = true;
        }
        #endregion

        #region Send Message
        public virtual async Task SendMessageAsync(string message, string? routeKey = null)
        {
            await _channelGate.WaitAsync().ConfigureAwait(false);
            try
            {
                await EnsureReadyCoreAsync(CancellationToken.None).ConfigureAwait(false);

                if (string.IsNullOrWhiteSpace(basicProperties.CorrelationId))
                    basicProperties.CorrelationId = Guid.NewGuid().ToString("D");
                if (string.IsNullOrWhiteSpace(basicProperties.MessageId))
                    basicProperties.MessageId = Guid.NewGuid().ToString("D");

                var qType = exchangeData?.type switch
                {
                    null => "Queue",
                    ExchangeType.Direct => "Direct",
                    ExchangeType.Fanout => "Fanout",
                    ExchangeType.Topic => "Topic",
                    _ => "None",
                };

                var body = Encoding.UTF8.GetBytes(message);

                switch (exchangeData?.type)
                {
                    case null:
                        foreach (var queueData in lstQueueData)
                            await PublishConfirmedAsync(string.Empty, PrefixedQueue(queueData.name), body).ConfigureAwait(false);
                        break;
                    case ExchangeType.Direct:
                        await PublishConfirmedAsync(PrefixedExchange(exchangeData.name), routeKey ?? string.Empty, body).ConfigureAwait(false);
                        break;
                    case ExchangeType.Fanout:
                        await PublishConfirmedAsync(PrefixedExchange(exchangeData.name), string.Empty, body).ConfigureAwait(false);
                        break;
                    case ExchangeType.Topic:
                        await PublishConfirmedAsync(PrefixedExchange(exchangeData.name), routeKey ?? string.Empty, body).ConfigureAwait(false);
                        break;
                }

                logger.LogInformation(
                    "RabbitMQ - Send Message - {Publisher}({Type}): {Exchange}",
                    GetType().Name,
                    qType,
                    exchangeData?.ToString());
            }
            finally
            {
                _channelGate.Release();
            }
        }
        #endregion

        private async Task PublishConfirmedAsync(
            string exchange,
            string routingKey,
            ReadOnlyMemory<byte> body,
            CancellationToken cancellationToken = default)
        {
            if (channel is null)
                throw new InvalidOperationException("RabbitMQ channel is not ready.");

            var returned = false;
            Task OnReturn(object s, BasicReturnEventArgs e)
            {
                returned = true;
                return Task.CompletedTask;
            }

            channel.BasicReturnAsync += OnReturn;
            try
            {
                using var cts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
                cts.CancelAfter(PublishTimeout);

                await channel
                    .BasicPublishAsync(
                        exchange: exchange,
                        routingKey: routingKey,
                        mandatory: true,
                        basicProperties: basicProperties,
                        body: body,
                        cancellationToken: cts.Token)
                    .ConfigureAwait(false);

                if (returned)
                {
                    var rabbitCfg = appConnectionString.SRTCore_RabbitMQ;
                    var ex = new InvalidOperationException(
                        $"RabbitMQ message was returned as unroutable (exchange='{exchange}', routingKey='{routingKey}').");
                    await DiagnosticReporter.ReportAsync(
                        ex,
                        new DiagnosticEnvelope
                        {
                            Category = DiagnosticCategories.RabbitPublish,
                            Operation = "Publish",
                            Outcome = DiagnosticOutcomes.Failed,
                            Severity = "Error",
                            CorrelationId = basicProperties.CorrelationId,
                            Target = $"{rabbitCfg.host}:{rabbitCfg.GetRabbitMqPort()}",
                            PersistToErrorDb = true,
                            Detail = new DiagnosticDetail
                            {
                                Ssl = rabbitCfg.UseTls,
                                VirtualHost = rabbitCfg.GetVirtualHost(),
                                RoutingKey = routingKey,
                                MessageId = basicProperties.MessageId,
                            },
                        },
                        logger,
                        _diagScopeFactory).ConfigureAwait(false);
                    throw ex;
                }
            }
            finally
            {
                channel.BasicReturnAsync -= OnReturn;
            }
        }

        public async ValueTask DisposeAsync()
        {
            if (_disposed)
                return;
            _disposed = true;

            if (channel is not null)
            {
                try
                {
                    if (!channel.IsClosed)
                        await channel.CloseAsync().ConfigureAwait(false);
                }
                catch { /* ignore */ }

                try { await channel.DisposeAsync().ConfigureAwait(false); }
                catch { /* ignore */ }
                channel = null;
            }

            _channelGate.Dispose();
            // Shared IConnection is owned by IRabbitConnection — do not close it here.
        }
    }
}
