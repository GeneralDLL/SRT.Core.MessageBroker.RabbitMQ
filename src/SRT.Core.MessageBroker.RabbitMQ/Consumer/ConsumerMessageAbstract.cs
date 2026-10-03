using SRT.Core.MessageBroker.RabbitMQ.Domain;
using SRT.Core.Domain;
using SRT.Core.Domain.Diagnostics;

using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using System.Text;

namespace SRT.Core.MessageBroker.RabbitMQ.Consumer
{
    public abstract class ConsumerMessageAbstract : IConsumerMessage, IAsyncDisposable
    {
        protected readonly AppConnectionString appConnectionString;
        protected readonly ILogger _logger;
        protected readonly IServiceScopeFactory? _diagScopeFactory;
        protected readonly IRabbitConnection rabbitConnection;

        private IChannel? _channel;
        private bool _isWorking;
        private CancellationTokenSource? _lifetimeCts;
        private int _inFlight;

        /// <summary>After this many broker deaths, Nack without requeue (DLQ).</summary>
        protected virtual int MaxRedeliveries => 10;

        protected ConsumerMessageAbstract(
            AppConnectionString connectionString,
            ILogger logger,
            IRabbitConnection rabbitConnection,
            IServiceScopeFactory? scopeFactory = null)
        {
            appConnectionString = connectionString ?? throw new ArgumentNullException(nameof(connectionString));
            _logger = logger ?? throw new ArgumentNullException(nameof(logger));
            this.rabbitConnection = rabbitConnection ?? throw new ArgumentNullException(nameof(rabbitConnection));
            _diagScopeFactory = scopeFactory;
        }

        private const string RetryCountHeader = "x-retry-count";

        private static long GetAttemptCount(BasicDeliverEventArgs ea)
        {
            if (ea.BasicProperties?.Headers is null)
                return ea.Redelivered ? 1 : 0;

            if (ea.BasicProperties.Headers.TryGetValue(RetryCountHeader, out var retryObj)
                && retryObj is not null)
            {
                return Convert.ToInt64(retryObj);
            }

            if (ea.BasicProperties.Headers.TryGetValue("x-delivery-count", out var deliveryCount)
                && deliveryCount is not null)
            {
                return Convert.ToInt64(deliveryCount);
            }

            if (ea.BasicProperties.Headers.TryGetValue("x-death", out var raw) && raw is IList<object> list)
            {
                long total = 0;
                foreach (var item in list)
                {
                    if (item is IDictionary<string, object> table
                        && table.TryGetValue("count", out var countObj)
                        && countObj is not null)
                    {
                        total += Convert.ToInt64(countObj);
                    }
                }
                return total;
            }

            return ea.Redelivered ? 1 : 0;
        }

        private async Task RepublishWithRetryAsync(
            BasicDeliverEventArgs ea,
            long nextAttempt,
            CancellationToken cancellationToken)
        {
            var headers = new Dictionary<string, object?>();
            if (ea.BasicProperties?.Headers is not null)
            {
                foreach (var kv in ea.BasicProperties.Headers)
                    headers[kv.Key] = kv.Value;
            }

            headers[RetryCountHeader] = nextAttempt;

            var props = new BasicProperties
            {
                Persistent = ea.BasicProperties?.Persistent ?? true,
                MessageId = ea.BasicProperties?.MessageId,
                ContentType = ea.BasicProperties?.ContentType ?? "application/json",
                CorrelationId = ea.BasicProperties?.CorrelationId,
                Headers = headers,
            };

            await _channel!.BasicPublishAsync(
                exchange: string.Empty,
                routingKey: PrefixedQueueName,
                mandatory: true,
                basicProperties: props,
                body: ea.Body,
                cancellationToken: cancellationToken).ConfigureAwait(false);
        }

        #region Abstract Members
        public abstract QueueData queueData { get; }

        public abstract Task<string?> AfterReceiveData(string data, BasicDeliverEventArgs e);

        protected string PrefixedQueueName => rabbitConnection.ApplyNamePrefix(queueData.name);

        protected string PrefixedIntegrationExchange =>
            rabbitConnection.ApplyNamePrefix(RabbitQueueNames.IntegrationExchange);
        #endregion

        #region Init
        private async Task EnsureExchangeAndQueueReadyAsync(CancellationToken cancellationToken)
        {
            if (_channel is { IsClosed: false })
                return;

            if (_channel is not null)
            {
                try { await _channel.DisposeAsync().ConfigureAwait(false); }
                catch { /* ignore */ }
                _channel = null;
            }

            var rabbitCfg = appConnectionString.SRTCore_RabbitMQ;
            try
            {
                _channel = await rabbitConnection
                    .CreateChannelAsync(cancellationToken: cancellationToken)
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
                            Queue = PrefixedQueueName,
                        },
                    },
                    _logger,
                    _diagScopeFactory).ConfigureAwait(false);
                throw;
            }

            var queue = await _channel.QueueDeclareAsync(
                queue: PrefixedQueueName,
                durable: queueData.durable,
                exclusive: queueData.exclusive,
                autoDelete: queueData.autoDelete,
                arguments: queueData.arguments,
                cancellationToken: cancellationToken).ConfigureAwait(false);

            if (!string.IsNullOrWhiteSpace(queueData.routingKey))
            {
                await _channel.ExchangeDeclareAsync(
                    exchange: PrefixedIntegrationExchange,
                    type: ExchangeType.Direct,
                    durable: true,
                    autoDelete: false,
                    cancellationToken: cancellationToken).ConfigureAwait(false);

                await _channel.QueueBindAsync(
                    queue: PrefixedQueueName,
                    exchange: PrefixedIntegrationExchange,
                    routingKey: queueData.routingKey,
                    cancellationToken: cancellationToken).ConfigureAwait(false);
            }

            await _channel.BasicQosAsync(
                prefetchSize: 0,
                prefetchCount: 1,
                global: false,
                cancellationToken: cancellationToken).ConfigureAwait(false);

            var lifetimeToken = _lifetimeCts!.Token;
            var consumer = new AsyncEventingBasicConsumer(_channel);
            consumer.ReceivedAsync += async (_, ea) =>
            {
                Interlocked.Increment(ref _inFlight);
                try
                {
                    await HandleDeliveryAsync(ea, lifetimeToken).ConfigureAwait(false);
                }
                finally
                {
                    Interlocked.Decrement(ref _inFlight);
                }
            };

            await _channel.BasicConsumeAsync(
                queue: queue.QueueName,
                autoAck: false,
                consumer: consumer,
                cancellationToken: cancellationToken).ConfigureAwait(false);
        }

        private async Task HandleDeliveryAsync(BasicDeliverEventArgs ea, CancellationToken lifetimeToken)
        {
            var correlationId = ea.BasicProperties?.CorrelationId;
            var messageId = ea.BasicProperties?.MessageId;

            // Per-message DI scope for handlers that resolve scoped services.
            await using var scope = _diagScopeFactory?.CreateAsyncScope();

            try
            {
                var message = Encoding.UTF8.GetString(ea.Body.ToArray());
                var response = await AfterReceiveData(message, ea).ConfigureAwait(false);

                if (!string.IsNullOrWhiteSpace(response)
                    && !string.IsNullOrWhiteSpace(ea.BasicProperties?.ReplyTo))
                {
                    var replyProps = new BasicProperties { CorrelationId = ea.BasicProperties!.CorrelationId };
                    var responseBytes = Encoding.UTF8.GetBytes(response);
                    await _channel!.BasicPublishAsync(
                        exchange: string.Empty,
                        routingKey: ea.BasicProperties.ReplyTo!,
                        mandatory: true,
                        basicProperties: replyProps,
                        body: responseBytes,
                        cancellationToken: lifetimeToken).ConfigureAwait(false);
                }

                await _channel!.BasicAckAsync(ea.DeliveryTag, multiple: false, cancellationToken: lifetimeToken)
                              .ConfigureAwait(false);
            }
            catch (IntegrationDeadException ex)
            {
                _logger.LogError(ex,
                    "RabbitMQ consumer dead-letter: Consumer={Consumer} Queue={Queue} MessageId={MessageId} CorrelationId={CorrelationId} RoutingKey={RoutingKey} Outcome=Dead",
                    GetType().Name,
                    PrefixedQueueName,
                    messageId,
                    correlationId,
                    ea.RoutingKey);

                await DiagnosticReporter.ReportAsync(
                    ex,
                    BuildConsumeEnvelope(
                        DiagnosticOutcomes.Dead,
                        correlationId,
                        messageId,
                        ea.RoutingKey,
                        attempt: null,
                        persistToDb: true),
                    _logger,
                    _diagScopeFactory).ConfigureAwait(false);

                await _channel!.BasicNackAsync(ea.DeliveryTag, multiple: false, requeue: false, cancellationToken: lifetimeToken)
                              .ConfigureAwait(false);
            }
            catch (Exception ex)
            {
                var attempt = GetAttemptCount(ea);
                var canRetry = attempt < MaxRedeliveries;
                var outcome = canRetry ? DiagnosticOutcomes.Retrying : DiagnosticOutcomes.Exhausted;

                _logger.LogError(ex,
                    "RabbitMQ consumer failed: Consumer={Consumer} Queue={Queue} MessageId={MessageId} CorrelationId={CorrelationId} Attempt={Attempt} MaxAttempts={MaxAttempts} Outcome={Outcome}",
                    GetType().Name,
                    PrefixedQueueName,
                    messageId,
                    correlationId,
                    attempt,
                    MaxRedeliveries,
                    outcome);

                await DiagnosticReporter.ReportAsync(
                    ex,
                    BuildConsumeEnvelope(
                        outcome,
                        correlationId,
                        messageId,
                        ea.RoutingKey,
                        attempt,
                        persistToDb: !canRetry),
                    _logger,
                    _diagScopeFactory).ConfigureAwait(false);

                if (canRetry)
                {
                    await RepublishWithRetryAsync(ea, attempt + 1, lifetimeToken).ConfigureAwait(false);
                    await _channel!.BasicAckAsync(ea.DeliveryTag, multiple: false, cancellationToken: lifetimeToken)
                                  .ConfigureAwait(false);
                }
                else
                {
                    await _channel!.BasicNackAsync(ea.DeliveryTag, multiple: false, requeue: false, cancellationToken: lifetimeToken)
                                  .ConfigureAwait(false);
                }
            }
        }
        #endregion

        #region Start / Stop
        public async Task StartAsync(CancellationToken cancellationToken = default)
        {
            if (_isWorking)
                return;

            _isWorking = true;
            _lifetimeCts = new CancellationTokenSource();

            var rabbitCfg = appConnectionString.SRTCore_RabbitMQ;
            _logger.LogInformation(
                "RabbitMQ - Start Consumer: {Consumer} Queue={Queue} VirtualHost={VirtualHost} RoutingKey={RoutingKey} Ssl={Ssl}",
                GetType().Name,
                PrefixedQueueName,
                rabbitCfg.GetVirtualHost(),
                queueData.routingKey,
                rabbitCfg.UseTls);

            await EnsureExchangeAndQueueReadyAsync(cancellationToken).ConfigureAwait(false);
        }

        public async Task StopAsync(CancellationToken cancellationToken = default)
        {
            if (!_isWorking)
                return;

            _isWorking = false;
            _logger.LogInformation("RabbitMQ - Stop Consumer: {Consumer}", GetType().Name);

            try { _lifetimeCts?.Cancel(); }
            catch { /* ignore */ }

            // Brief wait for in-flight deliveries.
            var deadline = DateTime.UtcNow.AddSeconds(5);
            while (Volatile.Read(ref _inFlight) > 0 && DateTime.UtcNow < deadline)
            {
                await Task.Delay(50, cancellationToken).ConfigureAwait(false);
            }

            await DisposeAsync().ConfigureAwait(false);
        }
        #endregion

        private DiagnosticEnvelope BuildConsumeEnvelope(
            string outcome,
            string? correlationId,
            string? messageId,
            string? routingKey,
            long? attempt,
            bool persistToDb)
        {
            var rabbitCfg = appConnectionString.SRTCore_RabbitMQ;
            return new DiagnosticEnvelope
            {
                Category = DiagnosticCategories.RabbitConsume,
                Operation = "Consume",
                Outcome = outcome,
                Severity = persistToDb ? "Error" : "Warning",
                CorrelationId = correlationId,
                Target = $"{rabbitCfg.host}:{rabbitCfg.GetRabbitMqPort()}",
                PersistToErrorDb = persistToDb,
                Detail = new DiagnosticDetail
                {
                    Ssl = rabbitCfg.UseTls,
                    VirtualHost = rabbitCfg.GetVirtualHost(),
                    Queue = PrefixedQueueName,
                    RoutingKey = routingKey,
                    MessageId = messageId,
                    Attempt = attempt,
                    MaxAttempts = MaxRedeliveries,
                    DlqName = RabbitQueueNames.Dlq(PrefixedQueueName),
                },
            };
        }

        #region Dispose
        public async ValueTask DisposeAsync()
        {
            if (_channel is not null)
            {
                try
                {
                    if (!_channel.IsClosed)
                        await _channel.CloseAsync().ConfigureAwait(false);
                }
                catch { /* ignore */ }

                try { await _channel.DisposeAsync().ConfigureAwait(false); }
                catch { /* ignore */ }
                _channel = null;
            }

            _lifetimeCts?.Dispose();
            _lifetimeCts = null;
            // Shared IConnection is owned by IRabbitConnection — do not close it here.
        }
        #endregion
    }
}
