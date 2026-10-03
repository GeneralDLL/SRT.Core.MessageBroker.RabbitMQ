using SRT.Core.Domain;

using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using System.Collections.Concurrent;
using System.Text;

namespace SRT.Core.MessageBroker.RabbitMQ.Publisher
{
    public abstract class PublisherMessageRPCAbstract : PublisherMessageAbstract
    {
        public abstract Task AnalyzeResult(
            string dataResult,
            string routeKey,
            IReadOnlyBasicProperties propsFromSender,
            BasicDeliverEventArgs e);

        /// <summary>Default RPC wait timeout. Override to customize.</summary>
        protected virtual TimeSpan RpcTimeout => TimeSpan.FromSeconds(30);

        private readonly ConcurrentDictionary<string, TaskCompletionSource<(string message, BasicDeliverEventArgs e)>> _callbackMapper =
            new();

        #region Constructors
        protected PublisherMessageRPCAbstract(
            AppConnectionString appConnectionString,
            ILogger<PublisherMessageRPCAbstract> logger,
            IRabbitConnection rabbitConnection,
            IServiceScopeFactory? scopeFactory = null)
            : base(appConnectionString, logger, rabbitConnection, scopeFactory)
        {
        }
        #endregion

        #region Send Message
        public override async Task SendMessageAsync(string message, string? routeKey = null)
        {
            await EnsureExchangeAndQueueReadyAsync().ConfigureAwait(false);

            var correlationId = Guid.NewGuid().ToString("D");
            var replyTo = PrefixedQueue("QueueTemp" + Guid.NewGuid().ToString("N"));

            basicProperties = new BasicProperties
            {
                CorrelationId = correlationId,
                ReplyTo = replyTo,
                Persistent = true
            };

            var tcs = new TaskCompletionSource<(string message, BasicDeliverEventArgs e)>(
                TaskCreationOptions.RunContinuationsAsynchronously);
            _callbackMapper.TryAdd(correlationId, tcs);

            try
            {
                await EnsureReceiveResultAsync(replyTo).ConfigureAwait(false);
                await base.SendMessageAsync(message, routeKey).ConfigureAwait(false);

                using var cts = new CancellationTokenSource(RpcTimeout);
                await using var reg = cts.Token.Register(() =>
                {
                    if (_callbackMapper.TryRemove(correlationId, out var pending))
                        pending.TrySetCanceled();
                });

                var r = await tcs.Task.ConfigureAwait(false);
                await AnalyzeResult(r.message, r.e.RoutingKey, r.e.BasicProperties, r.e).ConfigureAwait(false);
            }
            catch
            {
                _callbackMapper.TryRemove(correlationId, out _);
                throw;
            }
        }
        #endregion

        private async Task EnsureReceiveResultAsync(string replyQueueName)
        {
            if (channel is null)
                throw new InvalidOperationException("RabbitMQ channel is not ready.");

            var queue = await channel
                .QueueDeclareAsync(queue: replyQueueName, exclusive: true, autoDelete: true)
                .ConfigureAwait(false);

            var consumer = new AsyncEventingBasicConsumer(channel);
            consumer.ReceivedAsync += (_, ea) =>
            {
                var correlationId = ea.BasicProperties.CorrelationId;
                if (!string.IsNullOrEmpty(correlationId)
                    && _callbackMapper.TryRemove(correlationId, out var pending))
                {
                    var response = Encoding.UTF8.GetString(ea.Body.ToArray());
                    pending.TrySetResult((response, ea));
                }
                // autoAck: true — do not BasicAck
                return Task.CompletedTask;
            };

            await channel.BasicConsumeAsync(queue.QueueName, autoAck: true, consumer).ConfigureAwait(false);
        }
    }
}
