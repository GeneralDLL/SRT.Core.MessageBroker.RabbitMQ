// Ignore Spelling: Rpc

using SRT.Core.MessageBroker.RabbitMQ.Domain;
using SRT.Core.Domain;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using RabbitMQ.Client.Events;

namespace SRT.Core.MessageBroker.RabbitMQ.Consumer
{
    public abstract class ConsumerOfPublisherResultAbstract : ConsumerMessageAbstract
    {
        /// <summary>
        /// نام صف که نتایج Publisher روی آن قرار می‌گیرند.
        /// </summary>
        public abstract string QueueName { get; }

        /// <summary>
        /// منطق پردازش داده‌های دریافتی در حالت RPC.
        /// </summary>
        protected abstract Task AfterReceiveRPCData(string data, BasicDeliverEventArgs e);

        public override QueueData queueData => new()
        {
            name = QueueName,
            autoDelete = false,
            durable = true,
            exclusive = false,
            routingKey = string.Empty,
            autoAck = false
        };

        #region Constructors
        protected ConsumerOfPublisherResultAbstract(
            AppConnectionString appConnectionString,
            ILogger<ConsumerOfPublisherResultAbstract> logger,
            IRabbitConnection rabbitConnection,
            IServiceScopeFactory? scopeFactory = null)
            : base(appConnectionString, logger, rabbitConnection, scopeFactory)
        {
        }
        #endregion

        /// <inheritdoc />
        public override async Task<string?> AfterReceiveData(string data, BasicDeliverEventArgs e)
        {
            if (!string.IsNullOrEmpty(e.BasicProperties?.CorrelationId))
            {
                await AfterReceiveRPCData(data, e).ConfigureAwait(false);
            }

            return null;
        }
    }
}
