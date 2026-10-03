using SRT.Core.Domain;

using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using RabbitMQ.Client;

namespace SRT.Core.MessageBroker.RabbitMQ.Publisher
{
    public abstract class PublisherMessageRPCStaticAbstract : PublisherMessageAbstract
    {
        public abstract string CallBackQueueName { get; }

        #region Constructors
        protected PublisherMessageRPCStaticAbstract(
            AppConnectionString appConnectionString,
            ILogger<PublisherMessageRPCStaticAbstract> logger,
            IRabbitConnection rabbitConnection,
            IServiceScopeFactory? scopeFactory = null)
            : base(appConnectionString, logger, rabbitConnection, scopeFactory)
        {
        }
        #endregion

        #region Send Message
        public override async Task SendMessageAsync(string message, string? routeKey = null)
        {
            basicProperties = new BasicProperties
            {
                CorrelationId = Guid.NewGuid().ToString("D"),
                ReplyTo = PrefixedQueue(CallBackQueueName),
                Persistent = true
            };

            await base.SendMessageAsync(message, routeKey).ConfigureAwait(false);
        }
        #endregion
    }
}
