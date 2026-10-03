using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace SRT.Core.MessageBroker.RabbitMQ.Publisher
{
    public interface IPublisherMessage
    {
        Domain.ExchangeData? exchangeData { get; }

        List<Domain.QueueData> lstQueueData { get; }

        Task SendMessageAsync(string message, string? routeKey = null);
    }
}


