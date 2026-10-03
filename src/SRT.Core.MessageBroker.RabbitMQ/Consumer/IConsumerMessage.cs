using Microsoft.AspNetCore.Builder;
using Microsoft.Extensions.Hosting;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace SRT.Core.MessageBroker.RabbitMQ.Consumer
{
    public interface IConsumerMessage : IHostedService
    {
        Domain.QueueData queueData { get; }

        /// <summary>
        /// if has return data => RPC Structure
        /// routeKey = e.RoutingKey
        /// IReadOnlyBasicProperties propsFromSender = e.BasicProperties 
        /// </summary>
        /// <param name="data"></param>
        /// <param name="e"></param>
        /// <returns></returns>
        Task<string?> AfterReceiveData(string data, BasicDeliverEventArgs e);
    }
}


