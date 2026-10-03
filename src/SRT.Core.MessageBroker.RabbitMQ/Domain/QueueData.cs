// Ignore Spelling: Ack

using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace SRT.Core.MessageBroker.RabbitMQ.Domain
{
    public class QueueData
    {
        public string name { get; set; } = string.Empty;
        /// <summary>
        /// for created
        /// </summary>
        public string routingKey { get; set; } = "*.image.*";
        /// <summary>
        /// for created
        /// true => for not delete after create
        /// به حفظ پیام‌ها در صورت خرابی سیستم کمک می‌کند
        /// </summary>
        public bool durable { get; set; }
        /// <summary>
        /// for created
        /// به معنای حذف خودکار آن‌ها هنگامی است که هیچ مشترکی به آن‌ها متصل نیست.
        /// </summary>
        public bool autoDelete { get; set; }
        /// <summary>
        /// for created
        /// صف تنها برای یک اتصال خاص قابل دسترسی است.
        /// RPC ها معمولاً از صف‌های انحصاری استفاده می‌کنند.
        /// </summary>
        public bool exclusive { get; set; }
        /// <summary>
        /// for recive data
        /// </summary>
        public bool autoAck { get; set; } = true;

        public IDictionary<string, object?>? arguments { get; set; } = null;

        public override string ToString()
        {
            return $"{name} - routeKey:{routingKey} - {durable} - {autoDelete} - {autoAck} - {exclusive}";
        }
    }
}


