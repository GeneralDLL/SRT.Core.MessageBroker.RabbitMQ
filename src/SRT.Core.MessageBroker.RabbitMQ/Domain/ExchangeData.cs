using RabbitMQ.Client;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace SRT.Core.MessageBroker.RabbitMQ.Domain
{
    public class ExchangeData
    {
        public string type { get; set; } = ExchangeType.Topic;

        public string name { get; set; } = string.Empty;

        /// <summary>
        /// for created
        /// به حفظ پیام‌ها در صورت خرابی سیستم کمک می‌کند
        /// </summary>
        public bool durable { get; set; } = true;
        /// <summary>
        /// for created
        /// به معنای حذف خودکار آن‌ها هنگامی است که هیچ مشترکی به آن‌ها متصل نیست.
        /// </summary>
        public bool autoDelete { get; set; } = true;

        public override string ToString()
        {
            return $"{name} - durable: {durable} - autoDelete: {autoDelete}";
        }
    }
}


