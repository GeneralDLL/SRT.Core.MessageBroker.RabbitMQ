namespace SRT.Core.MessageBroker.RabbitMQ.Domain
{
    public static class RabbitQueueNames
    {
        public const string IntegrationExchange = "srt.integration";

        public static string Dlq(string queueName) => $"{queueName}.dlq";
    }
}
