namespace SRT.Core.MessageBroker.RabbitMQ
{
    public class IntegrationDeadException : Exception
    {
        public IntegrationDeadException(string message) : base(message) { }
        public IntegrationDeadException(string message, Exception inner) : base(message, inner) { }
    }
}
