using RabbitMQ.Client;

namespace SRT.Core.MessageBroker.RabbitMQ
{
    /// <summary>
    /// Owns the shared RabbitMQ <see cref="IConnection"/>. Publishers and consumers
    /// create their own channels from this connection and must not dispose it.
    /// </summary>
    public interface IRabbitConnection : IAsyncDisposable
    {
        /// <summary>Normalized appsettings <c>RabbitMQ:Prefix</c>. Empty when unset.</summary>
        string Prefix { get; }

        /// <summary>Prepends <see cref="Prefix"/> unless the name is empty or already prefixed.</summary>
        string ApplyNamePrefix(string name);

        Task<IConnection> GetConnectionAsync(CancellationToken cancellationToken = default);

        Task<IChannel> CreateChannelAsync(
            CreateChannelOptions? options = null,
            CancellationToken cancellationToken = default);
    }
}
