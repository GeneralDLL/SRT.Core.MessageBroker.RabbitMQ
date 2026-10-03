using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using RabbitMQ.Client;
using SRT.Core.Domain;
using SRT.Core.Domain.Diagnostics;

namespace SRT.Core.MessageBroker.RabbitMQ
{
    /// <summary>
    /// Singleton shared RabbitMQ connection with single-flight connect and limited retry.
    /// </summary>
    public sealed class RabbitConnection : IRabbitConnection
    {
        private readonly GeneralConnectionString _cfg;
        private readonly RabbitMqOptions _options;
        private readonly string? _contentRoot;
        private readonly ILogger _logger;
        private readonly IServiceScopeFactory? _diagScopeFactory;
        private readonly SemaphoreSlim _gate = new(1, 1);

        private IConnection? _connection;
        private bool _disposed;

        private const int MaxInitialRetries = 3;

        public RabbitConnection(
            GeneralConnectionString cfg,
            string? contentRoot,
            ILogger logger,
            IServiceScopeFactory? diagScopeFactory = null,
            RabbitMqOptions? options = null)
        {
            _cfg = cfg ?? throw new ArgumentNullException(nameof(cfg));
            _options = options ?? new RabbitMqOptions();
            _contentRoot = contentRoot;
            _logger = logger ?? throw new ArgumentNullException(nameof(logger));
            _diagScopeFactory = diagScopeFactory;
        }

        public string Prefix => _options.NormalizedPrefix;

        public string ApplyNamePrefix(string name) => _options.ApplyPrefix(name);

        public async Task<IConnection> GetConnectionAsync(CancellationToken cancellationToken = default)
        {
            ObjectDisposedException.ThrowIf(_disposed, this);

            if (_connection is { IsOpen: true })
                return _connection;

            await _gate.WaitAsync(cancellationToken).ConfigureAwait(false);
            try
            {
                ObjectDisposedException.ThrowIf(_disposed, this);

                if (_connection is { IsOpen: true })
                    return _connection;

                if (_connection is not null)
                {
                    try { await _connection.DisposeAsync().ConfigureAwait(false); }
                    catch { /* ignore */ }
                    _connection = null;
                }

                Exception? last = null;
                for (var attempt = 1; attempt <= MaxInitialRetries; attempt++)
                {
                    cancellationToken.ThrowIfCancellationRequested();
                    try
                    {
                        var factory = RabbitConnectionFactoryBuilder.Create(_cfg, _contentRoot);
                        _connection = await factory.CreateConnectionAsync(cancellationToken).ConfigureAwait(false);
                        _logger.LogInformation(
                            "RabbitMQ connected: {Host}:{Port} vhost={VHost} ssl={Ssl} prefix={Prefix}",
                            _cfg.host,
                            _cfg.GetRabbitMqPort(),
                            _cfg.GetVirtualHost(),
                            _cfg.UseTls,
                            Prefix);
                        return _connection;
                    }
                    catch (Exception ex) when (attempt < MaxInitialRetries && !cancellationToken.IsCancellationRequested)
                    {
                        last = ex;
                        _logger.LogWarning(ex,
                            "RabbitMQ connect attempt {Attempt}/{Max} failed; retrying…",
                            attempt,
                            MaxInitialRetries);
                        await Task.Delay(TimeSpan.FromMilliseconds(250 * attempt), cancellationToken).ConfigureAwait(false);
                    }
                    catch (Exception ex)
                    {
                        last = ex;
                        break;
                    }
                }

                var fail = last ?? new InvalidOperationException("RabbitMQ connection failed.");
                await DiagnosticReporter.ReportAsync(
                    fail,
                    new DiagnosticEnvelope
                    {
                        Category = DiagnosticCategories.RabbitConnect,
                        Operation = "Connect",
                        Outcome = DiagnosticOutcomes.Failed,
                        Severity = "Critical",
                        Target = $"{_cfg.host}:{_cfg.GetRabbitMqPort()}",
                        PersistToErrorDb = true,
                        Detail = new DiagnosticDetail
                        {
                            Ssl = _cfg.UseTls,
                            VirtualHost = _cfg.GetVirtualHost(),
                        },
                    },
                    _logger,
                    _diagScopeFactory).ConfigureAwait(false);
                throw fail;
            }
            finally
            {
                _gate.Release();
            }
        }

        public async Task<IChannel> CreateChannelAsync(
            CreateChannelOptions? options = null,
            CancellationToken cancellationToken = default)
        {
            var connection = await GetConnectionAsync(cancellationToken).ConfigureAwait(false);
            return options is null
                ? await connection.CreateChannelAsync(cancellationToken: cancellationToken).ConfigureAwait(false)
                : await connection.CreateChannelAsync(options, cancellationToken).ConfigureAwait(false);
        }

        public async ValueTask DisposeAsync()
        {
            if (_disposed)
                return;

            await _gate.WaitAsync().ConfigureAwait(false);
            try
            {
                if (_disposed)
                    return;
                _disposed = true;

                if (_connection is not null)
                {
                    try
                    {
                        if (_connection.IsOpen)
                            await _connection.CloseAsync().ConfigureAwait(false);
                    }
                    catch { /* ignore */ }

                    try { await _connection.DisposeAsync().ConfigureAwait(false); }
                    catch { /* ignore */ }
                    _connection = null;
                }
            }
            finally
            {
                _gate.Release();
                _gate.Dispose();
            }
        }
    }
}
