using SRT.Core.Domain;
using RabbitMQ.Client;
using System.Net.Security;
using System.Security.Authentication;
using System.Security.Cryptography.X509Certificates;

namespace SRT.Core.MessageBroker.RabbitMQ
{
    /// <summary>
    /// Builds RabbitMQ <see cref="ConnectionFactory"/> from appsettings (TLS + vhost + CA).
    /// TLS when <see cref="GeneralConnectionString.UseTls"/>; non-empty <c>caPath</c> that is
    /// missing/unreadable/invalid PEM throws — no plaintext fallback.
    /// </summary>
    public static class RabbitConnectionFactoryBuilder
    {
        public static ConnectionFactory Create(GeneralConnectionString cfg, string? contentRoot = null)
        {
            var factory = new ConnectionFactory
            {
                HostName = cfg.host,
                Port = int.Parse(cfg.GetRabbitMqPort()),
                UserName = cfg.user,
                Password = cfg.pass,
                VirtualHost = cfg.GetVirtualHost(),
                AutomaticRecoveryEnabled = true,
                TopologyRecoveryEnabled = true,
            };

            if (!cfg.UseTls)
                return factory;

            factory.Ssl.Enabled = true;
            factory.Ssl.ServerName = cfg.host;
            factory.Ssl.Version = SslProtocols.Tls12 | SslProtocols.Tls13;

            if (!string.IsNullOrWhiteSpace(cfg.caPath))
            {
                var caCert = LoadCaCertificate(cfg.caPath, contentRoot);
                factory.Ssl.CertificateValidationCallback = (_, certificate, _, errors) =>
                    ValidateWithCa(certificate, errors, caCert);
            }

            return factory;
        }

        /// <summary>
        /// Resolves <paramref name="caPath"/> (absolute or relative to content root / base directory)
        /// and loads a CA-only PEM. Throws if missing, unreadable, or invalid.
        /// </summary>
        public static X509Certificate2 LoadCaCertificate(string caPath, string? contentRoot = null)
        {
            var resolved = ResolveCaPath(caPath, contentRoot);
            if (!File.Exists(resolved))
            {
                throw new InvalidOperationException(
                    $"RabbitMQ caPath is set but the CA file was not found: '{resolved}' (configured: '{caPath}').");
            }

            string pem;
            try
            {
                pem = File.ReadAllText(resolved);
            }
            catch (Exception ex)
            {
                throw new InvalidOperationException(
                    $"RabbitMQ caPath is set but the CA file could not be read: '{resolved}'.", ex);
            }

            try
            {
                // CA-only PEM — CreateFromPemFile treats the same file as a key and fails.
                return X509Certificate2.CreateFromPem(pem);
            }
            catch (Exception ex)
            {
                throw new InvalidOperationException(
                    $"RabbitMQ caPath points to an invalid PEM certificate: '{resolved}'.", ex);
            }
        }

        public static string ResolveCaPath(string caPath, string? contentRoot = null)
        {
            if (Path.IsPathRooted(caPath))
                return Path.GetFullPath(caPath);

            var baseDir = !string.IsNullOrWhiteSpace(contentRoot)
                ? contentRoot
                : AppContext.BaseDirectory;

            return Path.GetFullPath(Path.Combine(baseDir, caPath));
        }

        public static bool ValidateWithCa(
            X509Certificate? certificate,
            SslPolicyErrors errors,
            X509Certificate2 caCert)
        {
            if (certificate is null)
                return false;

            if ((errors & SslPolicyErrors.RemoteCertificateNotAvailable) != 0)
                return false;
            if ((errors & SslPolicyErrors.RemoteCertificateNameMismatch) != 0)
                return false;

            var serverCert = certificate as X509Certificate2 ?? new X509Certificate2(certificate);

            if (serverCert.NotAfter.ToUniversalTime() < DateTime.UtcNow
                || serverCert.NotBefore.ToUniversalTime() > DateTime.UtcNow)
                return false;

            using var customChain = new X509Chain();
            customChain.ChainPolicy.TrustMode = X509ChainTrustMode.CustomRootTrust;
            customChain.ChainPolicy.CustomTrustStore.Add(caCert);
            customChain.ChainPolicy.RevocationMode = X509RevocationMode.NoCheck;

            if (!customChain.Build(serverCert))
                return false;

            foreach (var element in customChain.ChainElements)
            {
                if (element.Certificate.Thumbprint.Equals(caCert.Thumbprint, StringComparison.OrdinalIgnoreCase))
                    return true;
            }

            return customChain.ChainElements[^1].Certificate.Thumbprint
                .Equals(caCert.Thumbprint, StringComparison.OrdinalIgnoreCase);
        }
    }
}
