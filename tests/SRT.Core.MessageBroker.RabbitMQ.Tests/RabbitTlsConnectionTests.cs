using System.Net.Security;
using System.Security.Cryptography;
using System.Security.Cryptography.X509Certificates;
using RabbitMQ.Client;
using RabbitMQ.Client.Exceptions;
using SRT.Core.Domain;
using SRT.Core.MessageBroker.RabbitMQ;
using Xunit;

namespace SRT.Core.MessageBroker.RabbitMQ.Tests;

public class RabbitTlsConnectionTests
{
    [Fact]
    public void UseTls_false_when_ssl_and_caPath_empty()
    {
        var cfg = new GeneralConnectionString { sslEnabled = "false", caPath = "" };
        Assert.False(cfg.UseTls);
        Assert.False(cfg.UseRedisTls);
        Assert.Equal("5672", cfg.GetRabbitMqPort());
        Assert.StartsWith("amqp://", cfg.GetConnectionString_RabbitMQ(), StringComparison.Ordinal);
    }

    [Fact]
    public void UseTls_true_when_sslEnabled_defaults_amqps_port()
    {
        var cfg = new GeneralConnectionString
        {
            host = "localhost",
            sslEnabled = "true",
            caPath = "",
        };
        Assert.True(cfg.UseTls);
        Assert.Equal("5671", cfg.GetRabbitMqPort());
        Assert.StartsWith("amqps://", cfg.GetConnectionString_RabbitMQ(), StringComparison.Ordinal);
        Assert.Contains(":5671/", cfg.GetConnectionString_RabbitMQ());
    }

    [Fact]
    public void UseTls_true_when_caPath_only()
    {
        var cfg = new GeneralConnectionString
        {
            host = "localhost",
            sslEnabled = "false",
            caPath = "tls/ca.pem",
        };
        Assert.True(cfg.UseTls);
        Assert.Equal("5671", cfg.GetRabbitMqPort());
    }

    [Fact]
    public void Explicit_port_is_never_overridden()
    {
        var cfg = new GeneralConnectionString
        {
            host = "localhost",
            port = "15671",
            sslEnabled = "true",
        };
        Assert.Equal("15671", cfg.GetRabbitMqPort());
    }

    [Fact]
    public void LoadCaCertificate_throws_when_file_missing()
    {
        var ex = Assert.Throws<InvalidOperationException>(() =>
            RabbitConnectionFactoryBuilder.LoadCaCertificate(
                Path.Combine(Path.GetTempPath(), "srt-missing-ca-" + Guid.NewGuid() + ".pem")));

        Assert.Contains("not found", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public void LoadCaCertificate_throws_when_pem_invalid()
    {
        var path = Path.Combine(Path.GetTempPath(), "srt-bad-ca-" + Guid.NewGuid() + ".pem");
        File.WriteAllText(path, "not-a-certificate");
        try
        {
            var ex = Assert.Throws<InvalidOperationException>(() =>
                RabbitConnectionFactoryBuilder.LoadCaCertificate(path));
            Assert.Contains("invalid PEM", ex.Message, StringComparison.OrdinalIgnoreCase);
        }
        finally
        {
            File.Delete(path);
        }
    }

    [Fact]
    public void Create_with_missing_caPath_does_not_fallback_to_plaintext()
    {
        var cfg = new GeneralConnectionString
        {
            host = "localhost",
            port = "5671",
            user = "guest",
            pass = "guest",
            sslEnabled = "true",
            caPath = Path.Combine(Path.GetTempPath(), "srt-missing-" + Guid.NewGuid() + ".pem"),
        };

        Assert.Throws<InvalidOperationException>(() =>
            RabbitConnectionFactoryBuilder.Create(cfg));
    }

    [Fact]
    public void ValidateWithCa_rejects_name_mismatch()
    {
        var (ca, server) = CreateCaAndServer("localhost");
        try
        {
            var ok = RabbitConnectionFactoryBuilder.ValidateWithCa(
                server,
                SslPolicyErrors.RemoteCertificateNameMismatch,
                ca);
            Assert.False(ok);
        }
        finally
        {
            ca.Dispose();
            server.Dispose();
        }
    }

    [Fact]
    public void ValidateWithCa_rejects_not_available()
    {
        var (ca, server) = CreateCaAndServer("localhost");
        try
        {
            var ok = RabbitConnectionFactoryBuilder.ValidateWithCa(
                server,
                SslPolicyErrors.RemoteCertificateNotAvailable,
                ca);
            Assert.False(ok);
        }
        finally
        {
            ca.Dispose();
            server.Dispose();
        }
    }

    [Fact]
    public void ValidateWithCa_rejects_wrong_ca()
    {
        var (ca, server) = CreateCaAndServer("localhost");
        var wrong = CreateUnrelatedCa();
        try
        {
            var ok = RabbitConnectionFactoryBuilder.ValidateWithCa(
                server,
                SslPolicyErrors.RemoteCertificateChainErrors,
                wrong);
            Assert.False(ok);
        }
        finally
        {
            ca.Dispose();
            server.Dispose();
            wrong.Dispose();
        }
    }

    [Fact]
    public void ValidateWithCa_accepts_matching_chain()
    {
        var (ca, server) = CreateCaAndServer("localhost");
        try
        {
            var ok = RabbitConnectionFactoryBuilder.ValidateWithCa(
                server,
                SslPolicyErrors.RemoteCertificateChainErrors,
                ca);
            Assert.True(ok);
        }
        finally
        {
            ca.Dispose();
            server.Dispose();
        }
    }

    [Fact]
    public async Task Connect_tls_with_correct_ca_when_server_up()
    {
        var caPath = FindTestCaPem();
        if (caPath is null)
            return;

        var cfg = new GeneralConnectionString
        {
            host = "localhost",
            port = "5671",
            user = "guest",
            pass = "guest",
            sslEnabled = "true",
            caPath = caPath,
        };

        try
        {
            var factory = RabbitConnectionFactoryBuilder.Create(cfg);
            await using var conn = await factory.CreateConnectionAsync();
            Assert.True(conn.IsOpen);
        }
        catch (BrokerUnreachableException)
        {
            // TLS RabbitMQ not running — factory/cert logic covered by other tests.
        }
        catch (Exception ex) when (ex.GetType().Name.Contains("Timeout", StringComparison.OrdinalIgnoreCase)
                                   || ex is IOException)
        {
            // Broker down.
        }
    }

    [Fact]
    public async Task Connect_plaintext_when_server_up()
    {
        var cfg = new GeneralConnectionString
        {
            host = "localhost",
            port = "5672",
            user = "guest",
            pass = "guest",
            sslEnabled = "false",
        };

        try
        {
            var factory = RabbitConnectionFactoryBuilder.Create(cfg);
            await using var conn = await factory.CreateConnectionAsync();
            Assert.True(conn.IsOpen);
        }
        catch (BrokerUnreachableException)
        {
            // Broker not running.
        }
        catch (Exception ex) when (ex.GetType().Name.Contains("Timeout", StringComparison.OrdinalIgnoreCase)
                                   || ex is IOException)
        {
        }
    }

    private static (X509Certificate2 ca, X509Certificate2 server) CreateCaAndServer(string cn)
    {
        using var caKey = RSA.Create(2048);
        var caReq = new CertificateRequest(
            "CN=SRT Unit Test CA",
            caKey,
            HashAlgorithmName.SHA256,
            RSASignaturePadding.Pkcs1);
        caReq.CertificateExtensions.Add(new X509BasicConstraintsExtension(true, false, 0, true));
        var ca = caReq.CreateSelfSigned(DateTimeOffset.UtcNow.AddDays(-1), DateTimeOffset.UtcNow.AddYears(1));

        using var serverKey = RSA.Create(2048);
        var serverReq = new CertificateRequest(
            $"CN={cn}",
            serverKey,
            HashAlgorithmName.SHA256,
            RSASignaturePadding.Pkcs1);
        var san = new SubjectAlternativeNameBuilder();
        san.AddDnsName(cn);
        serverReq.CertificateExtensions.Add(san.Build());
        var serial = new byte[8];
        RandomNumberGenerator.Fill(serial);
        serial[0] &= 0x7F;
        var server = serverReq.Create(ca, DateTimeOffset.UtcNow.AddDays(-1), DateTimeOffset.UtcNow.AddYears(1), serial);
        // Keep private key on server for completeness (not required for ValidateWithCa).
        server = server.CopyWithPrivateKey(serverKey);
        return (ca, server);
    }

    private static X509Certificate2 CreateUnrelatedCa()
    {
        using var rsa = RSA.Create(2048);
        var req = new CertificateRequest(
            "CN=Wrong CA",
            rsa,
            HashAlgorithmName.SHA256,
            RSASignaturePadding.Pkcs1);
        return req.CreateSelfSigned(DateTimeOffset.UtcNow.AddDays(-1), DateTimeOffset.UtcNow.AddYears(1));
    }

    private static string? FindTestCaPem()
    {
        var fromRepoSibling = Path.GetFullPath(Path.Combine(
            AppContext.BaseDirectory, "..", "..", "..", "..", "..", "..",
            "SRT.Core.TestApplications", "tls", "ca.pem"));
        var env = Environment.GetEnvironmentVariable("SRT_RABBIT_TEST_CA");
        var candidates = new[] { env, fromRepoSibling }.Where(p => !string.IsNullOrWhiteSpace(p));
        return candidates.FirstOrDefault(File.Exists);
    }
}
