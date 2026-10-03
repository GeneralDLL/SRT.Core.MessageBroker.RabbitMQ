using SRT.Core.MessageBroker.RabbitMQ;
using Xunit;

namespace SRT.Core.MessageBroker.RabbitMQ.Tests;

public class RabbitMqPrefixTests
{
    [Fact]
    public void Empty_prefix_leaves_name_unchanged()
    {
        var opts = new RabbitMqOptions { Prefix = "" };
        Assert.Equal("", opts.NormalizedPrefix);
        Assert.Equal("srt.test.rabbit.demo", opts.ApplyPrefix("srt.test.rabbit.demo"));
    }

    [Fact]
    public void Prefix_without_separator_gets_colon()
    {
        var opts = new RabbitMqOptions { Prefix = "srt:test" };
        Assert.Equal("srt:test:", opts.NormalizedPrefix);
        Assert.Equal("srt:test:srt.test.rabbit.demo", opts.ApplyPrefix("srt.test.rabbit.demo"));
    }

    [Fact]
    public void Prefix_with_dot_or_colon_is_kept()
    {
        Assert.Equal("srt.test.", new RabbitMqOptions { Prefix = "srt.test." }.NormalizedPrefix);
        Assert.Equal("srt:test:", new RabbitMqOptions { Prefix = "srt:test:" }.NormalizedPrefix);
    }

    [Fact]
    public void ApplyPrefix_does_not_double_prefix()
    {
        var opts = new RabbitMqOptions { Prefix = "srt:test:" };
        Assert.Equal(
            "srt:test:srt.test.rabbit.demo",
            opts.ApplyPrefix(opts.ApplyPrefix("srt.test.rabbit.demo")));
    }

    [Fact]
    public void IRabbitConnection_applies_options()
    {
        var conn = new RabbitConnection(
            new SRT.Core.Domain.GeneralConnectionString { host = "localhost" },
            contentRoot: null,
            logger: Microsoft.Extensions.Logging.Abstractions.NullLogger.Instance,
            options: new RabbitMqOptions { Prefix = "srt:test:" });

        Assert.Equal("srt:test:", conn.Prefix);
        Assert.Equal("srt:test:demo", conn.ApplyNamePrefix("demo"));
    }
}
