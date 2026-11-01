namespace SRT.Core.MessageBroker.RabbitMQ
{
    /// <summary>
    /// <see cref="Prefix"/> is set from <c>SRTCore_RabbitMQ.pattern</c> in appsettings
    /// (same rule as Redis <c>SRTCore_CacheRedis.pattern</c>). Empty → names are used as declared.
    /// </summary>
    public sealed class RabbitMqOptions
    {
        public string Prefix { get; set; } = "";

        public string NormalizedPrefix => NormalizePrefix(Prefix);

        public string ApplyPrefix(string name)
        {
            if (string.IsNullOrWhiteSpace(name))
                return name ?? "";

            var prefix = NormalizedPrefix;
            if (string.IsNullOrEmpty(prefix))
                return name;
            if (name.StartsWith(prefix, StringComparison.Ordinal))
                return name;
            return prefix + name;
        }

        public static string NormalizePrefix(string? prefix)
        {
            if (string.IsNullOrWhiteSpace(prefix))
                return "";
            return SRT.Core.Domain.GeneralConnectionString.NormalizePattern(prefix);
        }
    }
}
