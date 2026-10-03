namespace SRT.Core.MessageBroker.RabbitMQ
{
    /// <summary>
    /// Bound from appsettings section <c>RabbitMQ</c> (same idea as Redis <c>RedisCache:Prefix</c>).
    /// Empty Prefix → names are used as declared.
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
            prefix = prefix.Trim();
            if (prefix.EndsWith(':') || prefix.EndsWith('_') || prefix.EndsWith('-') || prefix.EndsWith('.'))
                return prefix;
            return prefix + ":";
        }
    }
}
