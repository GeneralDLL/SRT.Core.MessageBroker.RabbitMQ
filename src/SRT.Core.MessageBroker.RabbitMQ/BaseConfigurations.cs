// Ignore Spelling: SRT

using SRT.Core;
using SRT.Core.MessageBroker.RabbitMQ.Consumer;
using SRT.Core.MessageBroker.RabbitMQ.Publisher;
using SRT.Core.Domain;
using Microsoft.AspNetCore.Builder;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace SRT.Core.MessageBroker.RabbitMQ
{
    public static class BaseConfigurations
    {
        #region Basic Configure Builder
        public static async Task<WebApplicationBuilder> SRT_RabbitMQConfig(this WebApplicationBuilder builder, AppSetting config)
        {
            var rabbitCfg = config.ConnectionStrings.SRTCore_RabbitMQ;
            var contentRoot = builder.Environment.ContentRootPath;

            // Queue and exchange names start with SRTCore_RabbitMQ.pattern.
            // The old RabbitMQ:Prefix section is no longer read.
            var rabbitOptions = new RabbitMqOptions { Prefix = rabbitCfg.pattern };
            builder.Services.AddSingleton(rabbitOptions);

            builder.Services.AddSingleton<IRabbitConnection>(sp =>
            {
                var logger = sp.GetRequiredService<ILoggerFactory>().CreateLogger("RabbitConnect");
                return new RabbitConnection(
                    rabbitCfg,
                    contentRoot,
                    logger,
                    sp.GetService<IServiceScopeFactory>(),
                    sp.GetRequiredService<RabbitMqOptions>());
            });

            var consumers = 0;
            var publishers = 0;
            foreach (var configItem in config.lstRabbitMQNamespace)
            {
                var lstType = RegisterServicesByType.SRT_GetTypes<IConsumerMessage>(configItem);
                consumers += lstType.Count;
                RabbitMQConsumerConfig(builder, lstType, builder.SRT_GetLogger());

                lstType = RegisterServicesByType.SRT_GetTypes<IPublisherMessage>(configItem);
                publishers += lstType.Count;
                RabbitMQPublisherConfig(builder, lstType, builder.SRT_GetLogger());
            }

            config.StartupReport.Info(
                "rabbitmq",
                $"{rabbitCfg.host}:{rabbitCfg.GetRabbitMqPort()} vhost={rabbitCfg.GetVirtualHost()} user={rabbitCfg.user} " +
                $"pattern={rabbitCfg.pattern} tls={rabbitCfg.UseTls} consumers={consumers} publishers={publishers}");

            await Task.Delay(1);
            return builder;
        }
        #endregion

        private static WebApplicationBuilder RabbitMQPublisherConfig(WebApplicationBuilder builder, List<Type> lstType, ILogger logger)
        {
            foreach (var rabbitMQType in lstType)
            {
                builder.Services.AddScoped(rabbitMQType);
            }

            return builder;
        }

        private static WebApplicationBuilder RabbitMQConsumerConfig(WebApplicationBuilder builder, List<Type> lstType, ILogger logger)
        {
            var registeredTypes = new HashSet<Type>();

            foreach (var rabbitMQType in lstType)
            {
                if (!registeredTypes.Add(rabbitMQType))
                {
                    logger.LogWarning("Service {Type} is already registered.", rabbitMQType.FullName);
                    continue;
                }

                builder.Services.AddSingleton<IHostedService>(provider =>
                {
                    var loggerInstance = (ILogger)provider.GetRequiredService(
                        typeof(ILogger<>).MakeGenericType(rabbitMQType));
                    var appConnectionString = provider.GetRequiredService<AppConnectionString>();
                    var rabbitConnection = provider.GetRequiredService<IRabbitConnection>();
                    var scopeFactory = provider.GetRequiredService<IServiceScopeFactory>();

                    try
                    {
                        return (IHostedService)Activator.CreateInstance(
                            rabbitMQType,
                            appConnectionString,
                            loggerInstance,
                            rabbitConnection,
                            scopeFactory)!;
                    }
                    catch (MissingMethodException)
                    {
                        return (IHostedService)Activator.CreateInstance(
                            rabbitMQType,
                            appConnectionString,
                            loggerInstance,
                            rabbitConnection)!;
                    }
                });
            }

            return builder;
        }
    }
}
