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

            var rabbitOptions = new RabbitMqOptions();
            builder.Configuration.GetSection("RabbitMQ").Bind(rabbitOptions);
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

            foreach (var configItem in config.lstRabbitMQNamespace)
            {
                var lstType = RegisterServicesByType.SRT_GetTypes<IConsumerMessage>(configItem);
                RabbitMQConsumerConfig(builder, lstType, builder.SRT_GetLogger());

                lstType = RegisterServicesByType.SRT_GetTypes<IPublisherMessage>(configItem);
                RabbitMQPublisherConfig(builder, lstType, builder.SRT_GetLogger());
            }

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
