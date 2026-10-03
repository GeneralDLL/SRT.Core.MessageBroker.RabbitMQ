using Microsoft.AspNetCore.Builder;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using SRT.Core.Domain;
using SRT.Core.Interfaces;

namespace SRT.Core.MessageBroker.RabbitMQ
{
    public class RabbitMqBasicConfigurations : ISRTBasicConfigurations
    {
        public int Order => 40;
        public string FeatureName => SRTFeatureNames.RabbitMQ;

        public async Task<WebApplicationBuilder> SRT_ConfigBuilder(WebApplicationBuilder builder, AppSetting configApp)
        {
            if (configApp.lstRabbitMQNamespace.Count == 0)
                return builder;

            return await builder.SRT_RabbitMQConfig(configApp);
        }

        public Task<WebApplication> SRT_ConfigApp(WebApplication app, AppSetting configApp)
        {
            if (configApp.lstRabbitMQNamespace.Count == 0)
                return Task.FromResult(app);

            app.Lifetime.ApplicationStopping.Register(() =>
            {
                // Fire-and-forget async stop — avoid sync GetResult deadlock.
                _ = StopConsumersAndConnectionAsync(app);
            });

            return Task.FromResult(app);
        }

        private static async Task StopConsumersAndConnectionAsync(WebApplication app)
        {
            using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(10));
            try
            {
                foreach (var hosted in app.Services.GetServices<IHostedService>())
                {
                    if (hosted is Consumer.IConsumerMessage)
                        await hosted.StopAsync(cts.Token).ConfigureAwait(false);
                }
            }
            catch
            {
                // best-effort shutdown
            }

            try
            {
                var conn = app.Services.GetService<IRabbitConnection>();
                if (conn is not null)
                    await conn.DisposeAsync().ConfigureAwait(false);
            }
            catch
            {
                // best-effort shutdown
            }
        }
    }
}
