using Microsoft.AspNetCore.Builder;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using SRT.Core.Domain.Diagnostics;
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

        public async Task<WebApplication> SRT_ConfigApp(WebApplication app, AppSetting configApp)
        {
            if (configApp.lstRabbitMQNamespace.Count == 0)
            {
                configApp.StartupReport.Off("rabbitmq", "no lstRabbitMQNamespace");
                return app;
            }

            await ProbeConnectionAsync(app, configApp.StartupReport).ConfigureAwait(false);

            app.Lifetime.ApplicationStopping.Register(() =>
            {
                // Fire-and-forget async stop — avoid sync GetResult deadlock.
                _ = StopConsumersAndConnectionAsync(app);
            });

            return app;
        }

        /// <summary>
        /// Opens the shared connection once so the startup summary shows a real status.
        /// A failure is reported, not thrown: consumers keep their own retry, as before.
        /// </summary>
        private static async Task ProbeConnectionAsync(WebApplication app, SRTStartupReport report)
        {
            var detail = report.Entries.FirstOrDefault(e => e.Section == "rabbitmq")?.Detail ?? "";
            try
            {
                var connection = app.Services.GetRequiredService<IRabbitConnection>();
                using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(10));
                var opened = await connection.GetConnectionAsync(cts.Token).ConfigureAwait(false);
                report.Ok("rabbitmq", $"{detail} connected={opened.IsOpen}");
            }
            catch (Exception ex)
            {
                report.Failed("rabbitmq", $"{detail} connect FAILED: {ex.Message}");
            }
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
