using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using SampleKafka.ToHostedService.Extensions;
using SampleKafka.ToHostedService.Services;

CreateHostBuilder(args).Build().Run();

static IHostBuilder CreateHostBuilder(string[]? args)
{
    return Host.CreateDefaultBuilder(args)
        .ConfigureAppConfiguration((hostingContext, config) =>
        {
            config.AddJsonFile("appsettings.json", true);
            config.AddEnvironmentVariables();

            if (args is not null)
                config.AddCommandLine(args);
        })
        .ConfigureLogging((hostingContext, logging) =>
        {
            logging.AddSerilog(hostingContext.Configuration);
        })
        .ConfigureServices((hostContext, services) =>
        {
            services.AddMessageBus(hostContext.Configuration.GetConnectionString("MessageBusConnection"));
            services.AddHostedService<ConsumerService>();
            services.AddHostedService<ProducerService>();
        });
}