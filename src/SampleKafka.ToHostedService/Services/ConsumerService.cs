using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using SampleKafka.ToHostedService.Bus;

namespace SampleKafka.ToHostedService.Services;

public class ConsumerService : BackgroundService
{
    private readonly IMessageBus _messageBus;
    private const string _topic = "sample-kafka";
    private readonly ILogger<ConsumerService> _logger;

    public ConsumerService(IMessageBus messageBus, ILogger<ConsumerService> logger)
    {
        _messageBus = messageBus;
        _logger = logger;
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        await _messageBus.ConsumerAsync<string>(_topic, Print, stoppingToken);
        _logger.LogInformation("consumer is start.");
    }

    private Task Print(string message)
    {
        _logger.LogInformation(message);
        return Task.CompletedTask;
    }
}