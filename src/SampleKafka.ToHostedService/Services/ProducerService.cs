using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using SampleKafka.ToHostedService.Bus;

namespace SampleKafka.ToHostedService.Services;

public class ProducerService : IHostedService, IDisposable
{
    private readonly IMessageBus _messageBus;
    private const string _topic = "sample-kafka";
    private readonly ILogger<ProducerService> _logger;
    private Timer? _timer = null;

    public ProducerService(IMessageBus messageBus, ILogger<ProducerService> logger)
    {
        _messageBus = messageBus;
        _logger = logger;
    }

    public Task StartAsync(CancellationToken cancellationToken)
    {
        _timer = new Timer(SendMessage, null, TimeSpan.Zero, TimeSpan.FromSeconds(5));
        _logger.LogInformation("producer is start.");

        return Task.CompletedTask;
    }

    public Task StopAsync(CancellationToken cancellationToken)
    {
        _logger.LogInformation("producer is stopping.");

        _timer?.Change(Timeout.Infinite, 0);

        return Task.CompletedTask;
    }

    private async void SendMessage(object? state)
    {
        var message = $"Mensagem - {DateTime.Now}";
        _logger.LogDebug($"send message: {message}");

        await _messageBus.ProducerAsync(_topic, message);
    }

    public void Dispose()
    {
        _timer?.Dispose();
    }
}