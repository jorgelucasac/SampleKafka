namespace SampleKafka.ToHostedService.Bus
{
    public interface IMessageBus : IDisposable
    {
        Task ProducerAsync<T>(string topico, T message, CancellationToken cancellationToken = default) where T : class;

        Task ConsumerAsync<T>(string topico, Func<T, Task> onMessage, string? groupId = null, CancellationToken cancellationToken = default) where T : class;

        Task ConsumerAsync<T>(string topico, Func<T, Task> onMessage, CancellationToken cancellationToken = default) where T : class;
    }
}