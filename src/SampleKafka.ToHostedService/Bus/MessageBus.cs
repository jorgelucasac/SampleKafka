using Confluent.Kafka;
using SampleKafka.ToHostedService.Serializador;
using System.Net;

namespace SampleKafka.ToHostedService.Bus
{
    public class MessageBus : IMessageBus
    {
        private readonly string _bootstrapServer;

        public MessageBus(string bootstrapServer)
        {
            _bootstrapServer = bootstrapServer;
        }

        public async Task ProducerAsync<T>(string topico, T message, CancellationToken cancellationToken = default) where T : class
        {
            var config = new ProducerConfig
            {
                BootstrapServers = _bootstrapServer
            };

            // var payload = JsonSerializer.Serialize(message);
            var producer = new ProducerBuilder<string, T>(config)
                .SetValueSerializer(new CustomKafkaSerializer<T>())
                .Build();

            var msg = new Message<string, T>
            {
                Key = Guid.NewGuid().ToString(),
                //Value = payload
                Value = message
            };

            await producer.ProduceAsync(topico, msg, cancellationToken);

            producer.Flush(cancellationToken);

            await Task.CompletedTask;
        }

        public async Task ConsumerAsync<T>(string topico, Func<T, Task> onMessage, CancellationToken cancellationToken = default) where T : class
        {
            await ConsumerAsync(topico, onMessage, null, cancellationToken);
        }

        public async Task ConsumerAsync<T>(string topico, Func<T, Task> onMessage, string? groupId = null, CancellationToken cancellationToken = default) where T : class
        {
            _ = Task.Factory.StartNew(async () =>
            {
                var config = new ConsumerConfig
                {
                    BootstrapServers = _bootstrapServer,
                    GroupId = groupId ?? Dns.GetHostName(),
                    EnableAutoCommit = false,
                    EnablePartitionEof = true
                };

                using var consumer = new ConsumerBuilder<string, T>(config)
                .SetValueDeserializer(new CustomKafkaDeserializer<T>())
                .Build();
                consumer.Subscribe(topico);

                while (!cancellationToken.IsCancellationRequested)
                {
                    var result = consumer.Consume();

                    //se chegar no final da partição, voltar a consumir as mensagens
                    if (result.IsPartitionEOF)
                    {
                        continue;
                    }

                    //var message = JsonSerializer.Deserialize<T>(result.Message.Value);
                    var message = result.Message.Value;

                    await onMessage(message);

                    //informa que leu a mensagem com sucesso
                    consumer.Commit();
                }
            }, cancellationToken, TaskCreationOptions.LongRunning, TaskScheduler.Default);// thread de longa duração e execua em um thread separada da atual

            await Task.CompletedTask;
        }

        public void Dispose()
        {
            GC.SuppressFinalize(this);
        }
    }
}