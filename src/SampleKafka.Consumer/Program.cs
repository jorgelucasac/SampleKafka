using Confluent.Kafka;
using Confluent.Kafka.SyncOverAsync;
using Confluent.SchemaRegistry;
using Confluent.SchemaRegistry.Serdes;
using Schemas.Avros;

const string Topico = "cursos";

var schemaConfig = new SchemaRegistryConfig
{
    Url = "localhost:8081"
};

var schemaRegistry = new CachedSchemaRegistryClient(schemaConfig);

var config = new ConsumerConfig
{
    GroupId = "devio",
    BootstrapServers = "localhost:9092"
};

using var consumer = new ConsumerBuilder<string, Curso>(config)
    .SetValueDeserializer(new AvroDeserializer<Curso>(schemaRegistry).AsSyncOverAsync())
    .Build();

consumer.Subscribe(Topico);

while (true)
{
    var result = consumer.Consume();

    Console.WriteLine($"Key: {result.Message.Key}");
    Console.WriteLine($"Id: {result.Message.Value.Id}");
    Console.WriteLine($"Descricao: {result.Message.Value.Descricao}");
}