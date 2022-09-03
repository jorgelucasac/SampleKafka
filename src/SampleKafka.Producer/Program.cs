using Confluent.Kafka;
using Confluent.SchemaRegistry;
using Confluent.SchemaRegistry.Serdes;
using Schemas.Avros;

const string Topico = "cursos";

var schemaConfig = new SchemaRegistryConfig
{
    Url = "localhost:8081"
};

var schemaRegistry = new CachedSchemaRegistryClient(schemaConfig);

var config = new ProducerConfig
{
    BootstrapServers = "localhost:9092"
};

using var producer = new ProducerBuilder<string, Curso>(config)
    .SetValueSerializer(new AvroSerializer<Curso>(schemaRegistry))
    .Build();

var msg = new Message<string, Curso>
{
    Key = Guid.NewGuid().ToString(),
    Value = new Curso
    {
        Id = Guid.NewGuid().ToString(),
        Descricao = $"Curso de apache kafka {DateTime.Now}"
    }
};

var result = await producer.ProduceAsync(Topico, msg);

Console.WriteLine($"Offset: {result.Offset} \n Key:{result.Key} \n Partition:{result.Partition}");