using Confluent.Kafka;
using System.IO.Compression;
using System.Text.Json;

namespace SampleKafka.ToHostedService.Serializador
{
    internal class CustomKafkaDeserializer<T> : IDeserializer<T>
    {
        private static readonly JsonSerializerOptions _options = new JsonSerializerOptions
        {
            PropertyNameCaseInsensitive = true,
        };

        public T Deserialize(ReadOnlySpan<byte> data, bool isNull, SerializationContext context)
        {
            using var memoryStream = new MemoryStream(data.ToArray());
            using var zipStream = new GZipStream(memoryStream, CompressionMode.Decompress, true);

            return JsonSerializer.Deserialize<T>(zipStream, _options)!;
        }
    }
}