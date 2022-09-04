using Confluent.Kafka;
using System.IO;
using System.IO.Compression;
using System.Text.Json;

namespace SampleKafka.ToHostedService.Serializador
{
    internal class CustomKafkaSerializer<T> : ISerializer<T>
    {
        public byte[] Serialize(T data, SerializationContext context)
        {
            var bytes = JsonSerializer.SerializeToUtf8Bytes(data);

            return CompressData(bytes);
        }

        private byte[] CompressData(byte[] data)
        {
            using var memoryStream = new MemoryStream();
            using var zipStream = new GZipStream(memoryStream, CompressionMode.Compress, true);

            zipStream.Write(data, 0, data.Length);
            zipStream.Close();

            return memoryStream.ToArray();
        }
    }
}