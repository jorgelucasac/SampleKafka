using Confluent.Kafka;

namespace SampleKafka.Advanced
{
    internal static class ConsumerTools
    {
        private const string _bootstrapServers = "localhost:9092";

        #region Auto Offset Reset

        /// <summary>
        /// consome apenas as mensagens produzidas a partir do momento da subscrição
        /// </summary>
        /// <param name="topico">nome do tópico</param>
        /// <param name="groupId">nome do grupo de consumidores</param>
        public static void ConsumirAutoOffsetResetLatest(string topico, string groupId)
        {
            ConsumirAutoOffsetReset(topico, groupId, AutoOffsetReset.Latest);
        }

        /// <summary>
        /// consome todas as mensagens contidas no tópico,
        /// desde que ainda não tenham sido consumidas pelo grupo informado
        /// </summary>
        /// <param name="topico">nome do tópico</param>
        /// <param name="groupId">nome do grupo de consumidores</param>
        public static void ConsumirAutoOffsetResetEarliest(string topico, string groupId)
        {
            ConsumirAutoOffsetReset(topico, groupId, AutoOffsetReset.Earliest);
        }

        /// <summary>
        /// possivel informa a partir de quando vai começar a ler as mensagens
        /// </summary>
        /// <param name="topico">nome do tópico</param>
        /// <param name="groupId">nome do grupo de consumidores</param>
        /// <param name="autoOffsetReset"></param>
        private static void ConsumirAutoOffsetReset(string topico, string groupId, AutoOffsetReset autoOffsetReset)
        {
            var clientId = Guid.NewGuid().ToString().Substring(0, 5);

            var conf = new ConsumerConfig
            {
                //identificação da máquina dentro de um grupo de consumidores
                ClientId = clientId,
                GroupId = groupId,
                BootstrapServers = _bootstrapServers,
                AutoOffsetReset = autoOffsetReset,
                EnablePartitionEof = true,
                EnableAutoCommit = false,
            };

            using var consumer = new ConsumerBuilder<string, string>(conf).Build();

            consumer.Subscribe(topico);
            Console.WriteLine($"grupo: {groupId} - client {clientId} aguradando mensagens");

            while (true)
            {
                var result = consumer.Consume();

                if (result.IsPartitionEOF)
                {
                    continue;
                }

                var messsage = "<< Recebida: \t" + result.Message.Value + $" - {groupId}-{autoOffsetReset}-{clientId}";
                Console.WriteLine(messsage);

                consumer.Commit(result);
            }
        }

        #endregion Auto Offset Reset

        #region Consumindo mensagens mais de uma vez

        /// <summary>
        /// possivel informa a partir de quando vai começar a ler as mensagens
        /// </summary>
        /// <param name="topico">nome do tópico</param>
        /// <param name="groupId">nome do grupo de consumidores</param>
        /// <param name="autoOffsetReset"></param>
        private static void ConsumirEnableAutoOffsetStore(string topico, string groupId)
        {
            var clientId = Guid.NewGuid().ToString().Substring(0, 5);
            int tentativas = 0;
            var conf = new ConsumerConfig
            {
                //identificação da máquina dentro de um grupo de consumidores
                ClientId = clientId,
                GroupId = groupId,
                BootstrapServers = _bootstrapServers,
                AutoOffsetReset = AutoOffsetReset.Latest,
                EnablePartitionEof = true,
                EnableAutoCommit = false,
                EnableAutoOffsetStore = false, //não desloca o off set automaticamente
            };

            using var consumer = new ConsumerBuilder<string, string>(conf).Build();

            consumer.Subscribe(topico);
            Console.WriteLine($"grupo: {groupId} - client {clientId} aguradando mensagens");

            while (true)
            {
                var result = consumer.Consume();

                if (result.IsPartitionEOF)
                {
                    continue;
                }

                var messsage = "<< Recebida: \t" + result.Message.Value + $" - {groupId}-{clientId}";
                Console.WriteLine(messsage);

                // Tentar processar mensagem
                tentativas++;
                if (!ProcessarMensagem(result.Message.Key) && tentativas < 3)
                {
                    //desloca o offset para que seja lida a proxima msg da lista
                    consumer.Seek(result.TopicPartitionOffset);

                    continue;
                }

                if (tentativas > 1)
                {
                    // podee-se Publicar mensagem em uma fila para analise!
                    Console.WriteLine("Enviando mensagem para: DeadLetter");
                    tentativas = 0;
                }

                consumer.Commit(result);
                consumer.StoreOffset(result.TopicPartitionOffset);
            }
        }

        private static bool ProcessarMensagem(string key)
        {
            Console.WriteLine($"KEY:{key} - {DateTime.Now}");
            Task.Delay(2000).Wait();
            return false;
        }

        #endregion Consumindo mensagens mais de uma vez
    }
}