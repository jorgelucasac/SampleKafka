using Confluent.Kafka;

namespace SampleKafka.Advanced
{
    internal static class MessageTools
    {
        private static int _count;
        private const string _bootstrapServers = "localhost:9092";

        #region Acks

        /// <summary>
        /// não aguarda o boker confirmar que recebeu a mensagem
        /// </summary>
        /// <param name="topico"></param>
        /// <returns></returns>
        internal static async Task ProduzirAcksNone(string topico)
        {
            await ProduzirAcks(topico, Acks.None);
        }

        /// <summary>
        /// aguarda o boker líder confirmar que recebeu a mensagem
        /// </summary>
        /// <param name="topico"></param>
        /// <returns></returns>
        internal static async Task ProduzirAcksLeader(string topico)
        {
            await ProduzirAcks(topico, Acks.Leader);
        }

        /// <summary>
        /// aguarda todos os boker receberem a mensagem
        /// </summary>
        /// <param name="topico"></param>
        /// <returns></returns>
        internal static async Task ProduzirAcksAll(string topico)
        {
            await ProduzirAcks(topico, Acks.All);
        }

        private static async Task ProduzirAcks(string topico, Acks acks)
        {
            _count++;
            var configuracao = new ProducerConfig
            {
                BootstrapServers = _bootstrapServers,
                Acks = acks,
            };

            try
            {
                using var producer = new ProducerBuilder<Null, string>(configuracao).Build();
                var mensagem = $"Mensagem ({_count}) - {Guid.NewGuid()}";

                var result = await producer.ProduceAsync(topico, new Message<Null, string>
                {
                    Value = mensagem
                });

                Console.WriteLine($"<< enviada: \t {mensagem}");

                await Task.CompletedTask;
            }
            catch (Exception ex)
            {
                Console.Error.WriteLine(ex.ToString());
            }
        }

        #endregion Acks

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
        /// consome todas as mensagens contidas no tópico
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
            Console.WriteLine($"grupo: {groupId} aguradando mensagens");

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
    }
}