using Confluent.Kafka;

namespace SampleKafka.Advanced
{
    internal static class ProducerTools
    {
        private const string _bootstrapServers = "localhost:9092";
        private static int _count;

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
                Partitioner = Partitioner.ConsistentRandom
            };

            try
            {
                using var producer = new ProducerBuilder<string, string>(configuracao).Build();
                var mensagem = $"Mensagem ({_count}) - {Guid.NewGuid()}";

                var result = await producer.ProduceAsync(topico, new Message<string, string>
                {
                    Key = Guid.NewGuid().ToString(),
                    Value = mensagem
                }); ;

                Console.WriteLine($"<< enviada: \t {mensagem} - partição{result.Partition.Value}");

                await Task.CompletedTask;
            }
            catch (Exception ex)
            {
                Console.Error.WriteLine(ex.ToString());
            }
        }

        #endregion Acks
    }
}