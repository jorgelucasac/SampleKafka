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

        #region Indepotencia

        /// <summary>
        /// produz mensagens de forma indepotente, impedindo mensagens duplicadas
        /// </summary>
        /// <param name="topico"></param>
        /// <returns></returns>
        public static async Task ProduzirIndepotente(string topico)
        {
            _count++;
            var configuracao = new ProducerConfig
            {
                BootstrapServers = _bootstrapServers,
                Partitioner = Partitioner.ConsistentRandom,

                // propriedades para Habilitar idempotência
                EnableIdempotence = true,
                Acks = Acks.All,//todos os brokers devem receber uma cópia da msg
                MaxInFlight = 1, // quantidade de conexões na sessão atual, (uma unica conexão publicando msg, mantendo a ordem de envio das msg)
                MessageSendMaxRetries = 2, // maximo de retentativas de envio
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

        #endregion Indepotencia
    }
}