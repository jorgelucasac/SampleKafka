using Confluent.Kafka;
using System.Text;

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
                var mensagem = GetMessage();

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
                var mensagem = GetMessage();

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

        #region Transações

        /// <summary>
        /// produz mensangens em uma transação
        /// </summary>
        /// <param name="topico"></param>
        /// <returns></returns>
        public static async Task ProduzirComTransacao(string topico, Guid transactionalId)
        {
            _count++;
            var configuracao = new ProducerConfig
            {
                BootstrapServers = _bootstrapServers,
                Partitioner = Partitioner.ConsistentRandom,
                EnableIdempotence = true,
                Acks = Acks.All,
                MaxInFlight = 1,
                MessageSendMaxRetries = 2,

                TransactionalId = transactionalId.ToString(),//
            };

            try
            {
                using var producer = new ProducerBuilder<string, string>(configuracao).Build();
                var mensagem = GetMessage();

                //se comunica com o cordenador de transações, informando que um transação esta sendo iniciada
                producer.InitTransactions(TimeSpan.FromSeconds(5));
                producer.BeginTransaction();//inicia a transação

                var result = await producer.ProduceAsync(topico, new Message<string, string>
                {
                    Key = Guid.NewGuid().ToString(),
                    Value = mensagem
                }); ;

                // Confirma a transação
                producer.CommitTransaction();

                // Em caso de erro pode abortar a transação
                //producer.AbortTransaction();

                Console.WriteLine($"<< enviada: \t {mensagem} - partição{result.Partition.Value}");

                await Task.CompletedTask;
            }
            catch (Exception ex)
            {
                Console.Error.WriteLine(ex.ToString());
            }
        }

        #endregion Transações

        #region Headers e tracing

        /// <summary>
        /// produz mensangens com headers
        /// </summary>
        /// <param name="topico"></param>
        /// <returns></returns>
        public static async Task ProduzirComHeaders(string topico, Dictionary<string, string> headers, string? transactionalId = null)
        {
            _count++;
            var configuracao = new ProducerConfig
            {
                BootstrapServers = _bootstrapServers,
                Partitioner = Partitioner.ConsistentRandom,
                EnableIdempotence = true,
                Acks = Acks.All,
                MaxInFlight = 1,
                MessageSendMaxRetries = 2,

                TransactionalId = transactionalId ?? Guid.NewGuid().ToString(),
            };

            var producerHeaders = new Headers();
            foreach (var header in headers)
            {
                producerHeaders.Add(header.Key, Encoding.UTF8.GetBytes(header.Value));
            }
            producerHeaders.Add("transactionId", Encoding.UTF8.GetBytes(configuracao.TransactionalId));

            try
            {
                using var producer = new ProducerBuilder<string, string>(configuracao).Build();
                var mensagem = GetMessage();

                //se comunica com o cordenador de transações, informando que um transação esta sendo iniciada
                producer.InitTransactions(TimeSpan.FromSeconds(5));
                producer.BeginTransaction();//inicia a transação

                var result = await producer.ProduceAsync(topico, new Message<string, string>
                {
                    Key = Guid.NewGuid().ToString(),
                    Value = mensagem,
                    Headers = producerHeaders
                }); ;

                // Confirma a transação
                producer.CommitTransaction();

                // Em caso de erro pode abortar a transação
                //producer.AbortTransaction();

                Console.WriteLine($"<< enviada: \t {mensagem} - partição{result.Partition.Value}");

                await Task.CompletedTask;
            }
            catch (Exception ex)
            {
                Console.Error.WriteLine(ex.ToString());
            }
        }

        #endregion Headers e tracing

        private static string GetMessage()
        {
            return $"Mensagem ({_count}) - {Guid.NewGuid()}"; ;
        }
    }
}