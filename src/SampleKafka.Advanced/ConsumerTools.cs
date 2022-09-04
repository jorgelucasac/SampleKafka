using Confluent.Kafka;
using System.Text;

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
            Console.WriteLine($"grupo: {groupId} - client {clientId} aguardando mensagens");

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
        public static async void ConsumirEnableAutoOffsetStore(string topico, string groupId)
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
            Console.WriteLine($"grupo: {groupId} - client {clientId} aguardando mensagens");

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
                if (!await ProcessarMensagem(result.Message.Key) && tentativas < 3)
                {
                    //informa para o kafka que a proxima msg consumida deve ser dessa partição
                    //e desse offset especifico, ou seja, a mesma mensagem
                    consumer.Seek(result.TopicPartitionOffset);

                    continue;
                }

                if (tentativas > 1)
                {
                    // podee-se Publicar mensagem em uma fila para analise!
                    Console.WriteLine("Enviando mensagem para: DeadLetter");
                    tentativas = 0;
                }
                //move o offset para que seja possível ler as próximas mensagens
                consumer.StoreOffset(result.TopicPartitionOffset);
                consumer.Commit(result);
            }
        }

        private static async Task<bool> ProcessarMensagem(string key)
        {
            Console.WriteLine($"KEY:{key} - {DateTime.Now}");
            await Task.Delay(TimeSpan.FromSeconds(2));
            return false;
        }

        #endregion Consumindo mensagens mais de uma vez

        #region Consumindo Transações

        /// <summary>
        /// consome apenas mensagens que foram commitadas
        /// </summary>
        /// <param name="topico">nome do tópico</param>
        /// <param name="groupId">nome do grupo de consumidores</param>
        private static void ConsumirReadCommitted(string topico, string groupId)
        {
            var clientId = Guid.NewGuid().ToString().Substring(0, 5);

            var conf = new ConsumerConfig
            {
                //identificação da máquina dentro de um grupo de consumidores
                ClientId = clientId,
                GroupId = groupId,
                BootstrapServers = _bootstrapServers,
                AutoOffsetReset = AutoOffsetReset.Latest,
                EnablePartitionEof = true,
                EnableAutoCommit = false,

                // Configurar para consumir apenas mensagens confirmadas.
                IsolationLevel = IsolationLevel.ReadCommitted
            };

            using var consumer = new ConsumerBuilder<string, string>(conf).Build();

            consumer.Subscribe(topico);
            Console.WriteLine($"grupo: {groupId} - client {clientId} aguardando mensagens");

            while (true)
            {
                var result = consumer.Consume();

                if (result.IsPartitionEOF)
                {
                    continue;
                }

                var messsage = "<< Recebida: \t" + result.Message.Value + $" - {groupId}-{clientId}";
                Console.WriteLine(messsage);

                consumer.Commit(result);
            }
        }

        #endregion Consumindo Transações

        #region Consumindo msg com headers

        /// <summary>
        /// consome mensagens e headers
        /// </summary>
        /// <param name="topico">nome do tópico</param>
        /// <param name="groupId">nome do grupo de consumidores</param>
        public static void ConsumirComHeaders(string topico, string groupId)
        {
            var clientId = Guid.NewGuid().ToString().Substring(0, 5);

            var conf = new ConsumerConfig
            {
                //identificação da máquina dentro de um grupo de consumidores
                ClientId = clientId,
                GroupId = groupId,
                BootstrapServers = _bootstrapServers,
                AutoOffsetReset = AutoOffsetReset.Latest,
                EnablePartitionEof = true,
                EnableAutoCommit = false,

                // Configurar para consumir apenas mensagens confirmadas.
                IsolationLevel = IsolationLevel.ReadCommitted
            };

            using var consumer = new ConsumerBuilder<string, string>(conf).Build();

            consumer.Subscribe(topico);
            Console.WriteLine($"grupo: {groupId} - client {clientId} aguardando mensagens");

            while (true)
            {
                var result = consumer.Consume();

                if (result.IsPartitionEOF)
                {
                    continue;
                }

                var headers = result
                              .Message
                              .Headers
                              .ToDictionary(p => p.Key, p => Encoding.UTF8.GetString(p.GetValueBytes()));

                var messsage = "<< Recebida: \t" + result.Message.Value + $" - {groupId}-{clientId}";
                Console.WriteLine(messsage);

                foreach (var item in headers)
                {
                    Console.WriteLine($"nome: {item.Key} - valor: {item.Value}");
                }

                consumer.Commit(result);
            }
        }

        #endregion Consumindo msg com headers
    }
}