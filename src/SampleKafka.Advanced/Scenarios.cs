using System.Net;

namespace SampleKafka.Advanced
{
    internal static class Scenarios
    {
        private const string Topico = "curso-io";

        /// <summary>
        /// cenário de ConsumirAutoOffset
        /// </summary>
        /// <returns></returns>
        public static async Task AutoOffsetReset()
        {
            for (var i = 1; i <= 10; i++)
            {
                await ProducerTools.ProduzirAcksNone(Topico);
            }

            _ = Task.Run(() => ConsumerTools.ConsumirAutoOffsetResetLatest(Topico, "consumidor1"));

            _ = Task.Run(() => ConsumerTools.ConsumirAutoOffsetResetEarliest(Topico, "consumidor2"));

            await SendMessageWithEnter();
        }

        /// <summary>
        /// cenário de varios consumers no mesmo grupo conectato a um tópico
        /// </summary>
        /// <returns></returns>
        public static async Task CosumerPartitions()
        {
            var tasks = new List<Task>
            {
             Task.Run(() => ConsumerTools.ConsumirAutoOffsetResetLatest(Topico, "consumidor1")),
             Task.Run(() => ConsumerTools.ConsumirAutoOffsetResetLatest(Topico, "consumidor1")),
             Task.Run(() => ConsumerTools.ConsumirAutoOffsetResetLatest(Topico, "consumidor1")),
             Task.Run(() => ConsumerTools.ConsumirAutoOffsetResetLatest(Topico, "consumidor1")),
             Task.Run(() => ConsumerTools.ConsumirAutoOffsetResetLatest(Topico, "consumidor1")),

             Task.Run(() => ConsumerTools.ConsumirAutoOffsetResetLatest(Topico, "consumidor2")),
             Task.Run(() => ConsumerTools.ConsumirAutoOffsetResetLatest(Topico, "consumidor2")),
             Task.Run(() => ConsumerTools.ConsumirAutoOffsetResetLatest(Topico, "consumidor2")),
             Task.Run(() => ConsumerTools.ConsumirAutoOffsetResetLatest(Topico, "consumidor2")),
             Task.Run(() => ConsumerTools.ConsumirAutoOffsetResetLatest(Topico, "consumidor2"))
            };

            await Task.Delay(TimeSpan.FromSeconds(8));

            Console.WriteLine("ok");

            for (var i = 1; i <= 10; i++)
            {
                await ProducerTools.ProduzirAcksNone(Topico);
            }

            while (true)
            {
                Console.ReadLine();
                await ProducerTools.ProduzirAcksNone(Topico);
            }
        }

        /// <summary>
        /// lendo uma mensagem mais de uma vez
        /// </summary>
        /// <returns></returns>
        public static async Task ReadMessageManyTimes()
        {
            _ = Task.Run(() => ConsumerTools.ConsumirEnableAutoOffsetStore(Topico, "read-message-many-times"));
            await SendMessageWithEnter();
        }

        /// <summary>
        /// lendo uma mensagem mais de uma vez
        /// </summary>
        /// <returns></returns>
        public static async Task ProducerAndConsumerWithHeader()
        {
            _ = Task.Run(() => ConsumerTools.ConsumirComHeaders(Topico, "producer-and-consumer-with-header"));

            var headers = new Dictionary<string, string>
            {
                {"Application","SampleKafka.Advanced"},
                {"HostName",Dns.GetHostName()},
                {"time",DateTime.Now.ToString()},
            };

            await SendMessageWithHeaders(headers);
        }

        private static async Task SendMessageWithEnter()
        {
            Console.WriteLine("Pressione ENTER para enviar uma mensagem");
            while (true)
            {
                Console.ReadLine();
                await ProducerTools.ProduzirAcksNone(Topico);
            }
        }

        private static async Task SendMessageWithHeaders(Dictionary<string, string> headers, string? transactionalId = null)
        {
            Console.WriteLine("Pressione ENTER para enviar uma mensagem");
            while (true)
            {
                Console.ReadLine();
                await ProducerTools.ProduzirComHeaders(Topico, headers, transactionalId);
            }
        }
    }
}