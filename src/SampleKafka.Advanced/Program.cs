using SampleKafka.Advanced;

const string Topico = "curso-io";

var i = 1;
for (i = 1; i <= 5; i++)
{
    //await MessageTools.ProduzirAcksLeader(Topico);
}

_ = Task.Run(() => MessageTools.ConsumirAutoOffsetResetLatest(Topico, "consumidor1"));

_ = Task.Run(() => MessageTools.ConsumirAutoOffsetResetEarliest(Topico, "consumidor2"));

while (true)
{
    Console.ReadLine();
    MessageTools.ProduzirAcksLeader(Topico).GetAwaiter().GetResult();
    i++;
}