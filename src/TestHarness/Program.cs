using System;
using System.Threading.Tasks;
using KafkaNet.Configuration;
using KafkaNet.Model;
using KafkaNet.Protocol;

namespace TestHarness
{
    class Program
    {
        static void Main(string[] args)
        {
            var bus = BusFactory.Create(new KafkaOptions
                {
                    Hosts = new[] {new Uri("http://CSDKAFKA01:9092"), new Uri("http://CSDKAFKA02:9092")}
                }, x => { });


            Task.Factory.StartNew(() =>
                {
                    foreach (var data in bus.Consume("TestHarness"))
                    {
                        Console.WriteLine("Response: P{0},O{1} : {2}", data.Meta.PartitionId, data.Meta.Offset, data.Value);
                    }
                });


            Console.WriteLine("Type a message and press enter...");
            while (true)
            {
                var message = Console.ReadLine();
                if (message == "quit") break;
                bus.SendMessageAsync("TestHarness", new[] {new Message {Value = message}});
            }

            using (bus)
            {
            }
        }
    }
}
