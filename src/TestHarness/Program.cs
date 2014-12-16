using System;
using System.Threading.Tasks;
using KafkaNet;
using KafkaNet.Model;
using KafkaNet.Protocol;
using System.Collections.Generic;

namespace TestHarness
{
    class Program
    {
        static void Main(string[] args)
        {
            var options = new KafkaOptions(new Uri("http://CSDKAFKA01:9092"), new Uri("http://CSDKAFKA02:9092"))
                {
                    Log = new ConsoleLog()
                };
            
            var producer = new Producer(new BrokerRouter(options));

            Task.Factory.StartNew(() =>
                {
                    var consumer = new Consumer(new ConsumerOptions("TestHarness", new BrokerRouter(options)));
                    foreach (var data in consumer.Consume())
                    {
                        Console.WriteLine("Response: P{0},O{1} : {2}", data.Meta.PartitionId, data.Meta.Offset, data.Value);
                    }
                });


            Console.WriteLine("Type a message and press enter...");
            while (true)
            {
                var message = Console.ReadLine();
                if (message == "quit") break;
                if (string.IsNullOrEmpty(message))
                {
                    for (int i = 0; i < 20; i++)
                    {
                        producer.SendMessageAsync("TestHarness", new[] { new Message(i.ToString()) })
                            .ContinueWith(t =>
                            {
                                t.Result.ForEach(x => Console.WriteLine("Complete: {0}, Offset: {1}", x.PartitionId, x.Offset));
                            });
                    }
                }
                else
                {
                    producer.SendMessageAsync("TestHarness", new[] { new Message(message) });
                }

            }

            using (producer)
            {

            }
        }
    }
}
