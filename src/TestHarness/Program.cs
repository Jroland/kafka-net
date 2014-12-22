using System;
using System.Threading.Tasks;
using KafkaNet;
using KafkaNet.Common;
using KafkaNet.Model;
using KafkaNet.Protocol;
using System.Collections.Generic;

namespace TestHarness
{
    class Program
    {
        static void Main(string[] args)
        {
            //create an options file that sets up driver preferences
            var options = new KafkaOptions(new Uri("http://CSDKAFKA01:9092"), new Uri("http://CSDKAFKA02:9092"))
                {
                    Log = new ConsoleLog()
                };
            
            //start an out of process thread that runs a consumer that will write all received messages to the console
            Task.Factory.StartNew(() =>
                {
                    var consumer = new Consumer(new ConsumerOptions("TestHarness", new BrokerRouter(options)));
                    foreach (var data in consumer.Consume())
                    {
                        Console.WriteLine("Response: P{0},O{1} : {2}", data.Meta.PartitionId, data.Meta.Offset, data.Value.ToUtf8String());
                    }
                });

            //create a producer to send messages with
            var producer = new Producer(new BrokerRouter(options));

            Console.WriteLine("Type a message and press enter...");
            while (true)
            {
                var message = Console.ReadLine();
                if (message == "quit") break;
                if (string.IsNullOrEmpty(message))
                {
                    //special case, send multi messages quickly
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
