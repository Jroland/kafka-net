using System;
using System.Threading.Tasks;
using KafkaNet;
using KafkaNet.Model;
using KafkaNet.Protocol;

namespace TestHarness
{
    class Program
    {
        static void Main(string[] args)
        {
            var options = new KafkaOptions(new Uri("http://CSDKAFKA01:9092"), new Uri("http://CSDKAFKA02:9092"));
            var router = new BrokerRouter(options);
            var client = new Producer(options);

            Task.Factory.StartNew(() =>
                {
                    var consumer = new Consumer(new ConsumerOptions {Topic = "TestHarness", Router = router});
                    foreach (var data in consumer.Consume())
                    {
                        Console.WriteLine("Response: {0}", data.Value);
                    }
                });


            Console.WriteLine("Type a message and press enter...");
            while (true)
            {
                var message = Console.ReadLine();
                if (message == "quit") break;
                client.SendMessageAsync("TestHarness", new[] {new Message {Value = message}});
            }
            
        }
    }
}
