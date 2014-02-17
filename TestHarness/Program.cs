using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Text;
using System.Threading.Tasks;
using Kafka;
using Kafka.Model;

namespace TestHarness
{
    class Program
    {
        static void Main(string[] args)
        {
            
            var client = new KafkaClient(new Uri("http://CSDKAFKA01:9092"));

            var request = new MetadataRequest
                {
                    CorrelationId = 1
                };

            var result = client.SendAsync(request).Result;

        }

        private static void SendMessageTest(KafkaClient client)
        {
            var request = new ProduceRequest
                {
                    ClientId = "kafka-python",
                    CorrelationId = 1,
                    Payload = new List<Payload>(new[]
                        {
                            new Payload
                                {
                                    Topic = "TestHarness",
                                    Messages = new List<Message>(new[] {new Message {Value = "Test Message"}})
                                }
                        })
                };

            client.Send(request);
        }
    }
}
