using System;
using System.Collections.Generic;
using KafkaNet;
using KafkaNet.Model;

namespace TestHarness
{
    class Program
    {
        static void Main(string[] args)
        {
            
            var client = new KafkaClient(new Uri("http://CSDKAFKA01:9092"));
            SendMessageTest(client);
        }

        private static void SendOffsetCommitRequest(KafkaClient client)
        {
            var request = new OffsetCommitRequest()
            {
                CorrelationId = 1,
                ConsumerGroup = "TestHarnessGroup",
                OffsetCommits = new List<OffsetCommit>(new[]{
                    new OffsetCommit{
                        Topic = "TestHarness",
                        PartitionId = 0,
                        Offset = 6,
                        Metadata = "Test metadata"
                    }
                })
            };

            var result = client.SendAsync(request).Result;
        }

        private static void SendOffsetRequest(KafkaClient client)
        {
            var request = new OffsetRequest
            {
                CorrelationId = 1,
                Offsets = new List<Offset>(new[]
                        {
                            new Offset
                                {
                                    Topic = "TestHarness",
                                    PartitionId = 0,
                                    MaxOffsets = 1,
                                    Time = -1
                                }
                        })
            };

            var result = client.SendAsync(request).Result;
        }

        private static void SendFetchRequest(KafkaClient client)
        {
            var request = new FetchRequest
            {
                CorrelationId = 1,
                Fetches = new List<Fetch>(new[]
                        {
                            new Fetch
                                {
                                    Topic = "TestHarness",
                                    PartitionId = 0,
                                    Offset = 0
                                }
                        })
            };

            var result = client.SendAsync(request).Result;
        }

        private static void SendMetadataRequest(KafkaClient client)
        {
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

            var result = client.SendAsync(request).Result;
        }
    }
}
