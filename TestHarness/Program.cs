using System;
using System.Collections.Generic;
using KafkaNet;
using KafkaNet.Model;
using KafkaNet.Protocol;

namespace TestHarness
{
    class Program
    {
        static void Main(string[] args)
        {

            var client = new Producer(new KafkaOptions(new Uri("http://CSDKAFKA01:9092"), new Uri("http://CSDKAFKA02:9092")));

            var conn = new KafkaConnection(new Uri("http://CSDKAFKA01:9092"), 5000, new DefaultTraceLog());
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
                                },
                                 new Offset
                                {
                                    Topic = "TestHarness",
                                    PartitionId = 1,
                                    MaxOffsets = 1,
                                    Time = -1
                                }
                        })
            };
            var response = conn.SendAsync(request).Result;

            //var topic = client.GetTopicAsync("TestHarness").Result;


            //var response = client.SendMessageAsync("TestHarness", new[] { new Message { Value = "TestMe" } }).Result;


            //var client = new Producer(new Uri("http://CSDKAFKA01:9092"));
            //SendMetadataRequest(client);
        }

        //private static void SendOffsetCommitRequest(Producer client)
        //{
        //    var request = new OffsetCommitRequest()
        //    {
        //        CorrelationId = 1,
        //        ConsumerGroup = "TestHarnessGroup",
        //        OffsetCommits = new List<OffsetCommit>(new[]{
        //            new OffsetCommit{
        //                Topic = "TestHarness",
        //                PartitionId = 0,
        //                Offset = 6,
        //                Metadata = "Test metadata"
        //            }
        //        })
        //    };

        //    var result = client.SendAsync(request).Result;
        //}

        //private static void SendOffsetRequest(Producer client)
        //{
        //    var request = new OffsetRequest
        //    {
        //        CorrelationId = 1,
        //        Offsets = new List<Offset>(new[]
        //                {
        //                    new Offset
        //                        {
        //                            Topic = "TestHarness",
        //                            PartitionId = 0,
        //                            MaxOffsets = 1,
        //                            Time = -1
        //                        }
        //                })
        //    };

        //    var result = client.SendAsync(request).Result;
        //}

        //private static void SendFetchRequest(Producer client)
        //{
        //    var request = new FetchRequest
        //    {
        //        CorrelationId = 1,
        //        Fetches = new List<Fetch>(new[]
        //                {
        //                    new Fetch
        //                        {
        //                            Topic = "TestHarness",
        //                            PartitionId = 0,
        //                            Offset = 0
        //                        }
        //                })
        //    };

        //    var result = client.SendAsync(request).Result;
        //}

        //private static void SendMetadataRequest(Producer client)
        //{
        //    var request = new MetadataRequest()
        //        {
        //            Topics = new List<string>(new[] {"UnitTest"})
        //        };

        //    var result = client.SendAsync(request).Result;
        //}

        //private static void SendMessageTest(Producer client)
        //{
        //    var request = new ProduceRequest
        //        {
        //            ClientId = "kafka-python",
        //            CorrelationId = 1,
        //            Payload = new List<Payload>(new[]
        //                {
        //                    new Payload
        //                        {
        //                            Topic = "TestHarness",
        //                            Messages = new List<Message>(new[] {new Message {Value = "Test Message"}})
        //                        }
        //                })
        //        };

        //    var result = client.SendAsync(request).Result;
        //}
    }
}
