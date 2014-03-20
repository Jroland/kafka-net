using System.Collections.Generic;
using KafkaNet.Protocol;

namespace kafka_tests
{
    public static class RequestFactory
    {
        public static ProduceRequest CreateProduceRequest(string topic, string message, string key = null)
        {
            return new ProduceRequest
                {
                    Payload = new List<Payload>(new[]
                        {
                            new Payload
                                {
                                    Topic = topic,
                                    Messages = new List<Message>(new[] {new Message {Value = message}})
                                }
                        })
                };
        }

        public static FetchRequest CreateFetchRequest(string topic, int offset, int partitionId = 0)
        {
            return new FetchRequest
            {
                CorrelationId = 1,
                Fetches = new List<Fetch>(new[]
                        {
                            new Fetch
                                {
                                    Topic = topic,
                                    PartitionId = partitionId,
                                    Offset = offset
                                }
                        })
            };
        }

        public static OffsetRequest CreateOffsetRequest(string topic, int partitionId = 0, int maxOffsets = 1, int time = -1)
        {
            return new OffsetRequest
            {
                CorrelationId = 1,
                Offsets = new List<Offset>(new[]
                        {
                            new Offset
                                {
                                    Topic = topic,
                                    PartitionId = partitionId,
                                    MaxOffsets = maxOffsets,
                                    Time = time
                                }
                        })
            };
        }
    }
}
