using SimpleKafka.Protocol;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace SimpleKafka
{
    public class KafkaProducer<TKey,TValue>
    {
        private class MessageAndPartition
        {
            readonly KafkaMessage<TKey, TValue> message;
            readonly int partition;

            public MessageAndPartition(KafkaMessage<TKey,TValue> message, int partition)
            {

                this.message = message;
                this.partition = partition;
            }
        }

        private readonly KafkaBrokers brokers;
        private readonly IKafkaSerializer<TKey> keySerializer;
        private readonly IKafkaSerializer<TValue> valueSerializer;
        private readonly IKafkaMessagePartitioner<TKey, TValue> messagePartitioner;
        private readonly int acks = 1;
        private readonly int timeoutMs = 10000;
        private readonly MessageCodec codec = MessageCodec.CodecNone;

        public KafkaProducer(KafkaBrokers brokers, IKafkaSerializer<TKey> keySerializer, IKafkaSerializer<TValue> valueSerializer, 
            IKafkaMessagePartitioner<TKey,TValue> messagePartitioner)
        {
            this.brokers = brokers;
            this.keySerializer = keySerializer;
            this.valueSerializer = valueSerializer;
            this.messagePartitioner = messagePartitioner;
        }

        public async Task SendAsync(KafkaMessage<TKey,TValue> message, CancellationToken token)
        {
            await SendAsync(new KafkaMessage<TKey, TValue>[] { message }, token);
        }

        public async Task SendAsync(IEnumerable<KafkaMessage<TKey,TValue>> messages, CancellationToken token)
        {
            var topicMap = BuildTopicMap(messages);

            while (topicMap.Count > 0)
            {
                var brokerMap = await brokers.BuildBrokerMapAsync(token, topicMap).ConfigureAwait(false);

                var completed = await SendMessagesToBrokersAsync(token, topicMap, brokerMap).ConfigureAwait(false);
                if (!completed)
                {
                    var refreshed = await brokers.RefreshAsync(token).ConfigureAwait(false);
                    if (!refreshed)
                    {
                        throw new InvalidOperationException("Failed to refresh");
                    }
                }

            }
        }

        private async Task<bool> SendMessagesToBrokersAsync(CancellationToken token, Dictionary<string, Dictionary<int, List<Message>>> topicMap, Dictionary<int, Dictionary<Tuple<string, int>, List<Message>>> brokerMap)
        {
            foreach (var brokerKvp in brokerMap)
            {
                var responses = await ProduceMessagesToBroker(brokerKvp.Key, brokerKvp.Value, token).ConfigureAwait(false);
                foreach (var response in responses)
                {
                    switch ((ErrorResponseCode)response.Error)
                    {
                        case ErrorResponseCode.NoError:
                            var partitions = topicMap[response.Topic];
                            partitions.Remove(response.PartitionId);
                            if (partitions.Count == 0)
                            {
                                topicMap.Remove(response.Topic);
                            }
                            break;


                        case ErrorResponseCode.LeaderNotAvailable:
                        case ErrorResponseCode.NotLeaderForPartition:
                            break;

                        default:
                            throw new InvalidOperationException("Unhandled error " + (ErrorResponseCode)response.Error + ", " + response.Topic + ":" + response.PartitionId);
                    }
                }
            }

            return brokerMap.Count > 0;
        }


        private async Task<IEnumerable<ProduceResponse>> ProduceMessagesToBroker(int brokerId, Dictionary<Tuple<string,int>,List<Message>> topicMessages, CancellationToken token)
        {
            var payload = new List<Payload>(topicMessages.Count);
            foreach (var kvp in topicMessages)
            {
                payload.Add(new Payload
                {
                    Topic = kvp.Key.Item1,
                    Partition = kvp.Key.Item2,
                    Codec = codec,
                    Messages = kvp.Value
                });
            }
            var request = new ProduceRequest
            {
                Acks = (short)acks,
                TimeoutMS = timeoutMs,
                Payload = payload,
            };
            var response = await brokers[brokerId].SendRequestAsync(request, token).ConfigureAwait(false);
            return response;
        }

        private Dictionary<string, Dictionary<int, List<Message>>> BuildTopicMap(IEnumerable<KafkaMessage<TKey, TValue>> messages)
        {
            var topicMap = new Dictionary<string, Dictionary<int, List<Message>>>();
            foreach (var message in messages)
            {
                var partitionMap = topicMap.FindOrCreate(message.Topic);
                var partition = messagePartitioner.CalculatePartition(message);
                var messageList = partitionMap.FindOrCreate(partition);
                var encodedMessage = new Message
                {
                    Key = keySerializer.Serialize(message.Key),
                    Value = valueSerializer.Serialize(message.Value),
                };
                messageList.Add(encodedMessage);
            }
            return topicMap;
        }
    }
}
