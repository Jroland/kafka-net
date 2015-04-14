using SimpleKafka.Protocol;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace SimpleKafka
{
    public static class KafkaProducer {
        public static KafkaProducer<object,object,TValue> Create<TValue>(
            KafkaBrokers brokers, 
            IKafkaSerializer<TValue> valueSerializer) {
            return new KafkaProducer<object,object,TValue>(brokers, 
                new NullSerializer<object>(), 
                valueSerializer, 
                new LoadBalancedPartitioner<object>());
        }

        public static KafkaProducer<TKey,TKey,TValue> Create<TKey,TValue>(
            KafkaBrokers brokers, 
            IKafkaSerializer<TKey> keySerializer,
            IKafkaSerializer<TValue> valueSerializer
            ) {
            return new KafkaProducer<TKey,TKey,TValue>(
                brokers,
                keySerializer,
                valueSerializer,
                new LoadBalancedPartitioner<TKey>());
        }

        public static KafkaProducer<TKey,TPartitionKey,TValue> Create<TKey,TPartitionKey,TValue>(
            KafkaBrokers brokers,
            IKafkaSerializer<TKey> keySerializer,
            IKafkaSerializer<TValue> valueSerializer,
            IKafkaMessagePartitioner<TPartitionKey> partitioner)
        {
            return new KafkaProducer<TKey,TPartitionKey,TValue>(
                brokers,
                keySerializer,
                valueSerializer,
                partitioner);
        }
    }

    public class KafkaProducer<TKey, TPartitionKey, TValue>
    {


        private readonly KafkaBrokers brokers;
        private readonly IKafkaSerializer<TKey> keySerializer;
        private readonly IKafkaSerializer<TValue> valueSerializer;
        private readonly IKafkaMessagePartitioner<TPartitionKey> messagePartitioner;
        private readonly int acks = 1;
        private readonly int timeoutMs = 10000;
        private readonly MessageCodec codec = MessageCodec.CodecNone;

        public KafkaProducer(KafkaBrokers brokers, 
            IKafkaSerializer<TKey> keySerializer, 
            IKafkaSerializer<TValue> valueSerializer,
            IKafkaMessagePartitioner<TPartitionKey> messagePartitioner)
        {
            this.brokers = brokers;
            this.keySerializer = keySerializer;
            this.valueSerializer = valueSerializer;
            this.messagePartitioner = messagePartitioner;
        }

        public async Task SendAsync(KeyedMessage<TKey, TPartitionKey, TValue> message, CancellationToken token)
        {
            await SendAsync(new[] { message}, token).ConfigureAwait(false);
        }

        public async Task SendAsync(IEnumerable<KeyedMessage<TKey, TPartitionKey, TValue>> messages, CancellationToken token)
        {
            var topicMap = BuildTopicMap(messages);
            while (topicMap != null)
            {
                var partitionsMap = await brokers.GetValidPartitionsForTopicsAsync(topicMap.Keys, token).ConfigureAwait(false);
                var brokerMap = BuildBrokerMap(topicMap, partitionsMap);
                var results = await SendMessagesAsync(brokerMap, token).ConfigureAwait(false);

                topicMap = ProcessResults(topicMap, brokerMap, results);
                if (topicMap != null)
                {
                    await brokers.BackoffAndRefresh(token).ConfigureAwait(false);
                }
            }
        }

        private Dictionary<int, Dictionary<Tuple<string, int>, List<KeyedMessage<object, TPartitionKey, Message>>>> BuildBrokerMap(Dictionary<string, List<KeyedMessage<object, TPartitionKey, Message>>> topicMap, Dictionary<string, Partition[]> partitionsMap)
        {
            var brokerMap = new Dictionary<int, Dictionary<Tuple<string, int>, List<KeyedMessage<object, TPartitionKey, Message>>>>();
            foreach (var messageSet in topicMap.Values)
            {
                foreach (var message in messageSet)
                {
                    var partitions = partitionsMap[message.Topic];
                    var partitionId = messagePartitioner.CalculatePartition(message.PartitionKey, partitions.Length);
                    var partition = partitions[partitionId];

                    brokerMap.GetOrCreate(partition.LeaderId)
                        .GetOrCreate(Tuple.Create(message.Topic, partitionId))
                        .Add(message);
                }
            }

            return brokerMap;
        }

        private async Task<Dictionary<int, List<ProduceResponse>>> SendMessagesAsync(Dictionary<int, Dictionary<Tuple<string, int>, List<KeyedMessage<object, TPartitionKey, Message>>>> brokerMap, CancellationToken token)
        {
            var tasks = new List<Task>(brokerMap.Count);
            var results = new Dictionary<int, List<ProduceResponse>>();
            foreach (var brokerKvp in brokerMap)
            {
                var request = new ProduceRequest
                {
                    Acks = (short)acks,
                    TimeoutMS = timeoutMs,
                    Payload = brokerKvp.Value.Select(kvp => new Payload
                    {
                        Codec = codec,
                        Messages = kvp.Value.Select(m => m.Value).ToList(),
                        Partition = kvp.Key.Item2,
                        Topic = kvp.Key.Item1
                    }).ToList()
                };
                var brokerId = brokerKvp.Key;
                tasks.Add(
                    brokers[brokerId]
                        .SendRequestAsync(request, token)
                        .ContinueWith(task => results.Add(brokerId, task.Result), token)
                    );
            }
            await Task.WhenAll(tasks).ConfigureAwait(false);
            return results;
        }

        private static Dictionary<string, List<KeyedMessage<object, TPartitionKey, Message>>> ProcessResults(Dictionary<string, List<KeyedMessage<object, TPartitionKey, Message>>> topicMap, Dictionary<int, Dictionary<Tuple<string, int>, List<KeyedMessage<object, TPartitionKey, Message>>>> brokerMap, Dictionary<int, List<ProduceResponse>> results)
        {
            Dictionary<string, List<KeyedMessage<object, TPartitionKey, Message>>> remainingTopicMap = null;

            foreach (var kvp in results)
            {
                var brokerId = kvp.Key;
                var brokerMessages = brokerMap[brokerId];

                foreach (var response in kvp.Value)
                {
                    if (response.Error == ErrorResponseCode.NoError)
                    {
                        // nothing to do - success!!
                    }
                    else
                    {
                        if (remainingTopicMap == null)
                        {
                            remainingTopicMap = new Dictionary<string, List<KeyedMessage<object, TPartitionKey, Message>>>();
                        }
                        remainingTopicMap
                            .GetOrCreate(response.Topic)
                            .AddRange(brokerMessages[Tuple.Create(response.Topic, response.PartitionId)]);
                    }
                }
            }

            topicMap = remainingTopicMap;
            return topicMap;
        }

        private Dictionary<string, List<KeyedMessage<object,TPartitionKey,Message>>> BuildTopicMap(IEnumerable<KeyedMessage<TKey,TPartitionKey,TValue>> messages)
        {
            var result = new Dictionary<string, List<KeyedMessage<object, TPartitionKey, Message>>>();

            foreach (var message in messages)
            {
                var encoded = new Message
                {
                    Key = keySerializer.Serialize(message.Key),
                    Value = valueSerializer.Serialize(message.Value)
                };
                var prepared = KeyedMessage.Create(message.Topic, (object)null, message.PartitionKey, encoded);
                result.GetOrCreate(message.Topic).Add(prepared);
            }
            return result;
        }
    }
}
