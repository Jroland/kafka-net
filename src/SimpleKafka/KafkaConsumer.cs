using Serilog;
using SimpleKafka.Protocol;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace SimpleKafka
{
    public class KafkaConsumer<TKey,TValue>
    {
        private class TopicTracker
        {
            public readonly OffsetSelectionStrategy failureOffsetSelection;
            public long nextOffset;

            public TopicTracker(TopicSelector selector)
            {
                switch (selector.DefaultOffsetSelection)
                {
                    case OffsetSelectionStrategy.Earliest: nextOffset = -2; break;
                    case OffsetSelectionStrategy.Latest: nextOffset = -1; break;
                    case OffsetSelectionStrategy.Specified: nextOffset = selector.Offset; break;
                    default: throw new InvalidOperationException("Unknown default offset selection: " + selector.DefaultOffsetSelection);
                }
                failureOffsetSelection = selector.FailureOffsetSelection;
            }
        }

        private readonly KafkaBrokers brokers;
        private readonly IKafkaSerializer<TKey> keySerializer;
        private readonly IKafkaSerializer<TValue> valueSerializer;
        private readonly Dictionary<string, Dictionary<int, TopicTracker>> topicMap;
        private readonly int maxWaitTimeMs = 1000;
        private readonly int minBytes = 1024;
        private readonly int maxBytes = 65536;

        public KafkaConsumer(KafkaBrokers brokers, IKafkaSerializer<TKey> keySerializer, IKafkaSerializer<TValue> valueSerializer, params TopicSelector[] topics)
        {
            this.brokers = brokers;
            this.keySerializer = keySerializer;
            this.valueSerializer = valueSerializer;
            topicMap = new Dictionary<string, Dictionary<int, TopicTracker>>();
            foreach (var topic in topics)
            {
                var partitionMap = topicMap.FindOrCreate(topic.Topic);
                if (partitionMap.ContainsKey(topic.Partition))
                {
                    throw new InvalidOperationException("Topic " + topic.Topic + ", partition " + topic.Partition + " duplicated");
                }
                partitionMap.Add(topic.Partition, new TopicTracker(topic));
            }
        }

        public async Task<List<ReceivedKafkaMessage<TKey,TValue>>> ReceiveAsync(CancellationToken token)
        {
            var brokerMap = await brokers.BuildBrokerMapAsync(token, topicMap);
            var tasks = CreateFetchTasks(token, brokerMap);
            var taskResults = await Task.WhenAll(tasks).ConfigureAwait(false);

            var messages = new List<ReceivedKafkaMessage<TKey, TValue>>();
            foreach (var taskResult in taskResults)
            {
                foreach (var fetchResponse in taskResult)
                {
                    if (fetchResponse.Error != (int)ErrorResponseCode.NoError)
                    {
                        Log.Error("Error in fetch response {error} for {topic}/{partition}", (ErrorResponseCode)fetchResponse.Error, fetchResponse.Topic, fetchResponse.PartitionId);
                    } else
                    {
                        var tracker = topicMap[fetchResponse.Topic][fetchResponse.PartitionId];
                        foreach (var message in fetchResponse.Messages)
                        {
                            var result = new ReceivedKafkaMessage<TKey,TValue>(
                                fetchResponse.Topic,
                                keySerializer.Deserialize(message.Key),
                                valueSerializer.Deserialize(message.Value),
                                fetchResponse.PartitionId,
                                message.Meta.Offset
                                );
                            tracker.nextOffset = message.Meta.Offset + 1;
                            messages.Add(result);
                        }
                    }
                }
            }
            return messages;
        }

        private List<Task<List<FetchResponse>>> CreateFetchTasks(CancellationToken token, Dictionary<int, Dictionary<Tuple<string, int>, TopicTracker>> brokerMap)
        {
            var tasks = new List<Task<List<FetchResponse>>>(brokerMap.Count);

            foreach (var brokerKvp in brokerMap)
            {
                var brokerId = brokerKvp.Key;
                var trackerMap = brokerKvp.Value;
                var request = CreateRequest(trackerMap);

                tasks.Add(brokers[brokerId].SendRequestAsync(request, token));
            }

            return tasks;
        }

        private FetchRequest CreateRequest(Dictionary<Tuple<string, int>, TopicTracker> trackerMap)
        {
            var fetches = new List<Fetch>(trackerMap.Count);
            foreach (var kvp in trackerMap)
            {
                var topic = kvp.Key.Item1;
                var partition = kvp.Key.Item2;
                var tracker = kvp.Value;
                var fetch = new Fetch
                {
                    MaxBytes = maxBytes,
                    Offset = tracker.nextOffset,
                    PartitionId = partition,
                    Topic = topic,
                };
                fetches.Add(fetch);
            }
            var request = new FetchRequest
            {
                MaxWaitTime = maxWaitTimeMs,
                MinBytes = 1024,
                Fetches = fetches,
            };
            return request;
        }
    }
}
