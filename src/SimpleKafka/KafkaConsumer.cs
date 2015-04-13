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

                    case OffsetSelectionStrategy.Earliest:
                    case OffsetSelectionStrategy.Last: 
                    case OffsetSelectionStrategy.Next: 
                    case OffsetSelectionStrategy.NextUncommitted:
                        nextOffset = (long)selector.DefaultOffsetSelection; 
                        break;
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
        private readonly string consumerGroup;

        public KafkaConsumer(string consumerGroup, KafkaBrokers brokers, IKafkaSerializer<TKey> keySerializer, IKafkaSerializer<TValue> valueSerializer, params TopicSelector[] topics)
        {
            this.consumerGroup = consumerGroup;
            this.brokers = brokers;
            this.keySerializer = keySerializer;
            this.valueSerializer = valueSerializer;
            topicMap = new Dictionary<string, Dictionary<int, TopicTracker>>();
            foreach (var topic in topics)
            {
                var partitionMap = topicMap.GetOrCreate(topic.Topic);
                if (partitionMap.ContainsKey(topic.Partition))
                {
                    throw new InvalidOperationException("Topic " + topic.Topic + ", partition " + topic.Partition + " duplicated");
                }
                partitionMap.Add(topic.Partition, new TopicTracker(topic));
            }
        }

        public async Task CommitAsync(IEnumerable<TopicPartitionOffset> offsets, CancellationToken token)
        {
            var coordinator = await GetOffsetCoordinatorConnectionAsync(token).ConfigureAwait(false);
            var offsetCommits = new List<OffsetCommit>();
            foreach (var offset in offsets) {
                var offsetCommit = new OffsetCommit {
                     Offset = offset.Offset,
                     PartitionId = offset.Partition,
                     Topic = offset.Topic
                };
                offsetCommits.Add(offsetCommit);
            }
            var request = new OffsetCommitRequest
            {
                ConsumerGroup = consumerGroup,
                ConsumerGroupGenerationId = 0,
                ConsumerId = "test",
                OffsetCommits = offsetCommits
            };

            var responses = await coordinator.SendRequestAsync(request, token).ConfigureAwait(false);
            foreach (var response in responses)
            {
                if (response.Error != ErrorResponseCode.NoError)
                {
                    throw new InvalidOperationException("Failed to commit: " + response.Error);
                }
            }
        }

        private async Task<KafkaConnection> GetOffsetCoordinatorConnectionAsync(CancellationToken token)
        {
            var map = await brokers.BuildOffsetCoordinatorMapAsync(token, consumerGroup).ConfigureAwait(false);
            var coordinator = map[consumerGroup];
            return brokers[coordinator];
        }

        public async Task<List<ReceivedKafkaMessage<TKey,TValue>>> ReceiveAsync(CancellationToken token)
        {
            var brokerMap = await brokers.BuildBrokerMapAsync(token, topicMap).ConfigureAwait(false);
            await RetrieveAnyTopicOffsets(token, brokerMap).ConfigureAwait(false);
            await RetrieveAnyConsumerOffsets(token, brokerMap).ConfigureAwait(false);
            var tasks = CreateFetchTasks(token, brokerMap);
            var taskResults = await Task.WhenAll(tasks).ConfigureAwait(false);

            var messages = new List<ReceivedKafkaMessage<TKey, TValue>>();
            foreach (var taskResult in taskResults)
            {
                foreach (var fetchResponse in taskResult)
                {
                    if (fetchResponse.Error != (int)ErrorResponseCode.NoError)
                    {
                        Log.Error("Error in fetch response {error} for {topic}/{partition}", fetchResponse.Error, fetchResponse.Topic, fetchResponse.PartitionId);
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
                MinBytes = minBytes,
                Fetches = fetches,
            };
            return request;
        }

        private async Task RetrieveAnyTopicOffsets(CancellationToken token, Dictionary<int, Dictionary<Tuple<string, int>, TopicTracker>> brokerMap)
        {
            foreach (var brokerKvp in brokerMap)
            {
                List<Offset> offsets = null;
                var trackerMap = brokerKvp.Value;
                foreach (var trackerKvp in trackerMap)
                {
                    var tracker = trackerKvp.Value;
                    if ((tracker.nextOffset < 0) && (tracker.nextOffset != (long)OffsetSelectionStrategy.NextUncommitted))
                    {
                        if (offsets == null)
                        {
                            offsets = new List<Offset>();
                        }
                        var offset = new Offset
                        {
                            MaxOffsets = 1,
                            PartitionId = trackerKvp.Key.Item2,
                            Topic = trackerKvp.Key.Item1
                        };
                        switch (tracker.nextOffset)
                        {
                            case (long)OffsetSelectionStrategy.Earliest: offset.Time = -2; break;
                            case (long)OffsetSelectionStrategy.Next: offset.Time = -1; break;
                            case (long)OffsetSelectionStrategy.Last: offset.Time = -1; break;
                            default: throw new InvalidOperationException("Unknown offset: " + tracker.nextOffset);
                        }
                        offsets.Add(offset);
                    }
                }

                if (offsets != null)
                {
                    var request = new OffsetRequest { Offsets = offsets };
                    var responses = await brokers[brokerKvp.Key].SendRequestAsync(request, token).ConfigureAwait(false);
                    foreach (var response in responses)
                    {
                        if (response.Error != ErrorResponseCode.NoError)
                        {
                            throw new InvalidOperationException("Unknown error fetching offsets: " + response.Error);
                        }
                        var tracker = trackerMap[Tuple.Create(response.Topic, response.PartitionId)];
                        switch (tracker.nextOffset)
                        {
                            case (long)OffsetSelectionStrategy.Earliest:
                            case (long)OffsetSelectionStrategy.Next:
                                tracker.nextOffset = response.Offsets[0];
                                break;

                            case (long)OffsetSelectionStrategy.Last:
                                tracker.nextOffset = response.Offsets[0] - 1;
                                break;
                        }
                    }
                }
            }

        }

        private async Task RetrieveAnyConsumerOffsets(CancellationToken token, Dictionary<int, Dictionary<Tuple<string, int>, TopicTracker>> brokerMap)
        {
            foreach (var brokerKvp in brokerMap)
            {
                List<OffsetFetch> fetches = null;
                var trackerMap = brokerKvp.Value;
                foreach (var trackerKvp in trackerMap)
                {
                    var tracker = trackerKvp.Value;
                    if (tracker.nextOffset == (long)OffsetSelectionStrategy.NextUncommitted)
                    {
                        if (fetches == null)
                        {
                            fetches = new List<OffsetFetch>();
                        }
                        var fetch = new OffsetFetch
                        {
                            Topic = trackerKvp.Key.Item1,
                            PartitionId = trackerKvp.Key.Item2,
                        };
                        fetches.Add(fetch);
                    }
                }

                if (fetches != null)
                {
                    var request = new OffsetFetchRequest { ConsumerGroup = consumerGroup, Topics = fetches };
                    var coordinator = await GetOffsetCoordinatorConnectionAsync(token).ConfigureAwait(false);
                    var responses = await coordinator.SendRequestAsync(request, token).ConfigureAwait(false);
                    foreach (var response in responses)
                    {
                        if (response.Error != ErrorResponseCode.NoError)
                        {
                            throw new InvalidOperationException("Unknown error fetching offsets: " + response.Error);
                        }
                        var tracker = trackerMap[Tuple.Create(response.Topic, response.PartitionId)];
                        tracker.nextOffset = response.Offset + 1;
                    }
                }
            }


        }


    }
}
