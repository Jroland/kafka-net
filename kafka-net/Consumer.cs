using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using KafkaNet.Common;
using KafkaNet.Model;
using KafkaNet.Protocol;

namespace KafkaNet
{
    /// <summary>
    /// Provides a basic consumer of one Topic across all partitions and pulls them into one enumerable stream.
    /// 
    /// TODO: provide automatic offset saving when the feature is available in 0.8.1
    /// https://issues.apache.org/jira/browse/KAFKA-993
    /// </summary>
    public class Consumer : IDisposable
    {
        private readonly ConsumerOptions _options;
        private readonly TopicOffsetTracker _offsetTracker;
        private readonly BlockingCollection<Message> _fetchResponseQueue;
        private readonly ConcurrentDictionary<int, Task> _partitionPollingIndex = new ConcurrentDictionary<int, Task>();

        private readonly IScheduledTimer _topicPartitionQueryTimer;
        private readonly IScheduledTimer _topicMaxOffsetQueryTimer;
        private Topic _topic;
        private bool _interrupted;

        public Consumer(ConsumerOptions options)
        {
            _options = options;
            _offsetTracker = new TopicOffsetTracker(_options.Router);
            _fetchResponseQueue = new BlockingCollection<Message>(_options.ConsumerBufferSize);

            _topicPartitionQueryTimer = new ScheduledTimer()
                .Do(RefreshTopicPartition)
                .Every(TimeSpan.FromMilliseconds(_options.TopicPartitionQueryTimeMs))
                .StartingAt(DateTime.Now);

            _topicMaxOffsetQueryTimer = new ScheduledTimer()
                .Do(RefreshTopicMaxOffset)
                .Every(TimeSpan.FromMilliseconds(_options.TopicMaxOffsetQueryTimeMs))
                .StartingAt(DateTime.Now);
        }

        /// <summary>
        /// Returns a blocking enumerable of messages that receives messages when Kafka.
        /// </summary>
        /// <returns>Blocking enumberable of messages from Kafka.</returns>
        public IEnumerable<Message> Consume()
        {
            _topicMaxOffsetQueryTimer.Begin();
            _topicPartitionQueryTimer.Begin();

            return _fetchResponseQueue.GetConsumingEnumerable();
        }
        
        private void RefreshTopicPartition()
        {
            var topic = _options.Router.GetTopicMetadataAsync(_options.Topic).Result;
            if (topic.Count <= 0) throw new ApplicationException(string.Format("Unable to get metadata for topic:{0}.", _options.Topic));
            _topic = topic.First();

            //create one thread per partitions.  Only allow one.
            foreach (var partition in _topic.Partitions)
            {
                var partitionId = partition.PartitionId;
                _partitionPollingIndex.AddOrUpdate(partitionId,
                                                   i => ConsumeTopicPartitionAsync(_topic.Name, partitionId),
                                                   (i, task) => task);
            }
        }

        private void RefreshTopicMaxOffset()
        {
            if (_topic != null)
                _offsetTracker.RefreshTopicOffsets(_topic);
        }

        private Task ConsumeTopicPartitionAsync(string topic, int partitionId)
        {
            return Task.Factory.StartNew(() =>
            {
                while (_interrupted == false)
                {
                    try
                    {
                        var partitionOffset = _offsetTracker.GetTopicPartitionOffset(topic, partitionId);
                        if (partitionOffset != null) //if null the partition is not initialized yet. just wait.
                        {
                            //The max number returned is the next unassigned value in the kafka log.
                            while (partitionOffset.CurrentOffset < partitionOffset.MaxOffset)
                            {
                                //build fetch for each item in the batchSize
                                var fetches = new List<Fetch>
                                    {
                                        new Fetch
                                            {
                                                Topic = topic,
                                                PartitionId = partitionId,
                                                Offset = partitionOffset.CurrentOffset
                                            }
                                    };

                                var fetchRequest = new FetchRequest
                                    {
                                        Fetches = fetches
                                    };

                                //make request and post to queue
                                var route = _options.Router.SelectBrokerRouteAsync(topic, partitionId).Result;
                                var responses = route.Connection.SendAsync(fetchRequest).Result;

                                if (responses.Count > 0)
                                {
                                    var response = responses.First(); //we only asked for one response
                                    foreach (var message in response.Messages)
                                    {
                                        _fetchResponseQueue.TryAdd(message);
                                    }

                                    partitionOffset.CurrentOffset = response.HighWaterMark;
                                }
                            }
                        }

                        Thread.Sleep(100);
                    }
                    catch (Exception ex)
                    {
                        _options.Log.ErrorFormat("Exception occured while polling topic:{0} partition:{1}.  Polling will continue.  Exception={2}", topic, partitionId, ex);
                    }
                }
            });
        }
        
        public void Dispose()
        {
            using (_topicMaxOffsetQueryTimer)
            using (_topicPartitionQueryTimer)
            using (_options.Router)
            {
                _interrupted = true;
            }
        }
    }


    class TopicOffsetTracker
    {
        private readonly BrokerRouter _router;
        private readonly ConcurrentDictionary<string, PartitionOffsetTracker> _topicIndex = new ConcurrentDictionary<string, PartitionOffsetTracker>();

        public TopicOffsetTracker(BrokerRouter router)
        {
            _router = router;
        }

        public void ForceTopicPartitionCurrentOffset(string topic, int partitionId, long currentOffset)
        {
            PartitionOffsetTracker tracker;
            if (_topicIndex.TryGetValue(topic, out tracker))
            {
                tracker.ForcePartitionCurrentOffset(partitionId, currentOffset);
            }
        }

        public void UpdateTopicPartitionCurrentOffset(string topic, int partitionId, long currentOffset)
        {
            PartitionOffsetTracker tracker;
            if (_topicIndex.TryGetValue(topic, out tracker))
            {
                tracker.UpdatePartitionCurrentOffset(partitionId, currentOffset);
            }
        }

        public PartitionOffsetItem GetTopicPartitionOffset(string topic, int partitionId)
        {
            PartitionOffsetTracker tracker;
            if (_topicIndex.TryGetValue(topic, out tracker))
            {
                return tracker.GetPartitionOffset(partitionId);
            }

            return null;
        }

        public void RefreshTopicOffsets(Topic topic)
        {
            var offsetData = RequestTopicOffsetData(topic);
            _topicIndex.AddOrUpdate(topic.Name, s => new PartitionOffsetTracker(offsetData),
                (s, tracker) => { tracker.UpdateTracked(offsetData); return tracker; });
        }

        private IEnumerable<OffsetResponse> RequestTopicOffsetData(Topic topic)
        {
            var route = _router.SelectBrokerRouteAsync(topic.Name).Result;
            var request = new OffsetRequest
            {
                Offsets = new List<Offset>(topic.Partitions.Select(x => new Offset
                {
                    Topic = topic.Name,
                    PartitionId = x.PartitionId,
                    MaxOffsets = 1,
                    Time = -1
                }))
            };

            return route.Connection.SendAsync(request).Result;
        }
    }

    class PartitionOffsetTracker
    {
        private readonly ConcurrentDictionary<int, PartitionOffsetItem> _partitionIndex = new ConcurrentDictionary<int, PartitionOffsetItem>();

        public PartitionOffsetTracker(IEnumerable<OffsetResponse> responses)
        {
            UpdateTracked(responses);
        }

        public List<PartitionOffsetItem> PartitionOffsets { get { return _partitionIndex.Values.ToList(); } }

        public void ForcePartitionCurrentOffset(int partitionId, long currentOffset)
        {
            Seek(partitionId, currentOffset, true);
        }

        public void UpdatePartitionCurrentOffset(int partitionId, long currentOffset)
        {
            Seek(partitionId, currentOffset, false);
        }

        public PartitionOffsetItem GetPartitionOffset(int partitionId)
        {
            PartitionOffsetItem offsetItem;
            if (_partitionIndex.TryGetValue(partitionId, out offsetItem))
            {
                return offsetItem;
            }

            return null;
        }

        public void UpdateTracked(IEnumerable<OffsetResponse> responses)
        {
            foreach (var response in responses)
            {
                var temp = response;
                _partitionIndex.AddOrUpdate(response.PartitionId,
                    i => new PartitionOffsetItem { PartitionId = i, MaxOffset = temp.Offsets.Max() },
                    (i, item) => { item.MaxOffset = temp.Offsets.Max(); return item; });
            }
        }

        private void Seek(int partitionId, long currentOffset, bool forceUpdate)
        {
            if (_partitionIndex.ContainsKey(partitionId))
            {
                _partitionIndex.AddOrUpdate(partitionId, i => null, (i, item) =>
                    {
                        if (forceUpdate) item.MaxOffset = currentOffset;
                        else if (item.MaxOffset < currentOffset) item.MaxOffset = currentOffset;
                        return item;
                    });
            }
        }
    }

    class PartitionOffsetItem
    {
        public int PartitionId { get; set; }
        public long CurrentOffset { get; set; }
        public long MaxOffset { get; set; }
    }
}
