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
        private readonly BlockingCollection<Message> _fetchResponseQueue;
        private readonly ConcurrentDictionary<int, Task> _partitionPollingIndex = new ConcurrentDictionary<int, Task>();
        private readonly ConcurrentDictionary<int, long> _partitionOffsetIndex = new ConcurrentDictionary<int, long>();

        private readonly IScheduledTimer _topicPartitionQueryTimer;
        private Topic _topic;
        private bool _interrupted;

        public Consumer(ConsumerOptions options)
        {
            _options = options;
            _fetchResponseQueue = new BlockingCollection<Message>(_options.ConsumerBufferSize);

            _topicPartitionQueryTimer = new ScheduledTimer()
                .Do(RefreshTopicPartition)
                .Every(TimeSpan.FromMilliseconds(_options.TopicPartitionQueryTimeMs))
                .StartingAt(DateTime.Now);
        }

        /// <summary>
        /// Returns a blocking enumerable of messages that receives messages when Kafka.
        /// </summary>
        /// <returns>Blocking enumberable of messages from Kafka.</returns>
        public IEnumerable<Message> Consume()
        {
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

        private Task ConsumeTopicPartitionAsync(string topic, int partitionId)
        {
            return Task.Factory.StartNew(() =>
            {
                while (_interrupted == false)
                {
                    try
                    {
                        long offset = 0;
                        _partitionOffsetIndex.AddOrUpdate(partitionId, i => offset, (i, l) => { offset = l; return l; });

                        //build fetch for each item in the batchSize
                        var fetches = new List<Fetch>
                                    {
                                        new Fetch
                                            {
                                                Topic = topic,
                                                PartitionId = partitionId,
                                                Offset = offset
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
                            if (response.Messages.Count > 0)
                            {
                                foreach (var message in response.Messages)
                                {
                                    _fetchResponseQueue.TryAdd(message);
                                }

                                _partitionOffsetIndex.AddOrUpdate(partitionId, i => response.HighWaterMark, (i, l) => response.HighWaterMark);
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
            using (_topicPartitionQueryTimer)
            using (_options.Router)
            {
                _interrupted = true;
            }
        }
    }
}
