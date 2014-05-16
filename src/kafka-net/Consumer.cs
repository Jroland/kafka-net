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
    /// Provides a basic consumer of one Topic across all partitions or over a given whitelist of partitions.
    /// 
    /// TODO: provide automatic offset saving when the feature is available in 0.8.1
    /// https://issues.apache.org/jira/browse/KAFKA-993
    /// </summary>
    public class Consumer : CommonQueries
    {
        private readonly ConsumerOptions _options;
        private readonly BlockingCollection<Message> _fetchResponseQueue;
        private readonly ConcurrentDictionary<int, Task> _partitionPollingIndex = new ConcurrentDictionary<int, Task>();
        private readonly ConcurrentDictionary<int, long> _partitionOffsetIndex = new ConcurrentDictionary<int, long>();
        private readonly IScheduledTimer _topicPartitionQueryTimer;

        private int _ensureOneThread;
        private Topic _topic;
        private bool _interrupted;

        public Consumer(ConsumerOptions options, params OffsetPosition[] positions)
            : base(options.Router)
        {
            _options = options;
            _fetchResponseQueue = new BlockingCollection<Message>(_options.ConsumerBufferSize);

            //this timer will periodically look for new partitions and automatically add them to the consuming queue
            //using the same whitelist logic
            _topicPartitionQueryTimer = new ScheduledTimer()
                .Do(RefreshTopicPartition)
                .Every(TimeSpan.FromMilliseconds(_options.TopicPartitionQueryTimeMs))
                .StartingAt(DateTime.Now);

            SetOffsetPosition(positions);
        }

        /// <summary>
        /// Get the number of tasks created for consuming each partition.
        /// </summary>
        public int ConsumerTaskCount { get { return _partitionPollingIndex.Count; } }

        /// <summary>
        /// Returns a blocking enumerable of messages received from Kafka.
        /// </summary>
        /// <returns>Blocking enumberable of messages from Kafka.</returns>
        public IEnumerable<Message> Consume(CancellationToken? cancellationToken = null)
        {
            _topicPartitionQueryTimer.Begin();

            return _fetchResponseQueue.GetConsumingEnumerable(cancellationToken ?? new CancellationToken(false));
        }

        /// <summary>
        /// Force reset the offset position for a specific partition to a specific offset value.
        /// </summary>
        /// <param name="positions">Collection of positions to reset to.</param>
        public void SetOffsetPosition(params OffsetPosition[] positions)
        {
            foreach (var position in positions)
            {
                var temp = position;
                _partitionOffsetIndex.AddOrUpdate(position.PartitionId, i => temp.Offset, (i, l) => temp.Offset);
            }
        }

        /// <summary>
        /// Get the current running position (offset) for all consuming partition.
        /// </summary>
        /// <returns>List of positions for each consumed partitions.</returns>
        /// <remarks>Will only return data if the consumer is actively being consumed.</remarks>
        public List<OffsetPosition> GetOffsetPosition()
        {
            return _partitionOffsetIndex.Select(x => new OffsetPosition { PartitionId = x.Key, Offset = x.Value }).ToList();
        }

        private void RefreshTopicPartition()
        {
            try
            {
                if (Interlocked.Increment(ref _ensureOneThread) == 1)
                {
                    var topic = _options.Router.GetTopicMetadata(_options.Topic);
                    if (topic.Count <= 0) throw new ApplicationException(string.Format("Unable to get metadata for topic:{0}.", _options.Topic));
                    _topic = topic.First();

                    //create one thread per partitions, if they are in the white list.
                    foreach (var partition in _topic.Partitions)
                    {
                        var partitionId = partition.PartitionId;
                        if (_options.PartitionWhitelist.Count == 0 || _options.PartitionWhitelist.Any(x => x == partitionId))
                        {
                            _partitionPollingIndex.AddOrUpdate(partitionId,
                                                               i => ConsumeTopicPartitionAsync(_topic.Name, partitionId),
                                                               (i, task) => task);
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                _options.Log.ErrorFormat("Exception occured trying to setup consumer for topic:{0}.  Exception={1}", _options.Topic, ex);
            }
            finally
            {
                Interlocked.Decrement(ref _ensureOneThread);
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
                        var route = _options.Router.SelectBrokerRoute(topic, partitionId);
                        var responses = route.Connection.SendAsync(fetchRequest).Result;

                        if (responses.Count > 0)
                        {
                            var response = responses.First(); //we only asked for one response

                            if (response.Error != 0)
                            {
                                if (response.Error == (short)ErrorResponseCode.OffsetOutOfRange)
                                {
                                    FixOffsetRangeError(fetchRequest, response);
                                }
                            }
                            else if (response.Messages.Count > 0)
                            {
                                foreach (var message in response.Messages)
                                {
                                    _fetchResponseQueue.TryAdd(message);
                                }

                                _partitionOffsetIndex.AddOrUpdate(partitionId, i => response.HighWaterMark, (i, l) => response.HighWaterMark);
                            }

							// sleep is not needed if responses were received
	                        continue;
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

        private void FixOffsetRangeError(FetchRequest fetchRequest, FetchResponse response)
        {
            try
            {
                var fetch = fetchRequest.Fetches[0];

                Func<OffsetTime, long> getOffset =
                    offsetTime =>
                    this.GetTopicOffsetAsync(response.Topic, 1, (int)offsetTime)
                        .Result.First(r => r.PartitionId == fetch.PartitionId).Offsets[0];

                var latest = getOffset(OffsetTime.Latest);
                var earliest = getOffset(OffsetTime.Earliest);

                var asked = fetch.Offset;

                if (asked < earliest) _partitionOffsetIndex.AddOrUpdate(fetch.PartitionId, k => earliest, (k, current) => earliest);
                else if (asked > latest) _partitionOffsetIndex.AddOrUpdate(fetch.PartitionId, k => latest, (k, current) => latest);
            }
            catch
            {
                this.RefreshTopicPartition();
            }
        }

        public new void Dispose()
        {
            base.Dispose();
            using (_topicPartitionQueryTimer)
            {
                _interrupted = true;
            }
        }
    }
}
