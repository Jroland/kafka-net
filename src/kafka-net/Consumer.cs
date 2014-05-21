using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
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
        private readonly ConcurrentDictionary<int, PartitionConsumer> _partitionPollingIndex = new ConcurrentDictionary<int, PartitionConsumer>();
        private readonly IScheduledTimer _topicPartitionQueryTimer;

        private bool _disposed;

        private int _ensureOneThread;
        private Topic _topic;

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
        /// <param name="cancellationToken"></param>
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
            positions.Join(_partitionPollingIndex.Values, position => position.PartitionId, consumer => consumer.PartitionId, (position, consumer) => new{position, consumer}).ToList().ForEach(t => t.consumer.Offset = t.position.Offset);
        }

        /// <summary>
        /// Get the current running position (offset) for all consuming partition.
        /// </summary>
        /// <returns>List of positions for each consumed partitions.</returns>
        /// <remarks>Will only return data if the consumer is actively being consumed.</remarks>
        public List<OffsetPosition> GetOffsetPosition()
        {
            return _partitionPollingIndex.Values.Select(x => new OffsetPosition { PartitionId = x.PartitionId, Offset = x.Offset }).ToList();
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
                            Func<int, PartitionConsumer> addValueFactory = _ =>
                                {
                                    var partitionConsumer = new PartitionConsumer(
                                        _options,
                                        _topic.Name,
                                        partitionId,
                                        message => _fetchResponseQueue.Add(message));
                                    partitionConsumer.Start();
                                    return partitionConsumer;
                                };

                            this._partitionPollingIndex.AddOrUpdate(partitionId, addValueFactory, (i, consumer) => consumer);
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

        protected override void Dispose(bool disposing)
        {
            if (_disposed) return;

            if (disposing)
            {
                using (_topicPartitionQueryTimer)
                {
                    foreach (var partitionConsumer in _partitionPollingIndex.Values)
                    {
                        partitionConsumer.Dispose();
                    }
                }
            }

            base.Dispose(disposing);
            _disposed = true;
        }
    }
}
