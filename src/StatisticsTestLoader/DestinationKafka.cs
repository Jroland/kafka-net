using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using KafkaNet;
using KafkaNet.Model;
using KafkaNet.Protocol;
using KafkaNet.Statistics;

namespace StatisticsTestLoader
{
    public class DestinationKafka
    {
        private const string ConsumerGroupName = "pcs_encoding_updates";

        private readonly BrokerRouter _router;
        private readonly Producer _producer;

        public DestinationKafka(params Uri[] servers)
        {
            var options = new KafkaOptions(servers) { Log = new ConsoleLogger() };
            _router = new BrokerRouter(options);
            _producer = new Producer(_router, maximumMessageBuffer: 5000, maximumAsyncRequests: 10) { BatchSize = 1000, BatchDelayTime = TimeSpan.FromSeconds(1) };

            StatisticsTracker.OnStatisticsHeartbeat += StatisticsTracker_OnStatisticsHeartbeat;
        }

        void StatisticsTracker_OnStatisticsHeartbeat(StatisticsSummary stats)
        {
            Console.WriteLine("Producer: Buffer: {0} AsyncQueued: {1}", _producer.BufferCount, _producer.AsyncCount);
            Console.WriteLine("Produced: Msgs: {0} New/s: {1}  MsgKilobytes/s: {2} PayloadKiloytes/s: {3} CompressionRatio: {4}",
                stats.ProduceRequestSummary.MessageCount,
                stats.ProduceRequestSummary.MessagesPerSecond,
                stats.ProduceRequestSummary.MessageKilobytesPerSecond,
                stats.ProduceRequestSummary.PayloadKilobytesPerSecond,
                stats.ProduceRequestSummary.AverageCompressionRatio);

            stats.NetworkWriteSummaries.ForEach(s =>
            {
                Console.WriteLine("Endpoint: {0}", s.Endpoint);
                if (s.QueueSummary != null)
                {
                    Console.WriteLine("Q = Messages: {0}, Q Kilobytes: {1}, OldestInQueue:{2},  BatchCount: {3}",
                        s.QueueSummary.QueuedMessages,
                        s.QueueSummary.KilobytesQueued, 
                        s.QueueSummary.OldestBatchInQueue.TotalMilliseconds,
                        s.QueueSummary.QueuedBatchCount);
                }

                if (s.TcpSummary != null)
                {
                    Console.WriteLine("C = Msg/s: {0},  Last: {1},  Kilobytes/s: {2}, AvgTcpMS:{3} AvgTotalMS: {4} Async: {5}",
                        s.TcpSummary.MessagesPerSecond,
                        s.TcpSummary.MessagesLastBatch,
                        s.TcpSummary.KilobytesPerSecond,
                        s.TcpSummary.AverageWriteDuration.TotalMilliseconds,
                        s.TcpSummary.AverageTotalDuration.TotalMilliseconds,
                        stats.Gauges.ActiveWriteOperation);
                }
            });

            Console.WriteLine("Upload Rate: Msg/s: {0}  Kilobytes/s: {1}  Max Msg/s: {2}  Last Batch: {3}", 
                stats.NetworkWriteSummaries.Where(x => x.TcpSummary != null).Sum(x => x.TcpSummary.MessagesPerSecond),
                stats.NetworkWriteSummaries.Where(x => x.TcpSummary != null).Sum(x => x.TcpSummary.KilobytesPerSecond),
                stats.NetworkWriteSummaries.Where(x => x.TcpSummary != null).Sum(x => x.TcpSummary.MaxMessagesPerSecond),
                stats.NetworkWriteSummaries.Where(x => x.TcpSummary != null).Sum(x => x.TcpSummary.MessagesLastBatch));

            Console.WriteLine("");
        }

        public int AsyncCount { get { return _producer.AsyncCount; } }
        public int BufferCount { get { return _producer.BufferCount; } }

        public async Task PostBatchAsync(List<KafkaRecord> records)
        {
            try
            {
                foreach (var topicBatch in records.GroupBy(x => x.Topic))
                {
                    Console.WriteLine("POSTING: {0}, {1}", topicBatch.Count(), _producer.BufferCount);
                    var result = await _producer
                        .SendMessageAsync(topicBatch.Key, topicBatch.Select(x => x.Message), acks: 0, codec: MessageCodec.CodecGzip)
                        .ConfigureAwait(false);

                    if (result.Any(x => x.Error != (int)ErrorResponseCode.NoError))
                    {
                        Console.WriteLine("Some send messages failed to store.");
                    }
                    else
                    {
                        await SetStoredOffsetAsync(topicBatch.Key, topicBatch.Max(x => x.Offset)).ConfigureAwait(false);
                    }
                }
            }
            catch (Exception ex)
            {
                //TODO need to signal a stop for the failed topic
                Console.WriteLine("Failed {0}", ex);
            }
        }

        public long GetStoredOffset(string topic)
        {
            return GetOffset(topic).Offset;
        }

        public async Task<OffsetCommitResponse> SetStoredOffsetAsync(string topic, long offset)
        {
            var request = new OffsetCommitRequest
            {
                ConsumerGroup = ConsumerGroupName,
                OffsetCommits = new List<OffsetCommit>
                {
                    new OffsetCommit
                    {
                        PartitionId = 0,
                        Topic = topic,
                        Offset = offset
                    }
                }
            };

            var result = await _router.SelectBrokerRoute(topic).Connection.SendAsync(request).ConfigureAwait(false);
            return result.FirstOrDefault();
        }

        private OffsetFetchResponse GetOffset(string topic)
        {
            var request = new OffsetFetchRequest
            {
                ConsumerGroup = ConsumerGroupName,
                Topics = new List<OffsetFetch>
                {
                    new OffsetFetch
                    {
                        PartitionId = 0,
                        Topic = topic
                    }
                }
            };


            return _router.SelectBrokerRoute(topic).Connection.SendAsync(request).Result.FirstOrDefault();
        }
    }
}