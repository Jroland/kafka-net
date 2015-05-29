using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using KafkaNet.Common;
using KafkaNet.Model;

namespace KafkaNet.Statistics
{
    /// <summary>
    /// Statistics tracker uses circular buffers to capture a maximum set of current statistics.  
    /// </summary>
    public static class StatisticsTracker
    {
        public static event Action<StatisticsSummary> OnStatisticsHeartbeat;

        private static readonly IScheduledTimer HeartbeatTimer;
        private static readonly Gauges Gauges = new Gauges();
        private static readonly ConcurrentCircularBuffer<ProduceRequestStatistic> ProduceRequestStatistics = new ConcurrentCircularBuffer<ProduceRequestStatistic>(500);
        private static readonly ConcurrentCircularBuffer<NetworkWriteStatistic> CompletedNetworkWriteStatistics = new ConcurrentCircularBuffer<NetworkWriteStatistic>(500);
        private static readonly ConcurrentDictionary<int, NetworkWriteStatistic> NetworkWriteQueuedIndex = new ConcurrentDictionary<int, NetworkWriteStatistic>();

        static StatisticsTracker()
        {
            HeartbeatTimer = new ScheduledTimer()
                .StartingAt(DateTime.Now)
                .Every(TimeSpan.FromSeconds(5))
                .Do(HeartBeatAction)
                .Begin();
        }

        private static void HeartBeatAction()
        {
            if (OnStatisticsHeartbeat != null)
            {
                OnStatisticsHeartbeat(new StatisticsSummary(ProduceRequestStatistics.ToList(),
                    NetworkWriteQueuedIndex.Values.ToList(),
                    CompletedNetworkWriteStatistics.ToList(),
                    Gauges));
            }
        }

        public static void RecordProduceRequest(int messageCount, int payloadBytes, int compressedBytes)
        {
            ProduceRequestStatistics.Enqueue(new ProduceRequestStatistic(messageCount, payloadBytes, compressedBytes));
        }

        public static void IncrementGauge(StatisticGauge gauge)
        {
            switch (gauge)
            {
                case StatisticGauge.ActiveReadOperation:
                    Interlocked.Increment(ref Gauges.ActiveReadOperation);
                    break;
                case StatisticGauge.ActiveWriteOperation:
                    Interlocked.Increment(ref Gauges.ActiveWriteOperation);
                    break;
                case StatisticGauge.QueuedWriteOperation:
                    Interlocked.Increment(ref Gauges.QueuedWriteOperation);
                    break;
            }
        }

        public static void DecrementGauge(StatisticGauge gauge)
        {
            switch (gauge)
            {
                case StatisticGauge.ActiveReadOperation:
                    Interlocked.Decrement(ref Gauges.ActiveReadOperation);
                    break;
                case StatisticGauge.ActiveWriteOperation:
                    Interlocked.Decrement(ref Gauges.ActiveWriteOperation);
                    break;
                case StatisticGauge.QueuedWriteOperation:
                    Interlocked.Decrement(ref Gauges.QueuedWriteOperation);
                    break;
            }
        }

        public static void QueueNetworkWrite(KafkaEndpoint endpoint, KafkaDataPayload payload)
        {
            if (payload.TrackPayload == false) return;

            var stat = new NetworkWriteStatistic(endpoint, payload);
            NetworkWriteQueuedIndex.TryAdd(payload.CorrelationId, stat);
            Interlocked.Increment(ref Gauges.QueuedWriteOperation);
        }

        public static void CompleteNetworkWrite(KafkaDataPayload payload, long milliseconds, bool failed)
        {
            if (payload.TrackPayload == false) return;

            NetworkWriteStatistic stat;
            if (NetworkWriteQueuedIndex.TryRemove(payload.CorrelationId, out stat))
            {
                stat.SetCompleted(milliseconds, failed);
                CompletedNetworkWriteStatistics.Enqueue(stat);
            }
            Interlocked.Decrement(ref Gauges.QueuedWriteOperation);
        }
    }

    public enum StatisticGauge
    {
        QueuedWriteOperation,
        ActiveWriteOperation,
        ActiveReadOperation
    }

    public class StatisticsSummary
    {
        public ProduceRequestSummary ProduceRequestSummary { get; private set; }
        public List<NetworkWriteSummary> NetworkWriteSummaries { get; private set; }

        public List<ProduceRequestStatistic> ProduceRequestStatistics { get; private set; }
        public List<NetworkWriteStatistic> CompletedNetworkWriteStatistics { get; private set; }
        public List<NetworkWriteStatistic> QueuedNetworkWriteStatistics { get; private set; }
        public Gauges Gauges { get; private set; }

        public StatisticsSummary(List<ProduceRequestStatistic> produceRequestStatistics,
                                 List<NetworkWriteStatistic> queuedWrites,
                                 List<NetworkWriteStatistic> completedWrites,
                                 Gauges gauges)
        {
            ProduceRequestStatistics = produceRequestStatistics;
            QueuedNetworkWriteStatistics = queuedWrites;
            CompletedNetworkWriteStatistics = completedWrites;
            Gauges = gauges;


            if (queuedWrites.Count > 0 || completedWrites.Count > 0)
            {
                var queuedSummary = queuedWrites.GroupBy(x => x.Endpoint)
                    .Select(e => new
                    {
                        Endpoint = e.Key,
                        QueuedSummary = new NetworkQueueSummary
                        {
                            SampleSize = e.Count(),
                            OldestBatchInQueue = e.Max(x => x.TotalDuration),
                            BytesQueued = e.Sum(x => x.Payload.Buffer.Length),
                            QueuedMessages = e.Sum(x => x.Payload.MessageCount),
                            QueuedBatchCount = Gauges.QueuedWriteOperation,
                        }
                    }).ToList();

                var networkWriteSampleTimespan = completedWrites.Count <= 0 ? TimeSpan.FromMilliseconds(0) : DateTime.UtcNow - completedWrites.Min(x => x.CreatedOnUtc);
                var completedSummary = completedWrites.GroupBy(x => x.Endpoint)
                    .Select(e =>
                        new
                        {
                            Endpoint = e.Key,
                            CompletedSummary = new NetworkTcpSummary
                            {
                                MessagesPerSecond = (int)(e.Sum(x => x.Payload.MessageCount) /
                                                 networkWriteSampleTimespan.TotalSeconds),
                                MessagesLastBatch = e.OrderByDescending(x => x.CompletedOnUtc).Select(x => x.Payload.MessageCount).FirstOrDefault(),
                                MaxMessagesPerSecond = e.Max(x => x.Payload.MessageCount),
                                BytesPerSecond = (int)(e.Sum(x => x.Payload.Buffer.Length) /
                                                 networkWriteSampleTimespan.TotalSeconds),
                                AverageWriteDuration = TimeSpan.FromMilliseconds(e.Sum(x => x.WriteDuration.TotalMilliseconds) /
                                                       completedWrites.Count),
                                AverageTotalDuration = TimeSpan.FromMilliseconds(e.Sum(x => x.TotalDuration.TotalMilliseconds) /
                                                       completedWrites.Count),
                                SampleSize = completedWrites.Count
                            }
                        }
                    ).ToList();

                NetworkWriteSummaries = new List<NetworkWriteSummary>();
                var endpoints = queuedSummary.Select(x => x.Endpoint).Union(completedWrites.Select(x => x.Endpoint));
                foreach (var endpoint in endpoints)
                {
                    NetworkWriteSummaries.Add(new NetworkWriteSummary
                    {
                        Endpoint = endpoint,
                        QueueSummary = queuedSummary.Where(x => x.Endpoint.Equals(endpoint)).Select(x => x.QueuedSummary).FirstOrDefault(),
                        TcpSummary = completedSummary.Where(x => x.Endpoint.Equals(endpoint)).Select(x => x.CompletedSummary).FirstOrDefault()
                    });
                }
            }
            else
            {
                NetworkWriteSummaries = new List<NetworkWriteSummary>();
            }

            if (ProduceRequestStatistics.Count > 0)
            {
                var produceRequestSampleTimespan = DateTime.UtcNow -
                                                   ProduceRequestStatistics.Min(x => x.CreatedOnUtc);

                ProduceRequestSummary = new ProduceRequestSummary
                {
                    SampleSize = ProduceRequestStatistics.Count,
                    MessageCount = ProduceRequestStatistics.Sum(s => s.MessageCount),
                    MessageBytesPerSecond = (int)
                            (ProduceRequestStatistics.Sum(s => s.MessageBytes) / produceRequestSampleTimespan.TotalSeconds),
                    PayloadBytesPerSecond = (int)
                            (ProduceRequestStatistics.Sum(s => s.PayloadBytes) / produceRequestSampleTimespan.TotalSeconds),
                    CompressedBytesPerSecond = (int)
                            (ProduceRequestStatistics.Sum(s => s.CompressedBytes) / produceRequestSampleTimespan.TotalSeconds),
                    AverageCompressionRatio =
                        Math.Round(ProduceRequestStatistics.Sum(s => s.CompressionRatio) / ProduceRequestStatistics.Count, 4),
                    MessagesPerSecond = (int)
                            (ProduceRequestStatistics.Sum(x => x.MessageCount) / produceRequestSampleTimespan.TotalSeconds)
                };
            }
            else
            {
                ProduceRequestSummary = new ProduceRequestSummary();
            }
        }
    }

    public class Gauges
    {
        public int ActiveWriteOperation;
        public int ActiveReadOperation;
        public int QueuedWriteOperation;
    }

    public class NetworkWriteStatistic
    {
        public DateTime CreatedOnUtc { get; private set; }
        public DateTime CompletedOnUtc { get; private set; }
        public bool IsCompleted { get; private set; }
        public bool IsFailed { get; private set; }
        public KafkaEndpoint Endpoint { get; private set; }
        public KafkaDataPayload Payload { get; private set; }
        public TimeSpan TotalDuration { get { return (IsCompleted ? CompletedOnUtc : DateTime.UtcNow) - CreatedOnUtc; } }
        public TimeSpan WriteDuration { get; private set; }

        public NetworkWriteStatistic(KafkaEndpoint endpoint, KafkaDataPayload payload)
        {
            CreatedOnUtc = DateTime.UtcNow;
            Endpoint = endpoint;
            Payload = payload;
        }

        public void SetCompleted(long milliseconds, bool failedFlag)
        {
            IsCompleted = true;
            IsFailed = failedFlag;
            CompletedOnUtc = DateTime.UtcNow;
            WriteDuration = TimeSpan.FromMilliseconds(milliseconds);
        }

        public void SetSuccess(bool failed)
        {
            IsFailed = failed;
        }
    }

    public class NetworkWriteSummary
    {
        public KafkaEndpoint Endpoint;

        public NetworkTcpSummary TcpSummary = new NetworkTcpSummary();
        public NetworkQueueSummary QueueSummary = new NetworkQueueSummary();
    }

    public class NetworkQueueSummary
    {
        public int BytesQueued;
        public double KilobytesQueued { get { return MathHelper.ConvertToKilobytes(BytesQueued); } }
        public TimeSpan OldestBatchInQueue { get; set; }
        public int QueuedMessages { get; set; }
        public int QueuedBatchCount;
        public int SampleSize { get; set; }
    }

    public class NetworkTcpSummary
    {
        public int MessagesPerSecond;
        public int MaxMessagesPerSecond;
        public int BytesPerSecond;
        public TimeSpan AverageWriteDuration;
        public double KilobytesPerSecond { get { return MathHelper.ConvertToKilobytes(BytesPerSecond); } }
        public TimeSpan AverageTotalDuration { get; set; }
        public int SampleSize { get; set; }
        public int MessagesLastBatch { get; set; }
    }

    public class ProduceRequestSummary
    {
        public int SampleSize;
        public int MessageCount;
        public int MessagesPerSecond;
        public int MessageBytesPerSecond;
        public double MessageKilobytesPerSecond { get { return MathHelper.ConvertToKilobytes(MessageBytesPerSecond); } }
        public int PayloadBytesPerSecond;
        public double PayloadKilobytesPerSecond { get { return MathHelper.ConvertToKilobytes(PayloadBytesPerSecond); } }
        public int CompressedBytesPerSecond;
        public double CompressedKilobytesPerSecond { get { return MathHelper.ConvertToKilobytes(CompressedBytesPerSecond); } }
        public double AverageCompressionRatio;
    }

    public class ProduceRequestStatistic
    {
        public DateTime CreatedOnUtc { get; private set; }
        public int MessageCount { get; private set; }
        public int MessageBytes { get; private set; }
        public int PayloadBytes { get; private set; }
        public int CompressedBytes { get; private set; }
        public double CompressionRatio { get; private set; }

        public ProduceRequestStatistic(int messageCount, int payloadBytes, int compressedBytes)
        {
            CreatedOnUtc = DateTime.UtcNow;
            MessageCount = messageCount;
            MessageBytes = payloadBytes + compressedBytes;
            PayloadBytes = payloadBytes;
            CompressedBytes = compressedBytes;

            CompressionRatio = MessageBytes == 0 ? 0 : Math.Round((double)compressedBytes / MessageBytes, 4);
        }


    }

    public static class MathHelper
    {
        public static double ConvertToMegabytes(int bytes)
        {
            if (bytes == 0) return 0;
            return Math.Round((double)bytes / 1048576, 4);
        }

        public static double ConvertToKilobytes(int bytes)
        {
            if (bytes == 0) return 0;
            return Math.Round((double)bytes / 1000, 4);
        }
    }
}
