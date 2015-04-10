using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using KafkaNet.Common;
using KafkaNet.Model;

namespace KafkaNet.Statistics
{
    public static class StatisticsTracker
    {
        public static event Action<StatisticsSummary> OnStatisticsHeartbeat;

        private static readonly IScheduledTimer HeartbeatTimer;
        private static readonly Gauges _gauges = new Gauges();
        private static readonly ConcurrentBag<ProduceRequestStatistic> ProduceRequestStatistics = new ConcurrentBag<ProduceRequestStatistic>();
        private static readonly ConcurrentBag<NetworkWriteStatistic> NetworkWriteStatistics = new ConcurrentBag<NetworkWriteStatistic>();

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
                OnStatisticsHeartbeat(new StatisticsSummary(ProduceRequestStatistics.ToList(), NetworkWriteStatistics.ToList(), _gauges));
            }
        }

        public static void RecordProduceRequest(int payloadBytes, int compressedBytes)
        {
            ProduceRequestStatistics.Add(new ProduceRequestStatistic(payloadBytes, compressedBytes));
        }

        public static void RecordNetworkWrite(KafkaEndpoint endpoint, int payloadBytes)
        {
            NetworkWriteStatistics.Add(new NetworkWriteStatistic(endpoint, payloadBytes));
        }

        public static void IncrementActiveWrite()
        {
            Interlocked.Increment(ref _gauges.ActiveWrites);
        }

        public static void DecrementActiveWrite()
        {
            Interlocked.Decrement(ref _gauges.ActiveWrites);
        }
    }

    public class StatisticsSummary
    {
        public ProduceRequestSummary ProduceRequestSummary { get; private set; }
        public List<NetworkWriteSummary> NetworkWriteSummaries { get; private set; }

        public List<ProduceRequestStatistic> ProduceRequestStatistics { get; private set; }
        public List<NetworkWriteStatistic> NetworkWriteStatistics { get; private set; }
        public Gauges Gauges { get; private set; }

        public StatisticsSummary(List<ProduceRequestStatistic> produceRequestStatistics, List<NetworkWriteStatistic> networkWriteStatistics, Gauges gauges)
        {
            ProduceRequestStatistics = produceRequestStatistics;
            NetworkWriteStatistics = networkWriteStatistics;
            Gauges = gauges;

            if (networkWriteStatistics.Count > 0)
            {
                var networkWriteSampleTimespan = NetworkWriteStatistics.Max(x => x.CreatedOnUtc) -
                                                 NetworkWriteStatistics.Min(x => x.CreatedOnUtc);
                NetworkWriteSummaries = NetworkWriteStatistics
                    .GroupBy(x => x.Endpoint)
                    .Select(g => new NetworkWriteSummary
                    {
                        Endpoint = g.Key,
                        BytesPerSecond = (int)(g.Sum(x => x.PayloadBytes) / networkWriteSampleTimespan.TotalSeconds),
                        SampleSize = NetworkWriteStatistics.Count
                    }).ToList();
            }
            else
            {
                NetworkWriteSummaries = new List<NetworkWriteSummary>();
            }

            if (ProduceRequestStatistics.Count > 0)
            {
                var produceRequestSampleTimespan = ProduceRequestStatistics.Max(x => x.CreatedOnUtc) -
                                                   ProduceRequestStatistics.Min(x => x.CreatedOnUtc);

                ProduceRequestSummary = new ProduceRequestSummary
                {
                    SampleSize = ProduceRequestStatistics.Count,
                    MessageBytesPerSecond = (int)(ProduceRequestStatistics.Sum(s => s.MessageBytes) / produceRequestSampleTimespan.TotalSeconds),
                    PayloadBytesPerSecond = (int)(ProduceRequestStatistics.Sum(s => s.PayloadBytes) / produceRequestSampleTimespan.TotalSeconds),
                    CompressedBytesPerSecond = (int)(ProduceRequestStatistics.Sum(s => s.CompressedBytes) / produceRequestSampleTimespan.TotalSeconds),
                    AverageCompressionRatio = Math.Round(ProduceRequestStatistics.Sum(s => s.CompressionRatio) / ProduceRequestStatistics.Count, 4)
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
        public int ActiveWrites;
    }

    public class NetworkWriteStatistic
    {
        public DateTime CreatedOnUtc { get; private set; }
        public KafkaEndpoint Endpoint { get; private set; }
        public int PayloadBytes { get; private set; }

        public NetworkWriteStatistic(KafkaEndpoint endpoint, int payloadBytes)
        {
            CreatedOnUtc = DateTime.UtcNow.RoundToSeconds();
            Endpoint = endpoint;
            PayloadBytes = payloadBytes;
        }
    }

    public class NetworkWriteSummary
    {
        public KafkaEndpoint Endpoint;
        public int SampleSize;
        public int BytesPerSecond;

        public double MBytesPerSecond { get { return MathHelper.ConvertToMegabytes(BytesPerSecond); } }
    }

    public class ProduceRequestSummary
    {
        public int SampleSize;
        public int MessageBytesPerSecond;
        public double MessageMBytesPerSecond { get { return MathHelper.ConvertToMegabytes(MessageBytesPerSecond); } }
        public int PayloadBytesPerSecond;
        public double PayloadMBytesPerSecond { get { return MathHelper.ConvertToMegabytes(PayloadBytesPerSecond); } }
        public int CompressedBytesPerSecond;
        public double CompressedMBytesPerSecond { get { return MathHelper.ConvertToMegabytes(CompressedBytesPerSecond); } }
        public double AverageCompressionRatio;
    }

    public class ProduceRequestStatistic
    {
        public DateTime CreatedOnUtc { get; private set; }
        public int MessageBytes { get; private set; }
        public int PayloadBytes { get; private set; }
        public int CompressedBytes { get; private set; }
        public double CompressionRatio { get; private set; }

        public ProduceRequestStatistic(int payloadBytes, int compressedBytes)
        {
            CreatedOnUtc = DateTime.UtcNow.RoundToSeconds();
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
    }
}
