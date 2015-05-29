using System;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using KafkaNet.Common;

namespace StatisticsTestLoader
{
    /// <summary>
    /// Note: This project is for testing large documents being pushed and compressed to a kafka server.
    /// This is not currently a generic loader program and requires a certain infrastructure to run.  It
    /// was created as a quick prototype to test out statistic tracking of long running, large data flow.
    /// </summary>
    class Program
    {

        private static DestinationKafka _kafka;
        private static SourcePropertyChanges _propertySource;
        private static bool _interrupted;

        static void Main(string[] args)
        {
            var configuration = new Configuration();

            _kafka = new DestinationKafka(configuration.KafkaUrl);

            _propertySource = new SourcePropertyChanges(configuration.CacheUsername, configuration.CachePassword,
                configuration.PropertyCacheUrl);

            Task.Run(() => StartPolling(_propertySource, _kafka));

            Console.WriteLine("Running...");
            Console.ReadLine();
            _interrupted = true;
            Console.WriteLine("Quitting...");
        }

        private static void StartPolling(IRecordSource source, DestinationKafka kafka)
        {
            while (_interrupted == false)
            {
                try
                {
                    var index = kafka.GetStoredOffset(source.Topic);
                    foreach (var batchEnumerable in source.Poll(index).Batch(500))
                    {
                        var batch = batchEnumerable.ToList();

                        kafka.PostBatchAsync(batch);
                    }

                    Thread.Sleep(TimeSpan.FromMinutes(1));
                }
                catch (Exception ex)
                {
                    Console.WriteLine("Exception occured trying to post to topic:{0}.  Expcetion:{1}", source.Topic, ex);
                }
            }
        }
    }
}
