using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Net.Http;
using System.Threading;
using System.Threading.Tasks;
using Couchbase;
using Couchbase.Configuration;
using KafkaNet.Common;

namespace StatisticsTestLoader
{
    public class SourcePropertyChanges : IRecordSource
    {
        private const string ApiTopic = "pcs_encoding_updates";

        private readonly CouchbaseClient _couch;
        private BlockingCollection<KafkaRecord> _dataQueue;

        public SourcePropertyChanges(string username, string password, params Uri[] couchbaseServers)
        {
            var config = new CouchbaseClientConfiguration
            {
                Bucket = username,
                BucketPassword = password,
            };

            Array.ForEach(couchbaseServers, uri => config.Urls.Add(uri));
            _couch = new CouchbaseClient(config);
            _couch.NodeFailed += node => Console.WriteLine(node.ToString());
        }

        public string Topic { get { return ApiTopic; } }
        public IEnumerable<KafkaRecord> Poll(long index)
        {
            return PollForChanges(index);
        }

        public int QueueCount { get { return _dataQueue.Count; } }

        private IEnumerable<KafkaRecord> PollForChanges(long index)
        {
            if (index <= 0) index = DateTime.UtcNow.AddYears(-1).Ticks;

            _dataQueue = new BlockingCollection<KafkaRecord>(100000);

            Task.Factory.StartNew(() => PopulateData(index, _dataQueue), CancellationToken.None,
                TaskCreationOptions.LongRunning, TaskScheduler.Default);

            return _dataQueue.GetConsumingEnumerable();
        }

        private void PopulateData(long index, BlockingCollection<KafkaRecord> data)
        {
            try
            {
                //load the large documet set from couchbase
                Console.WriteLine("Polling for couchbase changes...");
                var changes = _couch.GetView("Kafka", "by_versiontick", false)
                    .StartKey(index)
                    .Select(x => new KafkaRecord
                    {
                        Key = x.ItemId,
                        Offset = (long)x.ViewKey[0],
                        Topic = ApiTopic,
                    });

                //as fast as we can, pull the documents from CB and push to our output collection
                Parallel.ForEach(changes.Batch(100), new ParallelOptions { MaxDegreeOfParallelism = 20 },
                    (batch) =>
                    {
                        var temp = batch.ToList();
                        var records = _couch.Get(temp.Select(x => x.Key));

                        foreach (var change in temp)
                        {
                            if (records.ContainsKey(change.Key))
                            {
                                change.AddDocument(records[change.Key].ToString());
                                data.Add(change);
                            }
                        }
                    });

            }
            catch (Exception ex)
            {
                Console.WriteLine("Failed to populate _dataQueue: {0}", ex);
            }
            finally
            {
                data.CompleteAdding();
            }
        }
    }
}
