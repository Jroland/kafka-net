using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using KafkaNet.Model;
using KafkaNet.Protocol;

namespace KafkaNet
{
    /// <summary>
    /// This class provides an abstraction from querying multiple Kafka servers for Metadata details and caching this data.
    /// 
    /// All metadata queries are cached lazily.  If metadata from a topic does not exist in cache it will be queried for using
    /// the default brokers provided in the constructor.  Each Uri will be queried to get metadata information in tern until a
    /// response is received.  It is recommended therefore to provide more than one Kafka Uri as this API will be able to to get
    /// metadata information even if one of the Kafka servers goes down.
    /// 
    /// TODO : there is currently no way to update the cache once it is in there in this class
    /// </summary>
    public class BrokerRouter : IBrokerRouter
    {
        private readonly object _threadLock = new object();
        private readonly KafkaOptions _kafkaOptions;
        private readonly ConcurrentDictionary<int, IKafkaConnection> _brokerConnectionIndex = new ConcurrentDictionary<int, IKafkaConnection>();
        private readonly ConcurrentDictionary<string, Topic> _topicIndex = new ConcurrentDictionary<string, Topic>();
        private readonly List<IKafkaConnection> _defaultConnections = new List<IKafkaConnection>();

        public BrokerRouter(KafkaOptions kafkaOptions)
        {
            _kafkaOptions = kafkaOptions;
            _defaultConnections
                .AddRange(kafkaOptions.KafkaServerUri.Distinct()
                .Select(uri => _kafkaOptions.KafkaConnectionFactory.Create(uri, _kafkaOptions.ResponseTimeoutMs, _kafkaOptions.Log)));
        }

        /// <summary>
        /// Select a broker for a specific topic and partitionId.
        /// </summary>
        /// <param name="topic">The topic name to select a broker for.</param>
        /// <param name="partitionId">The exact partition to select a broker for.</param>
        /// <returns>A broker route for the given partition of the given topic.</returns>
        /// <remarks>
        /// This function does not use any selector criteria.  If the given partitionId does not exist an exception will be thrown.
        /// </remarks>
        /// <exception cref="InvalidTopicMetadataException">Thrown if the returned metadata for the given topic is invalid or missing.</exception>
        /// <exception cref="InvalidPartitionException">Thrown if the give partitionId does not exist for the given topic.</exception>
        /// <exception cref="ServerUnreachableException">Thrown if none of the Default Brokers can be contacted.</exception>
        public async Task<BrokerRoute> SelectBrokerRouteAsync(string topic, int partitionId)
        {
            var cachedTopic = await GetTopicMetadataAsync(topic);

            if (cachedTopic.Count <= 0)
                throw new InvalidTopicMetadataException(string.Format("The Metadata is invalid as it returned no data for the given topic:{0}", topic));

            var topicMetadata = cachedTopic.First();

            var partition = topicMetadata.Partitions.FirstOrDefault(x => x.PartitionId == partitionId);
            if (partition == null) throw new InvalidPartitionException(string.Format("The topic:{0} does not have a partitionId:{1} defined.", topic, partitionId));

            return GetCachedRoute(topicMetadata.Name, partition);
        }

        /// <summary>
        /// Select a broker for a given topic using the IPartitionSelector function.
        /// </summary>
        /// <param name="topic">The topic to retreive a broker route for.</param>
        /// <param name="key">The key used by the IPartitionSelector to collate to a consistent partition. Null value means key will be ignored in selection process.</param>
        /// <returns>A broker route for the given topic.</returns>
        /// <exception cref="InvalidTopicMetadataException">Thrown if the returned metadata for the given topic is invalid or missing.</exception>
        /// <exception cref="ServerUnreachableException">Thrown if none of the Default Brokers can be contacted.</exception>
        public async Task<BrokerRoute> SelectBrokerRouteAsync(string topic, string key = null)
        {
            //get topic either from cache or server.
            var cachedTopic = await GetTopicMetadataAsync(topic);

            if (cachedTopic.Count <= 0)
                throw new InvalidTopicMetadataException(string.Format("The Metadata is invalid as it returned no data for the given topic:{0}", topic));

            return SelectConnectionFromCache(cachedTopic.First(), key);
        }

        /// <summary>
        /// Returns Topic metadata for each topic requested. 
        /// </summary>
        /// <param name="topics">Collection of topids to request metadata for.</param>
        /// <returns>List of Topics as provided by Kafka.</returns>
        /// <remarks>The topic metadata will by default check the cache first and then request metadata from the server if it does not exist in cache.</remarks>
        public Task<List<Topic>> GetTopicMetadataAsync(params string[] topics)
        {
            //TODO : need to stop the thread race here so faking async for the moment
            var missingTopics = topics.Where(x => _topicIndex.ContainsKey(x) == false).ToList();

            if (missingTopics.Count > 0)
            {
                //if any are missing we need to lock so we dont race here
                lock (_threadLock)
                {
                    missingTopics = topics.Where(x => _topicIndex.ContainsKey(x) == false).ToList();
                    if (missingTopics.Count > 0)
                    {
                        //Cycle method will throw if any of the topics cannot be found.
                        CycleDefaultBrokersForTopicMetadataAsync(missingTopics).Wait();
                    }
                }
            }

            var tcs = new TaskCompletionSource<List<Topic>>();
            tcs.SetResult(topics.Select(GetCachedTopic).ToList());
            return tcs.Task;
        }

        private Topic GetCachedTopic(string topic)
        {
            Topic cachedTopic;
            return _topicIndex.TryGetValue(topic, out cachedTopic) ? cachedTopic : null;
        }

        private List<Topic> RefreshTopicMetadata(IEnumerable<string> topics)
        {
            lock (_threadLock)
            {
                //we have not found the collection of brokers get this from 
                if (_defaultConnections.Count > 0)
                {

                }
            }
        }

        private Tuple<List<Topic>, List<string>> GetHitAndMissCachedTopic(IEnumerable<string> topics)
        {
            var missingTopics = new List<string>();

            var topicMetadata = new List<Topic>();
            foreach (var topic in topics)
            {
                var cachedTopic = GetCachedTopic(topic);
                if (cachedTopic == null)
                    missingTopics.Add(topic);
                else
                    topicMetadata.Add(cachedTopic);
            }

            return new Tuple<List<Topic>, List<string>>(topicMetadata, missingTopics);
        }

        private BrokerRoute SelectConnectionFromCache(Topic topic, string key = null)
        {
            if (topic == null) throw new ArgumentNullException("topic");
            var partition = _kafkaOptions.PartitionSelector.Select(topic, key);
            return GetCachedRoute(topic.Name, partition);
        }

        private BrokerRoute GetCachedRoute(string topic, Partition partition)
        {
            IKafkaConnection conn;
            if (_brokerConnectionIndex.TryGetValue(partition.LeaderId, out conn))
            {
                return new BrokerRoute
                {
                    Topic = topic,
                    PartitionId = partition.PartitionId,
                    Connection = conn
                };
            }

            //TODO when we cant find a leader then maybe we need to refresh our cache.  Handle here?
            throw new LeaderNotFoundException(string.Format("Lead broker cannot be found for parition: {0}, leader: {1}", partition.PartitionId, partition.LeaderId));
        }

        private async Task<MetadataResponse> CycleDefaultBrokersForTopicMetadataAsync(IEnumerable<string> topics)
        {
            var request = new MetadataRequest { Topics = topics.ToList() };

            //try each default broker until we find one that is available
            foreach (var conn in DefaultBrokers)
            {
                try
                {
                    var response = await conn.SendAsync(request);
                    if (response != null && response.Count > 0)
                    {
                        var metadataResponse = response.First();
                        UpdateInternalMetadataCache(metadataResponse);
                        return metadataResponse;
                    }
                }
                catch (Exception ex)
                {
                    _kafkaOptions.Log.WarnFormat("Failed to contact Kafka server={0}.  Trying next default server.  Exception={1}", conn.KafkaUri, ex);
                }
            }

            throw new ServerUnreachableException(
                    string.Format(
                        "Unable to query for metadata from any of the default Kafka servers.  At least one provided server must be available.  Server list: {0}",
                        string.Join(", ", _kafkaOptions.KafkaServerUri.Select(x => x.ToString()))));
        }

        private void UpdateInternalMetadataCache(MetadataResponse metadata)
        {
            foreach (var broker in metadata.Brokers)
            {
                var localBroker = broker;
                _brokerConnectionIndex.AddOrUpdate(broker.BrokerId,
                    i =>
                        {
                            var existing = _defaultConnections.FirstOrDefault(x => x.KafkaUri.Host == broker.Host && x.KafkaUri.Port == broker.Port);
                            return existing ?? _kafkaOptions.KafkaConnectionFactory.Create(localBroker.Address, _kafkaOptions.ResponseTimeoutMs, _kafkaOptions.Log);
                        },
                    (i, connection) =>
                        {
                            //if a connection changes for a broker close old connection and create a new one
                            if (connection.KafkaUri == localBroker.Address) return connection;
                            _kafkaOptions.Log.WarnFormat("Broker:{0} Uri changed from:{1} to {2}", localBroker.BrokerId, connection.KafkaUri, localBroker.Address);
                            using (connection) { return _kafkaOptions.KafkaConnectionFactory.Create(localBroker.Address, _kafkaOptions.ResponseTimeoutMs, _kafkaOptions.Log); }
                        });
            }

            foreach (var topic in metadata.Topics)
            {
                var localTopic = topic;
                _topicIndex.AddOrUpdate(topic.Name, s => localTopic, (s, existing) => localTopic);
            }
        }

        public void Dispose()
        {
            _defaultConnections.ForEach(conn => { using (conn) { } });
            _brokerConnectionIndex.Values.ToList().ForEach(conn => { using (conn) { } });
        }
    }

    public class BrokerRoute
    {
        public string Topic { get; set; }
        public int PartitionId { get; set; }
        public IKafkaConnection Connection { get; set; }
    }
}
