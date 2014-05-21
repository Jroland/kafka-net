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
        public BrokerRoute SelectBrokerRoute(string topic, int partitionId)
        {
            var cachedTopic = GetTopicMetadata(topic);

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
        public BrokerRoute SelectBrokerRoute(string topic, string key = null)
        {
            //get topic either from cache or server.
            var cachedTopic = GetTopicMetadata(topic);

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
        public List<Topic> GetTopicMetadata(params string[] topics)
        {
            var hitMissTopic = GetHitAndMissCachedTopic(topics);
            if (hitMissTopic.Item2.Count > 0)
            {
                //if any are missing we need to lock so we dont race here
                lock (_threadLock)
                {
                    var missingTopics = hitMissTopic.Item2.Where(x => _topicIndex.ContainsKey(x) == false).ToList();
                    if (missingTopics.Count > 0)
                    {
                        RefreshTopicMetadata(missingTopics);
                    }
                }

                hitMissTopic.Item1.AddRange(hitMissTopic.Item2.Select(GetCachedTopic).Where(x => x != null));
            }

            return hitMissTopic.Item1;
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

        private Topic GetCachedTopic(string topic)
        {
            Topic cachedTopic;
            return _topicIndex.TryGetValue(topic, out cachedTopic) ? cachedTopic : null;
        }

        private BrokerRoute SelectConnectionFromCache(Topic topic, string key = null)
        {
            if (topic == null) throw new ArgumentNullException("topic");
            var partition = _kafkaOptions.PartitionSelector.Select(topic, key);
            return GetCachedRoute(topic.Name, partition);
        }

        private BrokerRoute GetCachedRoute(string topic, Partition partition)
        {
            var route = TryGetRouteFromCache(topic, partition);

            //leader could not be found, refresh the broker information and try one more time
            if (route == null)
            {
                RefreshTopicMetadata(new[] { topic });
                route = TryGetRouteFromCache(topic, partition);
            }

            if (route != null) return route;

            throw new LeaderNotFoundException(string.Format("Lead broker cannot be found for parition: {0}, leader: {1}", partition.PartitionId, partition.LeaderId));
        }

        private BrokerRoute TryGetRouteFromCache(string topic, Partition partition)
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

            return null;
        }

        /// <summary>
        /// Refreshes topic metadata
        /// </summary>
        /// <param name="topics">
        /// Topics to be refreshed
        /// </param>
        /// <remarks>
        /// This method's accessibility was changed from private to public so that refresh can be called in case of broker failure
        /// </remarks>
        public void RefreshTopicMetadata(IEnumerable<string> topics)
        {
            //we retreived a collection of brokers, get this from the default
            if (_defaultConnections.Count > 0)
            {
                CycleConnectionsForTopicMetadataAsync(_defaultConnections, topics);
                if (_brokerConnectionIndex.Values.Count > 0)
                {
                    _defaultConnections.ForEach(x => { using (x) { } });
                    _defaultConnections.Clear();
                }
                return;
            }

            //cycle the brokers to get metadata update
            if (_brokerConnectionIndex.Values.Count > 0)
            {
                CycleConnectionsForTopicMetadataAsync(_brokerConnectionIndex.Values, topics);
                return;
            }
        }

        private MetadataResponse CycleConnectionsForTopicMetadataAsync(IEnumerable<IKafkaConnection> connections, IEnumerable<string> topics)
        {
            var request = new MetadataRequest { Topics = topics.ToList() };

            //try each default broker until we find one that is available
            foreach (var conn in connections)
            {
                try
                {
                    var response = conn.SendAsync(request).Result;
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
                        return _kafkaOptions.KafkaConnectionFactory.Create(localBroker.Address, _kafkaOptions.ResponseTimeoutMs, _kafkaOptions.Log);
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

    #region BrokerRoute Class...
    public class BrokerRoute
    {
        public string Topic { get; set; }
        public int PartitionId { get; set; }
        public IKafkaConnection Connection { get; set; }

        #region Equals Override...
        protected bool Equals(BrokerRoute other)
        {
            return string.Equals(Topic, other.Topic) && PartitionId == other.PartitionId;
        }

        public override int GetHashCode()
        {
            unchecked
            {
                return ((Topic != null ? Topic.GetHashCode() : 0) * 397) ^ PartitionId;
            }
        }

        public override bool Equals(object obj)
        {
            if (ReferenceEquals(null, obj)) return false;
            if (ReferenceEquals(this, obj)) return true;
            if (obj.GetType() != this.GetType()) return false;
            return Equals((BrokerRoute)obj);
        }
        #endregion
    } 
    #endregion
}
