using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using KafkaNet.Common;
using KafkaNet.Model;
using KafkaNet.Protocol;

namespace KafkaNet
{
    public class BrokerRouter : IDisposable
    {
        private readonly KafkaClientOptions _kafkaOptions;
        private readonly ConcurrentDictionary<int, KafkaConnection> _brokerConnectionIndex = new ConcurrentDictionary<int, KafkaConnection>();
        private readonly ConcurrentDictionary<string, Topic> _topicIndex = new ConcurrentDictionary<string, Topic>();
        private readonly List<KafkaConnection> _defaultConnections = new List<KafkaConnection>();

        public BrokerRouter(KafkaClientOptions kafkaOptions)
        {
            _kafkaOptions = kafkaOptions;
            _defaultConnections.AddRange(kafkaOptions.KafkaServerUri.Distinct()
                .Select(uri => new KafkaConnection(uri, kafkaOptions.ResponseTimeoutMs)));
        }

        public List<KafkaConnection> DefaultBrokers { get { return _defaultConnections; } }
        
        public async Task<BrokerRoute> SelectBrokerRouteAsync(string topic, string key = null)
        {
            var route = SelectConnectionFromCache(topic, key);

            //if connection does not exist, query for metadata update
            if (route == null)
            {
                await CycleDefaultBrokersForTopicMetadataAsync(topic);

                //with updated metadata try again
                route = SelectConnectionFromCache(topic, key);
            }

            return route;
        }

        public Task<MetadataResponse> GetTopicMetadataASync(params string[] topics)
        {
            //TODO get topics from cache, refresh the ones we dont have in cache
            return CycleDefaultBrokersForTopicMetadataAsync(topics);
        }
        
        private BrokerRoute SelectConnectionFromCache(string topic, string key = null)
        {
            Topic metaTopic;
            if (_topicIndex.TryGetValue(topic, out metaTopic))
            {
                var partition = _kafkaOptions.PartitionSelector.Select(topic, key, metaTopic.Partitions);
                KafkaConnection conn;
                if (_brokerConnectionIndex.TryGetValue(partition.LeaderId, out conn))
                {
                    return new BrokerRoute
                    {
                        Topic = topic,
                        PartitionId = partition.PartitionId,
                        Connection = conn
                    };
                }
            }

            return null;
        }
        
        private async Task<MetadataResponse> CycleDefaultBrokersForTopicMetadataAsync(params string[] topics)
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
                    //TODO log failed to query broker for metadata trying next
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
                _brokerConnectionIndex.AddOrUpdate(broker.BrokerId, i => new KafkaConnection(localBroker.Address, _kafkaOptions.ResponseTimeoutMs),
                                             (i, connection) => connection);
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
        public KafkaConnection Connection { get; set; }
    }
}
