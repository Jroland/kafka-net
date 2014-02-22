using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using KafkaNet.Model;
using KafkaNet.Protocol;

namespace KafkaNet
{
    public class KafkaRouter
    {
        public delegate void ResponseReceived(byte[] payload);
        public event ResponseReceived OnResponseReceived;

        private readonly KafkaClientOptions _kafkaOptions;
        private readonly ConcurrentDictionary<int, KafkaConnection> _brokerConnectionIndex = new ConcurrentDictionary<int, KafkaConnection>();
        private readonly ConcurrentDictionary<string, Topic> _topicIndex = new ConcurrentDictionary<string, Topic>();
        private readonly List<KafkaConnection> _defaultConnections = new List<KafkaConnection>();

        public KafkaRouter(KafkaClientOptions kafkaOptions)
        {
            _kafkaOptions = kafkaOptions;

            foreach (var uri in kafkaOptions.KafkaServerUri.Distinct())
            {
                _defaultConnections.Add(new KafkaConnection(uri));
            }
        }

        public List<KafkaConnection> DefaultBrokers { get { return _defaultConnections; } }

        public KafkaConnection GetBrokerConnection(string topic, int partitionId)
        {
            Topic metaTopic;
            if (_topicIndex.TryGetValue(topic, out metaTopic))
            {
                var partition = metaTopic.Partitions.FirstOrDefault(x => x.PartitionId == partitionId);
                if (partition == null) return null;

                KafkaConnection conn;
                if (_brokerConnectionIndex.TryGetValue(partition.LeaderId, out conn)) return conn;
            }

            return null;
        }

        public KafkaConnection SelectBrokerConnection(string topic, string key = null)
        {
            Topic metaTopic;
            if (_topicIndex.TryGetValue(topic, out metaTopic))
            {
                var partition = _kafkaOptions.PartitionSelector.Select(topic, key, metaTopic.Partitions);
                KafkaConnection conn;
                if (_brokerConnectionIndex.TryGetValue(partition.LeaderId, out conn)) return conn;
            }

            return null;
        }

        public void RefreshMetadata(MetadataResponse metadata)
        {
            foreach (var broker in metadata.Brokers)
            {
                var localBroker = broker;
                _brokerConnectionIndex.AddOrUpdate(broker.BrokerId, i => new KafkaConnection(localBroker.Address),
                                             (i, connection) => connection);
            }

            foreach (var topic in metadata.Topics)
            {
                var localTopic = topic;
                _topicIndex.AddOrUpdate(topic.Name, s => localTopic, (s, existing) => localTopic);
            }
        }
    }
}
