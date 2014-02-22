using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using KafkaNet.Model;

namespace KafkaNet
{
    public class KafkaRouter
    {
        private readonly IPartitionSelector _partitionSelector;
        private readonly ConcurrentDictionary<int, KafkaConnection> _brokerConnectionIndex = new ConcurrentDictionary<int, KafkaConnection>();
        private readonly ConcurrentDictionary<string, Topic> _topicIndex = new ConcurrentDictionary<string, Topic>();
        private readonly List<KafkaConnection> _defaultConnections = new List<KafkaConnection>();

        public KafkaRouter(IEnumerable<Uri> kafkaServers, IPartitionSelector partitionSelector)
        {
            _partitionSelector = partitionSelector;

            //TODO add query parcing to get readtimeout string
            foreach (var uri in kafkaServers.Distinct())
            {
                _defaultConnections.Add(new KafkaConnection(uri));
            }
        }

        public List<KafkaConnection> DefaultBrokers { get { return _defaultConnections; } }

        public KafkaConnection GetConnection(string topic, int partitionId)
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

        public KafkaConnection SelectConnection(string topic, string key = null)
        {
            Topic metaTopic;
            if (_topicIndex.TryGetValue(topic, out metaTopic))
            {
                var partition = _partitionSelector.Select(topic, key, metaTopic.Partitions);
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
