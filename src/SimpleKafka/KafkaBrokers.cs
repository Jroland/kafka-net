using Serilog;
using SimpleKafka.Protocol;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace SimpleKafka
{
    public class KafkaBrokers : IDisposable
    {
        private readonly Random backoffGenerator = new Random();
        private readonly HashSet<Uri> brokers = new HashSet<Uri>();
        private readonly Dictionary<string, Partition[]> topicToPartitions = new Dictionary<string, Partition[]>(StringComparer.CurrentCultureIgnoreCase);

        private readonly Dictionary<int, KafkaConnection> connections = new Dictionary<int, KafkaConnection>();
        public KafkaConnection this[int brokerId]
        {
            get
            {
                var connection = connections.TryGetValue(brokerId);
                if (connection == null)
                {
                    throw new KeyNotFoundException("Failed to find broker " + brokerId);
                }
                return connection;
            }
        }

        public KafkaBrokers(params Uri[] addresses)
        {
            foreach (var address in addresses)
            {
                brokers.Add(address);
            }
        }


        private bool IsLeaderElectionTakingPlaceForTopicAndPartition(string topic, int partition)
        {
            var partitionsMap = topicToPartitions.TryGetValue(topic);
            if (partitionsMap == null)
            {
                return false;
            }
            else
            {
                var partitionInfo = partitionsMap[partition];
                return partitionInfo.LeaderId == -1;
            }
        }

        public async Task<bool> RefreshAsync(CancellationToken token)
        {
            if (brokers.Count == 0)
            {
                throw new InvalidOperationException("No brokers defined");
            }

            if (connections.Count > 0)
            {
                await TryToRefreshFromCurrentConnectionsAsync(token).ConfigureAwait(false);
            }

            if (connections.Count == 0)
            {
                await TryToInitialiseFromBrokersAsync(brokers, token).ConfigureAwait(false);
            }

            return (connections.Count > 0);
        }


        private async Task TryToInitialiseFromBrokersAsync(IEnumerable<Uri> brokers, CancellationToken token)
        {
            foreach (var broker in brokers)
            {
                try {
                    var newConnection = await KafkaConnectionFactory.CreateSimpleKafkaConnectionAsync(broker, token).ConfigureAwait(false);
                    var success = await TryToRefreshFromConnectionAsync(newConnection, token).ConfigureAwait(false);
                    if (success)
                    {
                        return;
                    } else
                    {
                        newConnection.Dispose();
                    }
                }
                catch (Exception ex)
                {
                    Console.WriteLine(ex);
                }
            }
        }

        internal void AddTopic(string topic)
        {
            if (!topicToPartitions.ContainsKey(topic))
            {
                topicToPartitions.Add(topic, null);
            }
        }


        private readonly Dictionary<string, int> offsetCoordinatorMap = new Dictionary<string, int>();
        public async Task<Dictionary<string, int>> BuildOffsetCoordinatorMapAsync(CancellationToken token, params string[] consumerGroups)
        {
            if (connections.Count == 0)
            {
                await RefreshAsync(token).ConfigureAwait(false);
            }

            foreach (var consumerGroup in consumerGroups)
            {
                var currentCoordinator = offsetCoordinatorMap.GetOrCreate(consumerGroup, () => -1);
                if (currentCoordinator == -1)
                {
                    var request = new ConsumerMetadataRequest { ConsumerGroup = consumerGroup };
                    var response = await connections.Values.First().SendRequestAsync(request, token).ConfigureAwait(false);
                    if (response.Error != ErrorResponseCode.NoError)
                    {
                        throw new InvalidOperationException("Failed to retrieve consumer offsets " + response.Error);
                    }
                    offsetCoordinatorMap[consumerGroup] = response.CoordinatorId;
                }
            }
            return offsetCoordinatorMap;
        }

        public async Task<Dictionary<string, Partition[]>> GetValidPartitionsForTopicsAsync(IEnumerable<string> topics, CancellationToken token)
        {
            while (true)
            {
                var result = await GetPartitionsForTopicsAsync(topics, token).ConfigureAwait(false);
                var anyWithElection = result.Values.Any(partitions => partitions.Any(partition => partition.LeaderId == -1));
                if (!anyWithElection)
                {
                    return result;
                }

                await BackoffAndRefresh(token).ConfigureAwait(false);
            }
        }

        public async Task<Dictionary<string, Partition[]>> GetPartitionsForTopicsAsync(IEnumerable<string> topics, CancellationToken token)
        {
            var result = new Dictionary<string, Partition[]>();
            foreach (var topic in topics)
            {
                var partitions = await GetPartitionsForTopicAsync(topic, token).ConfigureAwait(false);
                result[topic] = partitions;
            }
            return result;
        }

        public async Task<Partition[]> GetPartitionsForTopicAsync(string topic, CancellationToken token)
        {
            var partitions = GetPartitionsForTopic(topic);
            if (partitions == null)
            {
                AddTopic(topic);
                var refreshed = await RefreshAsync(token).ConfigureAwait(false);
                if (!refreshed)
                {
                    throw new KeyNotFoundException("Failed to refresh brokers");
                }
                partitions = GetPartitionsForTopic(topic);
                if (partitions == null)
                {
                    throw new KeyNotFoundException("Failed to find topic: " + topic);
                }
            }
            return partitions;
        }

        public async Task<int> GetLeaderForTopicAndPartitionAsync(string topic, int partitionId, CancellationToken token)
        {
            while (true)
            {
                var partitions = await GetPartitionsForTopicAsync(topic, token).ConfigureAwait(false);
                var partition = GetPartitionIfReady(topic, partitionId, partitions);

                if (partition != null)
                {
                    return partition.LeaderId;
                }
                else
                {
                    token.ThrowIfCancellationRequested();
                    await BackoffAndRefresh(token).ConfigureAwait(false);
                }
            }
        }

        private static Partition GetPartitionIfReady(string topic, int partitionId, Partition[] partitions)
        {
            if (partitionId >= partitions.Length)
            {
                throw new IndexOutOfRangeException("Topic " + topic + ", partition " + partitionId + " is too big. Only have " + partitions.Length + " partitions");
            }

            var partition = partitions[partitionId];
            if (partition.LeaderId == -1)
            {
                return null;
            }
            else
            {
                return partition;
            }
        }

        public async Task<Dictionary<int, Dictionary<Tuple<string, int>, T>>> BuildBrokerMapAsync<T>(
            CancellationToken token, Dictionary<string, Dictionary<int, T>> topicMap)
        {
            if (connections.Count == 0)
            {
                await RefreshAsync(token).ConfigureAwait(false);
            }

            var ready = false;
            while (!ready)
            {
                ready = true;
                foreach (var topicKvp in topicMap)
                {
                    var topic = topicKvp.Key;
                    var partitions = await GetPartitionsForTopicAsync(topic, token).ConfigureAwait(false);

                    foreach (var partitionKvp in topicKvp.Value)
                    {
                        var partitionNumber = partitionKvp.Key;
                        var partition = GetPartitionIfReady(topic, partitionNumber, partitions);
                        if (partition == null)
                        {
                            ready = false;
                            break;
                        }
                    }
                    if (!ready)
                    {
                        break;
                    }
                }

                if (!ready)
                {
                    await BackoffAndRefresh(token).ConfigureAwait(false);
                }
            }

            var brokerMap = new Dictionary<int, Dictionary<Tuple<string, int>, T>>();
            foreach (var topicKvp in topicMap)
            {
                var topic = topicKvp.Key;
                var partitions = GetPartitionsForTopic(topic);
                foreach (var partitionKvp in topicKvp.Value)
                {
                    var partitionNumber = partitionKvp.Key;
                    var partition = partitions[partitionNumber];
                    var brokerTopics = brokerMap.GetOrCreate(partition.LeaderId);
                    brokerTopics.Add(Tuple.Create(topic, partitionNumber), partitionKvp.Value);
                }
            }

            return brokerMap;
        }

        public async Task BackoffAndRefresh(CancellationToken token)
        {
            Log.Verbose("Waiting before trying again");
            await Task.Delay(backoffGenerator.Next(1000, 10000)).ConfigureAwait(false);
            var refreshed = await RefreshAsync(token).ConfigureAwait(false);
            if (!refreshed)
            {
                throw new KeyNotFoundException("Failed to refresh brokers");
            }
        }

        internal Partition[] GetPartitionsForTopic(string topic)
        {
            return topicToPartitions.TryGetValue(topic);
        }

        private async Task TryToRefreshFromCurrentConnectionsAsync(CancellationToken token)
        {
            foreach (var connectionKvp in connections.ToList())
            {
                var connection = connectionKvp.Value;
                var success = await TryToRefreshFromConnectionAsync(connection, token).ConfigureAwait(false);
                if (success)
                {
                    break;
                }
                else { 
                    connection.Dispose();
                    connections.Remove(connectionKvp.Key);
                }
            }

        }

        private async Task<bool> TryToRefreshFromConnectionAsync(KafkaConnection connection, CancellationToken token)
        {
            var request = new MetadataRequest
            {
                 Topics = topicToPartitions.Keys.ToList()
            };
            
            try {
                var response = await connection.SendRequestAsync(request, token).ConfigureAwait(false);
                await RefreshBrokersAsync(response.Brokers, token).ConfigureAwait(false);
                RefreshTopics(response.Topics);
                return true;
            }
            catch (Exception ex)
            {
                Log.Error(ex, "Error refreshing connection");
                return false;
            }
        }


        private void RefreshTopics(Topic[] topics)
        {
            var previousTopics = new HashSet<string>(topicToPartitions.Keys);


            foreach (var topic in topics)
            {
                if (topic.ErrorCode != ErrorResponseCode.NoError)
                {
                    Log.Information("Topic {topic} has error {error}", topic.Name, topic.ErrorCode);
                }
                else
                {
                    var currentPartitions = topicToPartitions.TryGetValue(topic.Name);
                    if ((currentPartitions == null) || (currentPartitions.Length != topic.Partitions.Length))
                    {
                        currentPartitions = new Partition[topic.Partitions.Length];
                        topicToPartitions[topic.Name] = currentPartitions;
                    }

                    foreach (var partition in topic.Partitions)
                    {
                        if (partition.ErrorCode != ErrorResponseCode.NoError)
                        {
                            Log.Verbose("Topic {topic} partition {partition} has error {error}", topic.Name, partition.PartitionId, partition.ErrorCode);
                        }
                        currentPartitions[partition.PartitionId] = partition;
                    }

                    previousTopics.Remove(topic.Name);
                }
            }

            foreach (var oldTopic in previousTopics)
            {
                topicToPartitions.Remove(oldTopic);
            }
        }

        private async Task RefreshBrokersAsync(Broker[] latestBrokers, CancellationToken token)
        {
            var previousBrokers = new HashSet<Uri>(brokers);
            var previousConnections = connections.ToDictionary(kvp => kvp.Key, kvp => kvp.Value);
            foreach (var broker in latestBrokers)
            {
                var uri = broker.Address;

                if (!brokers.Contains(uri)) {
                    brokers.Add(uri);
                } else
                {
                    previousBrokers.Remove(uri);
                }

                var currentConnection = connections.TryGetValue(broker.BrokerId);
                if (currentConnection == null)
                {
                    var newConnection = await KafkaConnectionFactory.CreateSimpleKafkaConnectionAsync(uri, token).ConfigureAwait(false);
                    connections.Add(broker.BrokerId, newConnection);
                } else
                {
                    previousConnections.Remove(broker.BrokerId);
                }
            }

            foreach (var oldBroker in previousBrokers)
            {
                brokers.Remove(oldBroker);
            }

            foreach (var oldConnectionKvp in previousConnections)
            {
                connections.Remove(oldConnectionKvp.Key);
                oldConnectionKvp.Value.Dispose();
            }
        }

        public override string ToString()
        {
            var sb = new StringBuilder();
            sb.Append("Brokers: ").Append(String.Join(", ", brokers)).AppendLine();

            sb.Append("Connections: ").Append(String.Join(", ",
                connections
                .OrderBy(kvp => kvp.Key)
                .Select(kvp => kvp.Key + ":" + kvp.Value.ServerEndpoint))).AppendLine();

            sb.Append("Partitions:").AppendLine();
            foreach (var topicKvp in topicToPartitions)
            {
                sb.Append(topicKvp.Key).Append(String.Join(", ",
                    topicKvp.Value.Select(p => p.PartitionId + "@" + p.LeaderId)))
                    .AppendLine();
                    
            }

            return sb.ToString();
        }

        #region IDisposable Support
        private bool disposedValue = false; // To detect redundant calls

        protected virtual void Dispose(bool disposing)
        {
            if (!disposedValue)
            {
                if (disposing)
                {
                    foreach (var connection in connections.Values)
                    {
                        connection.Dispose();
                    }
                }

                disposedValue = true;
            }
        }

        // This code added to correctly implement the disposable pattern.
        public void Dispose()
        {
            // Do not change this code. Put cleanup code in Dispose(bool disposing) above.
            Dispose(true);
        }
        #endregion
    }
}
