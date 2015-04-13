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

        public async Task<Dictionary<int, Dictionary<Tuple<string, int>, T>>> BuildBrokerMapAsync<T>(CancellationToken token, Dictionary<string, Dictionary<int, T>> topicMap)
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

                    foreach (var partitionKvp in topicKvp.Value)
                    {
                        var partitionNumber = partitionKvp.Key;
                        if (partitionNumber >= partitions.Length)
                        {
                            throw new IndexOutOfRangeException("Topic " + topic + ", partition " + partitionNumber + " is too big. Only have " + partitions.Length + " partitions");
                        }

                        var partition = partitions[partitionNumber];
                        if (partition.LeaderId == -1)
                        {
                            Log.Information("Topic {topic}, partition {partition} has no leader, waiting", topic, partitionNumber);
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
                    Log.Verbose("Waiting before trying again");
                    await Task.Delay(backoffGenerator.Next(1000, 10000)).ConfigureAwait(false);
                    var refreshed = await RefreshAsync(token).ConfigureAwait(false);
                    if (!refreshed)
                    {
                        throw new KeyNotFoundException("Failed to refresh brokers");
                    }
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
                    var brokerTopics = brokerMap.FindOrCreate(partition.LeaderId);
                    brokerTopics.Add(Tuple.Create(topic, partitionNumber), partitionKvp.Value);
                }
            }

            return brokerMap;
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


        private void RefreshTopics(List<Topic> topics)
        {
            var previousTopics = new HashSet<string>(topicToPartitions.Keys);


            foreach (var topic in topics)
            {
                if (topic.ErrorCode != (short)ErrorResponseCode.NoError)
                {
                    Log.Information("Topic {topic} has error {error}", topic.Name, (ErrorResponseCode)topic.ErrorCode);
                }
                else
                {
                    var currentPartitions = topicToPartitions.TryGetValue(topic.Name);
                    if ((currentPartitions == null) || (currentPartitions.Length != topic.Partitions.Count))
                    {
                        currentPartitions = new Partition[topic.Partitions.Count];
                        topicToPartitions[topic.Name] = currentPartitions;
                    }

                    foreach (var partition in topic.Partitions)
                    {
                        if (partition.ErrorCode != (short)ErrorResponseCode.NoError)
                        {
                            Log.Verbose("Topic {topic} partition {partition} has error {error}", topic.Name, partition.PartitionId, (ErrorResponseCode)partition.ErrorCode);
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

        private async Task RefreshBrokersAsync(List<Broker> latestBrokers, CancellationToken token)
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
