using KafkaNet.Model;
using System;

namespace KafkaNet
{
    public interface IKafkaConnectionFactory
    {
        /// <summary>
        /// Create a new KafkaConnection.
        /// </summary>
        /// <param name="endpoint">The specific KafkaEndpoint of the server to connect to.</param>
        /// <param name="responseTimeoutMs">The amount of time to wait for a message response to be received after sending a message to Kafka</param>
        /// <param name="log">Logging interface used to record any log messages created by the connection.</param>
        /// <param name="maximumReconnectionTimeout">The maximum time to wait when backing off on reconnection attempts.</param>
        /// <returns>IKafkaConnection initialized to connecto to the given endpoint.</returns>
        IKafkaConnection Create(KafkaEndpoint endpoint, TimeSpan responseTimeoutMs, IKafkaLog log, TimeSpan? maximumReconnectionTimeout = null);

        /// <summary>
        /// Resolves a generic Uri into a uniquely identifiable KafkaEndpoint.
        /// </summary>
        /// <param name="kafkaAddress">The address to the kafka server to resolve.</param>
        /// <param name="log">Logging interface used to record any log messages created by the Resolving process.</param>
        /// <returns>KafkaEndpoint with resolved IP and Address.</returns>
        KafkaEndpoint Resolve(Uri kafkaAddress, IKafkaLog log);
    }
}
