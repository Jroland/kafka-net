using System.Collections.Generic;
using KafkaNet.Protocol;

namespace KafkaNet
{
    /// <summary>
    /// KafkaRequest represents a Kafka request messages as an object which can Encode itself into the appropriate 
    /// binary request and Decode any responses to that request.
    /// </summary>
    /// <typeparam name="T">The type of the KafkaResponse expected back from the request.</typeparam>
    public interface IKafkaRequest<out T>
    {
        /// <summary>
        /// Indicates this request should wait for a response from the broker
        /// </summary>
        bool ExpectResponse { get; }
        /// <summary>
        /// Descriptive name used to identify the source of this request. 
        /// </summary>
        string ClientId { get; set; }
        /// <summary>
        /// Id which will be echoed back by Kafka to correlate responses to this request.  Usually automatically assigned by driver.
        /// </summary>
        int CorrelationId { get; set; }
        /// <summary>
        /// Enum identifying the specific type of request message being represented.
        /// </summary>
        ApiKeyRequestType ApiKey { get; }
        /// <summary>
        /// Encode this request into the Kafka wire protocol.
        /// </summary>
        /// <returns>Byte[] representing the binary wire protocol of this request.</returns>
        KafkaDataPayload Encode();
        /// <summary>
        /// Decode a response payload from Kafka into an enumerable of T responses. 
        /// </summary>
        /// <param name="payload">Buffer data returned by Kafka servers.</param>
        /// <returns></returns>
        IEnumerable<T> Decode(byte[] payload);
    }
}