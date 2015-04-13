using System.Collections.Generic;
using SimpleKafka.Protocol;

namespace SimpleKafka
{
    /// <summary>
    /// KafkaRequest represents a Kafka request messages as an object which can Encode itself into the appropriate 
    /// binary request and Decode any responses to that request.
    /// </summary>
    public interface IKafkaRequest
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
        /// The API Version used for this request
        /// </summary>
        short ApiVersion { get; }
        /// <summary>
        /// Id which will be echoed back by Kafka to correlate responses to this request.  Usually automatically assigned by driver.
        /// </summary>
        int CorrelationId { get; set; }
    }
}