using System;
using KafkaNet.Common;

namespace KafkaNet.Protocol
{
    public abstract class BaseRequest
    {
        /// <summary>
        /// From Documentation: 
        /// The replica id indicates the node id of the replica initiating this request. Normal client consumers should always specify this as -1 as they have no node id. 
        /// Other brokers set this to be their own node id. The value -2 is accepted to allow a non-broker to issue fetch requests as if it were a replica broker for debugging purposes.
        /// 
        /// Kafka Protocol implementation:
        /// https://cwiki.apache.org/confluence/display/KAFKA/A+Guide+To+The+Kafka+Protocol
        /// </summary>
        protected const int ReplicaId = -1;
        protected const Int16 ApiVersion = 0;
        private string _clientId = "Kafka-Net";
        private int _correlationId = 1;

        /// <summary>
        /// Descriptive name of the source of the messages sent to kafka
        /// </summary>
        public string ClientId { get { return _clientId; } set { _clientId = value; } }

        /// <summary>
        /// Value supplied will be passed back in the response by the server unmodified. 
        /// It is useful for matching request and response between the client and server. 
        /// </summary>
        public int CorrelationId { get { return _correlationId; } set { _correlationId = value; } }

        /// <summary>
        /// Flag which tells the broker call to expect a response for this request.
        /// </summary>
        public virtual bool ExpectResponse { get { return true; } }

        /// <summary>
        /// Encode the common head for kafka request.
        /// </summary>
        /// <returns>KafkaMessagePacker with header populated</returns>
        /// <remarks>Format: (hhihs) </remarks>
        public static KafkaMessagePacker EncodeHeader<T>(IKafkaRequest<T> request)
        {
            return new KafkaMessagePacker()
                 .Pack(((Int16)request.ApiKey))
                 .Pack(ApiVersion)
                 .Pack(request.CorrelationId)
                 .Pack(request.ClientId, StringPrefixEncoding.Int16);
        }
    }
}