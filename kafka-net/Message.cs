using System.Collections.Generic;
using System.Text;
using Kafka.Common;

namespace Kafka
{
    /// <summary>
    /// Payload represents a collection of messages to be posted to a specified Topic on specified Partition.
    /// </summary>
    public class Payload
    {
        public string Topic { get; set; }
        public int Partition { get; set; }
        public List<Message> Messages { get; set; }
    }

    /// <summary>
    /// Message represents the data from a single event occurance.
    /// </summary>
    public class Message
    {
        public byte MagicNumber { get; set; }
        public byte Attributes { get; set; }
        public string Key { get; set; }
        public string Value { get; set; }
    }

    public class ProduceRequest
    {
        /// <summary>
        /// Descriptive name of the source of the messages sent to kafka
        /// </summary>
        public string ClientId = "Kafka-Net";

        public int CorrelationId { get; set; }
        /// <summary>
        /// Indicates the type of kafka encoding this request is
        /// </summary>
        public ProtocolEncoding EncodingKey { get { return ProtocolEncoding.Produce; } }

        /// <summary>
        /// Time kafka will wait for requested ack level before returning.
        /// </summary>
        public int TimeoutMS = 1000;
        /// <summary>
        /// Level of ack required by kafka.  0 immediate, 1 written to leader, 2+ replicas synced, -1 all replicas
        /// </summary>
        public int Acks = 0;
        /// <summary>
        /// Collection of payloads to post to kafka
        /// </summary>
        public List<Payload> Payload = new List<Payload>();
    }
}
