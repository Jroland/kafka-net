using System;
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

    public interface IKafkaRequest
    {
        string ClientId { get; set; }
        int CorrelationId { get; set; }
        ProtocolEncoding EncodingKey { get; }
    }

    public class ProduceRequest : BaseRequest, IKafkaRequest
    { 
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
        public Int16 Acks = 1;
        /// <summary>
        /// Collection of payloads to post to kafka
        /// </summary>
        public List<Payload> Payload = new List<Payload>();
    }

    public class FetchRequest : BaseRequest, IKafkaRequest
    {
        /// <summary>
        /// Indicates the type of kafka encoding this request is
        /// </summary>
        public ProtocolEncoding EncodingKey { get { return ProtocolEncoding.Fetch; } }
    }

    public class MetadataRequest : BaseRequest, IKafkaRequest
    {
        /// <summary>
        /// Indicates the type of kafka encoding this request is
        /// </summary>
        public ProtocolEncoding EncodingKey { get { return ProtocolEncoding.MetaData; } }

        /// <summary>
        /// The list of topics to get metadata for.
        /// </summary>
        public List<string> Topics { get; set; }
    }

    public class MetadataResponse
    {
        public MetadataResponse()
        {
            Brokers = new List<Broker>();
            Topics = new List<Topic>();
        }

        public List<Broker> Brokers { get; set; }
        public List<Topic> Topics { get; set; }
    }

    public abstract class BaseRequest
    {
        private string _clientId = "Kafka-Net";

        /// <summary>
        /// Descriptive name of the source of the messages sent to kafka
        /// </summary>
        public string ClientId { get { return _clientId; } set { _clientId = value; } }

        public int CorrelationId { get; set; }
    }
}
