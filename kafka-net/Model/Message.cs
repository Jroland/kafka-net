using System;
using System.Collections.Generic;


namespace KafkaNet.Model
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
        /// <summary>
        /// The offset of the message within the kafka log.
        /// </summary>
        public long Offset { get; set; }
        public byte MagicNumber { get; set; }
        /// <summary>
        /// Attribute value outside message body used for added codec/compression info.
        /// </summary>
        public byte Attribute { get; set; }
        /// <summary>
        /// Key value used for routing message to partitions.
        /// </summary>
        public string Key { get; set; }
        /// <summary>
        /// The message body contents.  Can contain compress message set.
        /// </summary>
        public string Value { get; set; }
    }

    public interface IKafkaRequest
    {
        string ClientId { get; set; }
        int CorrelationId { get; set; }
        ApiKeyRequestType ApiKey { get; }
    }

    public class ProduceRequest : BaseRequest, IKafkaRequest
    {
        /// <summary>
        /// Indicates the type of kafka encoding this request is
        /// </summary>
        public ApiKeyRequestType ApiKey { get { return ApiKeyRequestType.Produce; } }
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

    public class ProduceResponse
    {
        /// <summary>
        /// The topic the offset came from.
        /// </summary>
        public string Topic { get; set; }
        /// <summary>
        /// The partition the offset came from.
        /// </summary>
        public int PartitionId { get; set; }
        /// <summary>
        /// The offset number to commit as completed.
        /// </summary>
        public Int16 Error { get; set; }
        public long Offset { get; set; }
    }

    public class FetchRequest : BaseRequest, IKafkaRequest
    {
        /// <summary>
        /// Indicates the type of kafka encoding this request is
        /// </summary>
        public ApiKeyRequestType ApiKey { get { return ApiKeyRequestType.Fetch; } }

        public int MaxWaitTime = 100;
        public int MinBytes = 4096;
        public List<Fetch> Fetches { get; set; }
    }

    public class Fetch
    {
        public Fetch()
        {
            MaxBytes = 4096;
        }

        public string Topic { get; set; }

        public int PartitionId { get; set; }

        public long Offset { get; set; }

        public int MaxBytes { get; set; }
    }

    public class FetchResponse
    {
        public string Topic { get; set; }
        public int PartitionId { get; set; }
        public Int16 Error { get; set; }
        public long HighWaterMark { get; set; }
        public List<Message> Messages { get; set; }
    }

    public class OffsetCommitRequest : BaseRequest, IKafkaRequest
    {
        public ApiKeyRequestType ApiKey { get { return ApiKeyRequestType.OffsetCommit; } }
        public string ConsumerGroup { get; set; }
        public List<OffsetCommit> OffsetCommits { get; set; }
    }

    public class OffsetCommit
    {
        /// <summary>
        /// The topic the offset came from.
        /// </summary>
        public string Topic { get; set; }
        /// <summary>
        /// The partition the offset came from.
        /// </summary>
        public int PartitionId { get; set; }
        /// <summary>
        /// The offset number to commit as completed.
        /// </summary>
        public long Offset { get; set; }
        /// <summary>
        /// Descriptive metadata about this commit.
        /// </summary>
        public string Metadata { get; set; }
    }

    public class OffsetCommitResponse
    {
        public string Topic { get; set; }
        public int PartitionId { get; set; }
        public Int16 Error { get; set; }
    }

    public class OffsetRequest : BaseRequest, IKafkaRequest
    {
        public ApiKeyRequestType ApiKey { get { return ApiKeyRequestType.Offset; } }
        public List<Offset> Offsets { get; set; }
    }

    public class Offset
    {
        public Offset()
        {
            Time = -1;
            MaxOffsets = 1;
        }
        public string Topic { get; set; }
        public int PartitionId { get; set; }
        /// <summary>
        /// Used to ask for all messages before a certain time (ms). There are two special values. 
        /// Specify -1 to receive the latest offsets and -2 to receive the earliest available offset. 
        /// Note that because offsets are pulled in descending order, asking for the earliest offset will always return you a single element.
        /// </summary>
        public long Time { get; set; }
        public int MaxOffsets { get; set; }
    }

    public class OffsetResponse
    {
        public string Topic { get; set; }
        public int PartitionId { get; set; }
        public Int16 Error { get; set; }
        public List<long> Offsets { get; set; }
    }

    public class MetadataRequest : BaseRequest, IKafkaRequest
    {
        /// <summary>
        /// Indicates the type of kafka encoding this request is
        /// </summary>
        public ApiKeyRequestType ApiKey { get { return ApiKeyRequestType.MetaData; } }

        /// <summary>
        /// The list of topics to get metadata for.
        /// </summary>
        public List<string> Topics { get; set; }
    }

    public class MetadataResponse
    {
        public int CorrelationId { get; set; }
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
    }
}
