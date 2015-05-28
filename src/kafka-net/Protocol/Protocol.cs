using System;
using System.IO;
using System.IO.Compression;

namespace KafkaNet.Protocol
{
    /// <summary>
    /// Extension methods which allow compression of byte arrays
    /// </summary>
    public static class Compression
    {
        public static byte[] Zip(byte[] bytes)
        {
            using (var destination = new MemoryStream())
            using (var gzip = new GZipStream(destination, CompressionLevel.Fastest, false))
            {
                gzip.Write(bytes, 0, bytes.Length);
                gzip.Flush();
                gzip.Close();
                return destination.ToArray();
            }
        }

        public static byte[] Unzip(byte[] bytes)
        {
            using (var source = new MemoryStream(bytes))
            using (var destination = new MemoryStream())
            using (var gzip = new GZipStream(source, CompressionMode.Decompress, false))
            {
                gzip.CopyTo(destination);
                gzip.Flush();
                gzip.Close();
                return destination.ToArray();
            }
        }
    }

    /// <summary>
    /// Enumeration of numeric codes that the ApiKey in the request can take for each request types. 
    /// </summary>
    public enum ApiKeyRequestType
    {
        Produce = 0,
        Fetch = 1,
        Offset = 2,
        MetaData = 3,
        OffsetCommit = 8,
        OffsetFetch = 9,
        ConsumerMetadataRequest = 10
    }

    /// <summary>
    /// Enumeration of error codes that might be returned from a Kafka server
    /// </summary>
    public enum ErrorResponseCode : short
    {
        /// <summary>
        /// No error--it worked!
        /// </summary>
        NoError = 0,

        /// <summary>
        /// An unexpected server error
        /// </summary>
        Unknown = -1,

        /// <summary>
        /// The requested offset is outside the range of offsets maintained by the server for the given topic/partition.
        /// </summary>
        OffsetOutOfRange = 1,

        /// <summary>
        /// This indicates that a message contents does not match its CRC
        /// </summary>
        InvalidMessage = 2,

        /// <summary>
        /// This request is for a topic or partition that does not exist on this broker.
        /// </summary>
        UnknownTopicOrPartition = 3,

        /// <summary>
        /// The message has a negative size
        /// </summary>
        InvalidMessageSize = 4,

        /// <summary>
        /// This error is thrown if we are in the middle of a leadership election and there is currently no leader for this partition and hence it is unavailable for writes.
        /// </summary>
        LeaderNotAvailable = 5,

        /// <summary>
        /// This error is thrown if the client attempts to send messages to a replica that is not the leader for some partition. It indicates that the clients metadata is out of date.
        /// </summary>
        NotLeaderForPartition = 6,

        /// <summary>
        /// This error is thrown if the request exceeds the user-specified time limit in the request.
        /// </summary>
        RequestTimedOut = 7,

        /// <summary>
        /// This is not a client facing error and is used only internally by intra-cluster broker communication.
        /// </summary>
        BrokerNotAvailable = 8,

        /// <summary>
        /// If replica is expected on a broker, but is not.
        /// </summary>
        ReplicaNotAvailable = 9,

        /// <summary>
        /// The server has a configurable maximum message size to avoid unbounded memory allocation. This error is thrown if the client attempt to produce a message larger than this maximum.
        /// </summary>
        MessageSizeTooLarge = 10,

        /// <summary>
        /// Internal error code for broker-to-broker communication.
        /// </summary>
        StaleControllerEpochCode = 11,

        /// <summary>
        /// If you specify a string larger than configured maximum for offset metadata
        /// </summary>
        OffsetMetadataTooLargeCode = 12,

        /// <summary>
        /// The broker returns this error code for an offset fetch request if it is still loading offsets (after a leader change for that offsets topic partition).
        /// </summary>
        OffsetsLoadInProgressCode = 14,

        /// <summary>
        /// The broker returns this error code for consumer metadata requests or offset commit requests if the offsets topic has not yet been created.
        /// </summary>
        ConsumerCoordinatorNotAvailableCode = 15,

        /// <summary>
        /// The broker returns this error code if it receives an offset fetch or commit request for a consumer group that it is not a coordinator for.
        /// </summary>
        NotCoordinatorForConsumerCode = 16
    }

    /// <summary>
    /// Protocol specific constants
    /// </summary>
    public struct ProtocolConstants
    {
        /// <summary>
        ///  The lowest 2 bits contain the compression codec used for the message. The other bits should be set to 0.
        /// </summary>
        public static byte AttributeCodeMask = 0x03;
    }

    /// <summary>
    /// Enumeration which specifies the compression type of messages
    /// </summary>
    public enum MessageCodec
    {
        CodecNone = 0x00,
        CodecGzip = 0x01,
        CodecSnappy = 0x02
    }

    #region Exceptions...
    public class FailCrcCheckException : ApplicationException
    {
        public FailCrcCheckException(string message, params object[] args) : base(string.Format(message, args)) { }
    }

    public class ResponseTimeoutException : ApplicationException
    {
        public ResponseTimeoutException(string message, params object[] args) : base(string.Format(message, args)) { }
    }

    public class InvalidPartitionException : ApplicationException
    {
        public InvalidPartitionException(string message, params object[] args) : base(string.Format(message, args)) { }
    }

    public class ServerDisconnectedException : ApplicationException
    {
        public ServerDisconnectedException(string message, params object[] args) : base(string.Format(message, args)) { }
    }

    public class ServerUnreachableException : ApplicationException
    {
        public ServerUnreachableException(string message, params object[] args) : base(string.Format(message, args)) { }
    }

    public class InvalidTopicMetadataException : ApplicationException
    {
        public InvalidTopicMetadataException(ErrorResponseCode code, string message, params object[] args)
            : base(string.Format(message, args))
        {
            ErrorResponseCode = code;
        }
        public ErrorResponseCode ErrorResponseCode { get; private set; }
    }

    public class LeaderNotFoundException : ApplicationException
    {
        public LeaderNotFoundException(string message, params object[] args) : base(string.Format(message, args)) { }
    }

    public class UnresolvedHostnameException : ApplicationException
    {
        public UnresolvedHostnameException(string message, params object[] args) : base(string.Format(message, args)) { }
    }

    public class InvalidMetadataException : ApplicationException
    {
        public int ErrorCode { get; set; }
        public InvalidMetadataException(string message, params object[] args) : base(string.Format(message, args)) { }
    }

    public class OffsetOutOfRangeException : ApplicationException
    {
        public Fetch FetchRequest { get; set; }
        public OffsetOutOfRangeException(string message, params object[] args) : base(string.Format(message, args)) { }
    }

    public class BufferUnderRunException : ApplicationException
    {
        public int MessageHeaderSize { get; set; }
        public int RequiredBufferSize { get; set; }

        public BufferUnderRunException(int messageHeaderSize, int requiredBufferSize)
            : base("The size of the message from Kafka exceeds the provide buffer size.")
        {
            MessageHeaderSize = messageHeaderSize;
            RequiredBufferSize = requiredBufferSize;
        }
    }

    public class KafkaApplicationException : ApplicationException
    {
        public int ErrorCode { get; set; }
        public KafkaApplicationException(string message, params object[] args) : base(string.Format(message, args)) { }
    }
    #endregion


}
