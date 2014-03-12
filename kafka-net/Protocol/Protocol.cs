using System;

namespace KafkaNet
{
    public enum ApiKeyRequestType
    {
        Produce = 0,
        Fetch = 1,
        Offset = 2,
        MetaData = 3,
        LeaderAndIsr = 4,
        StopReplica = 5,
        OffsetCommit = 6,
        OffsetFetch = 7
    }

    public enum ErrorResponseCode
    {
        NoError = 0,
        Unknown = -1,
        OffsetOutOfRange = 1,
        InvalidMessage = 2,
        UnknownTopicOrPartition = 3,
        InvalidMessageSize = 4,
        LeaderNotAvailable = 5,
        NotLeaderForPartition = 6,
        RequestTimedOut = 7,
        BrokerNotAvailable = 8,
        ReplicaNotAvailable = 9,
        MessageSizeTooLarge = 10,
        StaleControllerEpochCode = 11,
        OffsetMetadataTooLargeCode = 12
    }

    public struct ProtocolConstants
    {
        public static byte AttributeCodeMask = 0x03;
    }

    public enum MessageCodec
    {
        CodecNone = 0x00,
        CodecGzip = 0x01,
        CodecSnappy = 0x02
    }
    
    public class FailCrcCheckException : Exception
    {
        public FailCrcCheckException(string message) : base(message) { }
    }

    public class ResponseTimeoutException : Exception
    {
        public ResponseTimeoutException(string message) : base(message) { }
    }

    public class InvalidPartitionException : Exception
    {
        public InvalidPartitionException(string message) : base(message) { }
    }

    public class ServerUnreachableException : Exception
    {
        public ServerUnreachableException(string message) : base(message) { }
    }

    public class InvalidTopicMetadataException : Exception
    {
        public InvalidTopicMetadataException(string message) : base(message) { }
    }

    public class LeaderNotFoundException : Exception
    {
        public LeaderNotFoundException(string message) : base(message) { }
    }
}
