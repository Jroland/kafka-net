using System;

namespace KafkaNet.Common
{
    public class FailCrcCheckException : Exception
    {
        public FailCrcCheckException(string message) : base(message) { }
    }

    public class ResponseTimeoutException : Exception
    {
        public ResponseTimeoutException(string message) : base(message) { }
    }

    public class InvalidPartitionIdSelectedException : Exception
    {
        public InvalidPartitionIdSelectedException(string message) : base(message) { }
    }

    public class ServerUnreachableException : Exception
    {
        public ServerUnreachableException(string message) : base(message) { }
    }
}
