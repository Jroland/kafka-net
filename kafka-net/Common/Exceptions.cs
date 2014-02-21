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
}
