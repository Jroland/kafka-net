using System;

namespace KafkaNet.Common
{
    public class FailCrcCheckException : Exception
    {
        public FailCrcCheckException(string message) : base(message) { }
    }
}
