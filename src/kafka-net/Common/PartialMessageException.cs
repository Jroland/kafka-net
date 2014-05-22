namespace KafkaNet.Common
{
    using System;

    public class PartialMessageException : Exception
    {
        public long ExpectedBytes { get; private set; }

        public long ActualBytes { get; private set; }

        public PartialMessageException(string message, long expectedBytes, long actualBytes):base(message)
        {
            this.ExpectedBytes = expectedBytes;
            this.ActualBytes = actualBytes;
        }
    }
}