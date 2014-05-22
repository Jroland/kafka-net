namespace KafkaNet.Protocol
{
    using System;

    public class InsufficientDataException : Exception
    {
        public int ActualSize { get; private set; }

        public int ExpectedSize { get; private set; }

        public InsufficientDataException(int actualSize, int expectedSize)
        {
            this.ActualSize = actualSize;
            this.ExpectedSize = expectedSize;
        }
    }
}