using System;
using System.IO;
using KafkaNet;

namespace kafka_tests.Fakes
{
    public class FakeStreamDecorator:IStreamDecorator
    {
        public int CalledCount { get; set; }

        public void Reset()
        {
            CalledCount = 0;
        }

        public Type WrapStreamType { get; private set; }

        public Stream WrapStream(Stream stream)
        {
            CalledCount++;
            return stream;
        }
    }
}
