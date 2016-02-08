using System;
using System.IO;

namespace KafkaNet
{
    public interface IStreamDecorator
    {
        Type WrapStreamType { get; }
        Stream WrapStream(Stream stream);
    }
}