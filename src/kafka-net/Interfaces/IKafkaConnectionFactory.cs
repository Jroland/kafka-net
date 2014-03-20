using KafkaNet.Protocol;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace KafkaNet
{
    public static class KafkaNetConfiguration
    {
        public static Func<Uri, int, IKafkaLog, IKafkaConnection> ConnectionFactory = (uri, timeout, log) => new KafkaConnection(uri, timeout, log);
        public static Func<Topic, string, Partition> PartitionSelector = (topic, key) => new DefaultPartitionSelector().Select(topic, key);
        public static Func<IKafkaLog> Log = () => new DefaultTraceLog();
        public static Func<MessageCodec, byte[], byte[]> CompressFunction ;
        public static Func<MessageCodec, byte[], byte[]> DeCompressFunction;
    }

    public interface IKafkaConnectionFactory
    {
        IKafkaConnection Create(Uri kafkaAddress, int responseTimeoutMs, IKafkaLog log);
    }

    public class DefaultKafkaConnectionFactory : IKafkaConnectionFactory
    {
        public IKafkaConnection Create(Uri kafkaAddress, int responseTimeoutMs, IKafkaLog log)
        {
            return new KafkaConnection(kafkaAddress, responseTimeoutMs, log);
        }
    }
}
