using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using KafkaNet.Protocol;

namespace KafkaNet.Configuration
{
    public class Bus : IBus
    {
        private readonly IConsumerFactory _consumerFactory;
        private readonly IProducer _producer;
        private readonly IMetadataQueries _metadataQueries;

        public Bus(IProducer producer, IConsumerFactory consumerFactory, IMetadataQueries metadataQueries)
        {
            _producer = producer;
            _consumerFactory = consumerFactory;
            _metadataQueries = metadataQueries;
        }

        public Task<List<ProduceResponse>> SendMessageAsync(string topic, IEnumerable<Message> messages, short acks = 1, int timeoutMS = 1000, MessageCodec codec = MessageCodec.CodecNone)
        {
            return _producer.SendMessageAsync(topic, messages, acks, timeoutMS, codec);
        }

        public IEnumerable<Message> Consume(string topic, CancellationToken? token = null)
        {
            var consumer = _consumerFactory.GetConsumer(topic);
            return consumer.Consume(token);
        }

        public Topic GetTopic(string topic)
        {
            return _metadataQueries.GetTopic(topic);
        }

        public Task<List<OffsetResponse>> GetTopicOffsetAsync(string topic, int maxOffsets = 2, int time = -1)
        {
            return _metadataQueries.GetTopicOffsetAsync(topic, maxOffsets, time);
        }

        public void Dispose()
        {
            _producer.Dispose();
        }
    }
}