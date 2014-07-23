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
        
        public Bus(IProducer producer, IConsumerFactory consumerFactory)
        {
            _producer = producer;
            _consumerFactory = consumerFactory;
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

        public void Dispose()
        {
            _producer.Dispose();
        }
    }
}