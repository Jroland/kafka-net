using System;
using System.Collections.Generic;
using System.Linq;
using KafkaNet.Model;
using KafkaNet.Protocol;
using Newtonsoft.Json;

namespace KafkaNet.Client
{
    public class JsonConsumer<T> : IDisposable
    {
        private readonly ConsumerOptions _options;
        private readonly Consumer _consumer;

        public JsonConsumer(ConsumerOptions options)
        {
            _options = options;
            _consumer = new Consumer(options);
        }

        public IEnumerable<Message<T>> Consume()
        {
            return _consumer.Consume()
                .Select(response => new Message<T>
                {
                    Meta = response.Meta,
                    Value = JsonConvert.DeserializeObject<T>(response.Value)
                });
        }

        public void Dispose()
        {
            using (_consumer) { }
        }
    }

    public class Message<T>
    {
        public MessageMetadata Meta { get; set; }
        public T Value { get; set; }
    }
}
