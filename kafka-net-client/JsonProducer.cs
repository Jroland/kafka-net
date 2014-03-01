using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using KafkaNet.Protocol;
using Newtonsoft.Json;

namespace KafkaNet.Client
{
    public class JsonProducer : IDisposable
    {
        private readonly Producer _producer;

        public JsonProducer(IBrokerRouter brokerRouter)
        {
            _producer = new Producer(brokerRouter);
        }

        public Task<List<ProduceResponse>> Publish<T>(string topic, IEnumerable<T> messages, Int16 acks = 1, int timeoutMS = 1000) where T : class 
        {
            return _producer.SendMessageAsync(topic, ConvertToKafkaMessage(messages), acks, timeoutMS);
        }

        private static IEnumerable<Message> ConvertToKafkaMessage<T>(IEnumerable<T> messages) where T : class 
        {
            var hasKey = typeof(T).GetProperty("Key", typeof(string)) != null;

            return messages.Select(m => new Message
                {
                    Key = hasKey ? GetKeyPropertyValue(m) : null,
                    Value = JsonConvert.SerializeObject(m)
                });
        }

        private static string GetKeyPropertyValue<T>(T message) where T : class 
        {
            if (message == null) return null;
            var info = message.GetType().GetProperty("Key", typeof(string));

            if (info == null) return null;
            return (string)info.GetValue(message);
        }

        public void Dispose()
        {
            using (_producer) { }
        }
    }
}
