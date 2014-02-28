using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using KafkaNet.Model;
using KafkaNet.Protocol;
using log4net;
using Newtonsoft.Json;

namespace KafkaNet.Client
{
    public class JsonProducer
    {
        private Producer _producer;

        public JsonProducer(KafkaOptions options)
        {
            _producer = new Producer(options);
        }

        public Task<List<ProduceResponse>> Publish<T>(string topic, IEnumerable<T> messages,  Int16 acks = 1, int timeoutMS = 1000)
        {
            return _producer.SendMessageAsync(topic, ConvertToKafkaMessage(messages), acks, timeoutMS);
        }

        private IEnumerable<Message> ConvertToKafkaMessage<T>(IEnumerable<T> messages)
        {
            var hasKey = typeof(T).GetProperty("Key", typeof(string)) != null;

            return messages.Select(m =>
            {
                return new Message
                    {
                        Key = hasKey ? GetKeyPropertyValue(m) : null,
                        Value = JsonConvert.SerializeObject(m)
                    };
            });
        }

        private string GetKeyPropertyValue<T>(T message)
        {
            if (message == null) return null;
            var info = message.GetType().GetProperty("Key", typeof(string));

            if (info == null) return null;
            return (string)info.GetValue(message);
        }
    }
}
