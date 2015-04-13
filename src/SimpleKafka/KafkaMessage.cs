using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace SimpleKafka
{
    public static class KafkaMessage
    {
        public static KafkaMessage<TKey, TValue> Create<TKey, TValue>(string topic, TKey key, TValue value)
        {
            return new KafkaMessage<TKey, TValue>(topic, key, value);
        }
    }

    public class KafkaMessage<TKey,TValue>
    {
        private readonly string topic;
        public string Topic {  get { return topic; } }
        private readonly TKey key;
        public TKey Key {  get { return key; } }
        private readonly TValue value;
        public TValue Value {  get { return value; } }

        public KafkaMessage(string topic, TKey key, TValue value)
        {
            this.topic = topic;
            this.key = key;
            this.value = value;
        }


    }
}
