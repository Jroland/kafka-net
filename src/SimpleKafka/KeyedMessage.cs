using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace SimpleKafka
{
    public static class KeyedMessage
    {
        public static KeyedMessage<object, object, TValue> Create<TValue>(string topic, TValue value)
        {
            return new KeyedMessage<object, object, TValue>(topic, value);
        }

        public static KeyedMessage<TKey, TKey, TValue> Create<TKey, TValue>(string topic, TKey key, TValue value)
        {
            return new KeyedMessage<TKey, TKey, TValue>(topic, key, key, value);
        }

        public static KeyedMessage<TKey, TPartitionKey, TValue> Create<TKey, TPartitionKey, TValue>(string topic, TKey key, TPartitionKey partitionKey, TValue value)
        {
            return new KeyedMessage<TKey, TPartitionKey, TValue>(topic, key, partitionKey, value);
        }
    }


    public class KeyedMessage<TKey,TPartitionKey,TValue>
    {
        public readonly string Topic;
        public readonly TKey Key;
        public readonly TPartitionKey PartitionKey;
        public readonly TValue Value;
        public readonly bool HasKey;

        internal KeyedMessage(string topic, TValue value)
        {
            this.Topic = topic;
            this.Value = value;
        }

        internal KeyedMessage(string topic, TKey key, TPartitionKey partitionKey, TValue value)
        {
            this.Topic = topic;
            this.Value = value;
            this.Key = key;
            this.PartitionKey = partitionKey;
            this.HasKey = true;
        }
    }
}
