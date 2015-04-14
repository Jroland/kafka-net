using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace SimpleKafka
{
    public class NullSerializer<T> : IKafkaSerializer<T>
    {
        public T Deserialize(byte[] serialized)
        {
            return default(T);
        }

        public byte[] Serialize(T value)
        {
            return null;
        }
    }
}
