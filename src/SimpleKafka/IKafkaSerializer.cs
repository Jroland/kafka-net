using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace SimpleKafka
{
    public interface IKafkaSerializer<T>
    {
        byte[] Serialize(T value);

        T Deserialize(byte[] serialized);
    }
}
