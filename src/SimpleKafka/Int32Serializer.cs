using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace SimpleKafka
{
    public class Int32Serializer : IKafkaSerializer<int>
    {
        public byte[] Serialize(int value)
        {
            var buffer = new byte[4];
            var encoder = new KafkaEncoder(buffer);
            encoder.Write(value);
            return buffer;
        }

        public int Deserialize(byte[] serialized)
        {
            return new KafkaDecoder(serialized).ReadInt32();
        }
    }
}
