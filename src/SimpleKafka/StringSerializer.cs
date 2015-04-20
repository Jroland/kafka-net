using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace SimpleKafka
{
    public class StringSerializer : IKafkaSerializer<string>
    {
        public string Deserialize(byte[] serialized)
        {
            return Encoding.UTF8.GetString(serialized);
        }

        public byte[] Serialize(string value)
        {
            return Encoding.UTF8.GetBytes(value);
        }
    }
}
