using System;
using SimpleKafka.Common;

namespace SimpleKafka.Protocol
{
    public class Broker
    {
        public int BrokerId { get; set; }
        public string Host { get; set; }
        public int Port { get; set; }
        public Uri Address { get { return new Uri(string.Format("http://{0}:{1}", Host, Port));} }

        public static Broker Decode(ref KafkaDecoder decoder)
        {
            return new Broker
            {
                BrokerId = decoder.ReadInt32(),
                Host = decoder.ReadInt16String(),
                Port = decoder.ReadInt32()
            };
        }
    }
}
