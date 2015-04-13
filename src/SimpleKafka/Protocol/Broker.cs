using System;
using SimpleKafka.Common;

namespace SimpleKafka.Protocol
{
    public class Broker
    {
        public readonly int BrokerId;
        public readonly string Host;
        public readonly int Port;
        public Uri Address { get { return new Uri(string.Format("http://{0}:{1}", Host, Port));} }

        private Broker(int brokerId, string host, int port)
        {
            this.BrokerId = brokerId;
            this.Host = host;
            this.Port = port;
        }

        internal static Broker Decode(KafkaDecoder decoder)
        {
            return new Broker(decoder.ReadInt32(), decoder.ReadString(), decoder.ReadInt32());
        }
    }
}
