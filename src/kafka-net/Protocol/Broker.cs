using System;
using KafkaNet.Common;

namespace KafkaNet.Protocol
{
    public class Broker
    {
        public int BrokerId { get; set; }
        public string Host { get; set; }
        public int Port { get; set; }
        public Uri Address { get { return new Uri(string.Format("http://{0}:{1}", Host, Port));} }

        public static Broker FromStream(BigEndianBinaryReader stream)
        {
            return new Broker
                {
                    BrokerId = stream.ReadInt32(),
                    Host = stream.ReadInt16String(),
                    Port = stream.ReadInt32()
                };
        }
    }
}
