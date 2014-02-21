using System;
using KafkaNet.Common;

namespace KafkaNet.Model
{
    public class Broker
    {
        public int BrokerId { get; set; }
        public string Host { get; set; }
        public int Port { get; set; }
        public Uri Address { get { return new Uri(string.Format("http://{0}:{1}", Host, Port));} }

        public static Broker FromStream(ReadByteStream stream)
        {
            return new Broker
                {
                    BrokerId = stream.ReadInt(),
                    Host = stream.ReadInt16String(),
                    Port = stream.ReadInt()
                };
        }
    }
}
