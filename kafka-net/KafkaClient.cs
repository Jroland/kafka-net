using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Kafka;

namespace Kafka
{
    public class KafkaClient
    {
        private KafkaConnection _conn;
        private Protocol _protocol;

        public KafkaClient(Uri connection)
        {
            _conn = new KafkaConnection(connection);
            _protocol = new Protocol();
        }

        public void Send(ProduceRequest request)
        {
            _conn.SendAsync(_protocol.EncodeProduceRequest(request)).Wait();
        }
    }
}
