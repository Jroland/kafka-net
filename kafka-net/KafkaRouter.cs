using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using KafkaNet.Model;

namespace KafkaNet
{
    public class KafkaRouter
    {
        private readonly ConcurrentDictionary<int, Broker> _brokerIndex = new ConcurrentDictionary<int, Broker>();
        private readonly ConcurrentDictionary<string, Topic> _topicIndex = new ConcurrentDictionary<string, Topic>();
        private readonly ConcurrentDictionary<Uri, KafkaConnection>  _connectionIndex= new ConcurrentDictionary<Uri, KafkaConnection>();

        public KafkaRouter(params Uri[] kafkaServers)
        {
            //TODO add query parcing to get readtimeout string
            foreach (var uri in kafkaServers.Distinct())
            {
                _connectionIndex.TryAdd(uri, new KafkaConnection(uri));
            }
        }

        
    }
}
