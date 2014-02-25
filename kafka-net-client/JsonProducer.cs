using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using KafkaNet.Model;
using KafkaNet.Protocol;
using log4net;

namespace KafkaNet.Client
{
    public class JsonProducer
    {
        public JsonProducer(KafkaOptions options)
        {
            
        }

        public Task<List<ProduceResponse>> Publish<T>(string topic, params T[] messages)
        {
            throw new NotImplementedException();
        }


    }
}
