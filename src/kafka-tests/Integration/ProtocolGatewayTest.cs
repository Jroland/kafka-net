
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading.Tasks;
using kafka_tests.Helpers;
using KafkaNet;
using KafkaNet.Common;
using KafkaNet.Model;
using KafkaNet.Protocol;
using NUnit.Framework;

namespace kafka_tests.Integration
{
    [TestFixture]
    [Category("Integration")]
    public class ProtocolGatewayTest
    {
        private readonly KafkaOptions Options = new KafkaOptions(IntegrationConfig.IntegrationUri);
        [Test]
        public async Task ProtocolGateway()
        {
            int partitionId = 0;
            var router = new BrokerRouter(Options);

            var producer = new Producer(router);
            string messge1 = Guid.NewGuid().ToString();
       var respose=   await  producer.SendMessageAsync(IntegrationConfig.IntegrationTopic, new[] {new Message(messge1)},1,null,MessageCodec.CodecNone,partitionId);
            var offset=respose.FirstOrDefault().Offset;
        
            ProtocolGateway protocolGateway=new ProtocolGateway(IntegrationConfig.IntegrationUri);
               var fetch = new Fetch
                            {
                                Topic = IntegrationConfig.IntegrationTopic,
                                PartitionId = partitionId,
                                Offset = offset,
                                MaxBytes = 32000,
                            };

                            var fetches = new List<Fetch> { fetch };

                            var fetchRequest = new FetchRequest
                                {
                                    MaxWaitTime = 1000,
                                    MinBytes =10,
                                    Fetches = fetches
                                };


            var r=await protocolGateway.SendProtocolRequest(fetchRequest, IntegrationConfig.IntegrationTopic, partitionId);
          //  var r1 = await protocolGateway.SendProtocolRequest(fetchRequest, IntegrationConfig.IntegrationTopic, partitionId);
            Assert.IsTrue( r.Messages.FirstOrDefault().Value.ToUtf8String() == messge1);

        }
    }
}
