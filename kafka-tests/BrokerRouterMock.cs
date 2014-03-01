using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using KafkaNet;
using KafkaNet.Model;
using KafkaNet.Protocol;
using Moq;
using Ninject.MockingKernel.Moq;

namespace kafka_tests
{
    public class BrokerRouterMock
    {
        private const string TestTopic = "UnitTest";

        private readonly MoqMockingKernel _kernel;
        private Mock<IPartitionSelector> _partitionSelectorMock;
        private Mock<IKafkaConnection> _connMock0;
        private Mock<IKafkaConnection> _connMock1;
        private Mock<IKafkaConnectionFactory> _factoryMock;

        public Mock<IKafkaConnection> BrokerConn0 { get { return _connMock0; } }
        public Mock<IKafkaConnection> BrokerConn1 { get { return _connMock1; } }
        public Mock<IKafkaConnectionFactory> KafkaConnectionFactory { get { return _factoryMock; } }

        public BrokerRouterMock(MoqMockingKernel kernel)
        {
            _kernel = kernel;

            //setup mock IKafkaConnection
            _connMock0 = _kernel.GetMock<IKafkaConnection>();
            _connMock1 = _kernel.GetMock<IKafkaConnection>();
            _factoryMock = _kernel.GetMock<IKafkaConnectionFactory>();
            _factoryMock.Setup(x => x.Create(It.Is<Uri>(uri => uri.Port == 1), It.IsAny<int>(), It.IsAny<IKafkaLog>())).Returns(() => _connMock0.Object);
            _factoryMock.Setup(x => x.Create(It.Is<Uri>(uri => uri.Port == 2), It.IsAny<int>(), It.IsAny<IKafkaLog>())).Returns(() => _connMock1.Object);
        }
        
        public IBrokerRouter CreateBrokerRouter()
        {
            return CreateBrokerRouter(CreateMetaResponse());
        }

        public IBrokerRouter CreateBrokerRouter(MetadataResponse response)
        {
            var router = new BrokerRouter(new KafkaNet.Model.KafkaOptions
            {
                KafkaServerUri = new List<Uri> { new Uri("http://localhost:1"), new Uri("http://localhost:2") },
                KafkaConnectionFactory = _factoryMock.Object
            });

            _connMock0.Setup(x => x.SendAsync(It.IsAny<IKafkaRequest<MetadataResponse>>()))
                      .Returns(() => Task.Factory.StartNew(() => new List<MetadataResponse> { response }));

            return router;
        }

        public MetadataResponse CreateMetaResponse()
        {
            return new MetadataResponse
                {
                    CorrelationId = 1,
                    Brokers = new List<Broker>
                        {
                            new Broker
                                {
                                    Host = "localhost",
                                    Port = 1,
                                    BrokerId = 0
                                },
                            new Broker
                                {
                                    Host = "localhost",
                                    Port = 2,
                                    BrokerId = 1
                                },
                        },
                    Topics = new List<Topic>
                        {
                            new Topic
                                {
                                    ErrorCode = 0,
                                    Name = TestTopic,
                                    Partitions = new List<Partition>
                                        {
                                            new Partition
                                                {
                                                    ErrorCode = 0,
                                                    Isrs = new List<int> {1},
                                                    PartitionId = 0,
                                                    LeaderId = 0,
                                                    Replicas = new List<int> {1},
                                                },
                                            new Partition
                                                {
                                                    ErrorCode = 0,
                                                    Isrs = new List<int> {1},
                                                    PartitionId = 1,
                                                    LeaderId = 1,
                                                    Replicas = new List<int> {1},
                                                }
                                        }

                                }
                        }
                };
        }
    }
}
