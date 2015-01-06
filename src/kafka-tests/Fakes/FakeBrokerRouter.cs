using System;
using System.Collections.Generic;
using System.Net;
using KafkaNet;
using KafkaNet.Model;
using KafkaNet.Protocol;
using kafka_tests.Fakes;
using System.Threading;
using NSubstitute;

namespace kafka_tests
{
    public class FakeBrokerRouter
    {
        public const string TestTopic = "UnitTest";

        private int _offset0;
        private int _offset1;
        private readonly FakeKafkaConnection _fakeConn0;
        private readonly FakeKafkaConnection _fakeConn1;
        private readonly IKafkaConnectionFactory _mockKafkaConnectionFactory;

        public FakeKafkaConnection BrokerConn0 { get { return _fakeConn0; } }
        public FakeKafkaConnection BrokerConn1 { get { return _fakeConn1; } }
        public IKafkaConnectionFactory KafkaConnectionMockKafkaConnectionFactory { get { return _mockKafkaConnectionFactory; } }

        public Func<MetadataResponse> MetadataResponse = () => DefaultMetadataResponse();

        public IPartitionSelector PartitionSelector = new DefaultPartitionSelector();

        public FakeBrokerRouter()
        {
            //setup mock IKafkaConnection
            _fakeConn0 = new FakeKafkaConnection(new Uri("http://localhost:1"));
            _fakeConn0.ProduceResponseFunction = () => new ProduceResponse { Offset = _offset0++, PartitionId = 0, Topic = TestTopic };
            _fakeConn0.MetadataResponseFunction = () => MetadataResponse();
            _fakeConn0.OffsetResponseFunction = () => new OffsetResponse { Offsets = new List<long> { 0, 99 }, PartitionId = 0, Topic = TestTopic };
            _fakeConn0.FetchResponseFunction = () => { Thread.Sleep(500); return null; };

            _fakeConn1 = new FakeKafkaConnection(new Uri("http://localhost:2"));
            _fakeConn1.ProduceResponseFunction = () => new ProduceResponse { Offset = _offset1++, PartitionId = 1, Topic = TestTopic };
            _fakeConn1.MetadataResponseFunction = () => MetadataResponse();
            _fakeConn1.OffsetResponseFunction = () => new OffsetResponse { Offsets = new List<long> { 0, 100 }, PartitionId = 1, Topic = TestTopic };
            _fakeConn1.FetchResponseFunction = () => { Thread.Sleep(500); return null; };

            _mockKafkaConnectionFactory = Substitute.For<IKafkaConnectionFactory>();
            _mockKafkaConnectionFactory.Create(Arg.Is<KafkaEndpoint>(e => e.Endpoint.Port == 1), Arg.Any<TimeSpan>(), Arg.Any<IKafkaLog>()).Returns(_fakeConn0);
            _mockKafkaConnectionFactory.Create(Arg.Is<KafkaEndpoint>(e => e.Endpoint.Port == 2), Arg.Any<TimeSpan>(), Arg.Any<IKafkaLog>()).Returns(_fakeConn1);
            _mockKafkaConnectionFactory.Resolve(Arg.Any<Uri>(), Arg.Any<IKafkaLog>())
                                       .Returns(info => new KafkaEndpoint
                                        {
                                            Endpoint = new IPEndPoint(IPAddress.Parse("127.0.0.1"), ((Uri)info[0]).Port),
                                            ServeUri = ((Uri)info[0])
                                        });
        }

        public IBrokerRouter Create()
        {
            return new BrokerRouter(new KafkaNet.Model.KafkaOptions
            {
                KafkaServerUri = new List<Uri> { new Uri("http://localhost:1"), new Uri("http://localhost:2") },
                KafkaConnectionFactory = _mockKafkaConnectionFactory,
                PartitionSelector = PartitionSelector
            });
        }

        public static MetadataResponse DefaultMetadataResponse()
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
