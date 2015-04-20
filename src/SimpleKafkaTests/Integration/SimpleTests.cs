using NUnit.Framework;
using SimpleKafka;
using SimpleKafka.Common;
using SimpleKafka.Protocol;
using SimpleKafkaTests.Helpers;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace SimpleKafkaTests.Integration
{
    [TestFixture]
    [Category("Integration")]
    class SimpleTests
    {
        private KafkaTestCluster testCluster;

        [OneTimeSetUp]
        public void BuildTestCluster()
        {
            testCluster = new KafkaTestCluster("server.home", 1);
        }

        [OneTimeTearDown]
        public void DestroyTestCluster()
        {
            testCluster.Dispose();
            testCluster = null;
        }

        [Test]
        public async Task TestProducingWorksOk()
        {
            using (var temporaryTopic = testCluster.CreateTemporaryTopic())
            using (var connection = await KafkaConnectionFactory.CreateSimpleKafkaConnectionAsync(testCluster.CreateBrokerUris()[0]).ConfigureAwait(true))
            {
                var request = new ProduceRequest
                {
                    Acks = 1,
                    TimeoutMS = 10000,
                    Payload = new List<Payload>
                     {
                         new Payload
                         {
                              Topic = temporaryTopic.Name,
                              Partition = 0,
                              Codec = MessageCodec.CodecNone,
                              Messages = new List<Message>
                              {
                                  new Message(Guid.NewGuid().ToString())
                              }
                         }
                     }
                };

                var response = await connection.SendRequestAsync(request, CancellationToken.None).ConfigureAwait(true);
                Assert.That(response, Has.Count.EqualTo(1));
                var first = response.First();
                Assert.That(first.Error, Is.EqualTo(ErrorResponseCode.NoError));
            }
        }

        [Test]
        public async Task TestFetchingWorksOk()
        {
            using (var temporaryTopic = testCluster.CreateTemporaryTopic())
            using (var connection = await KafkaConnectionFactory.CreateSimpleKafkaConnectionAsync(testCluster.CreateBrokerUris()[0]).ConfigureAwait(true))
            {
                var request = new FetchRequest
                {
                    MaxWaitTime = 0,
                    MinBytes = 1000,
                    Fetches = new List<Fetch>
                     {
                         new Fetch
                         {
                              Topic = temporaryTopic.Name,
                              PartitionId = 0,
                              MaxBytes = 1024,
                              Offset = 0
                         }
                     }
                };

                var response = await connection.SendRequestAsync(request, CancellationToken.None).ConfigureAwait(true);
                Assert.That(response, Has.Count.EqualTo(1));
                var first = response.First();
                Assert.That(first.Error, Is.EqualTo(ErrorResponseCode.NoError));
                Assert.That(first.Messages, Has.Count.EqualTo(0));
            }
        }

        [Test]
        public async Task TestListingAllTopicsWorksOk()
        {
            using (var temporaryTopic = testCluster.CreateTemporaryTopic())
            using (var connection = await KafkaConnectionFactory.CreateSimpleKafkaConnectionAsync(testCluster.CreateBrokerUris()[0]).ConfigureAwait(true))
            {
                var request = new MetadataRequest { };
                var response = await connection.SendRequestAsync(request, CancellationToken.None).ConfigureAwait(true);
                Assert.That(response, Is.Not.Null);
                Assert.That(response.Brokers, Has.Length.EqualTo(1));
                Assert.That(response.Topics, Has.Length.EqualTo(1));
                Assert.That(response.Topics[0].Name, Is.EqualTo(temporaryTopic.Name));
                Assert.That(response.Topics[0].ErrorCode, Is.EqualTo(ErrorResponseCode.NoError));
                Assert.That(response.Topics[0].Partitions, Has.Length.EqualTo(1));
            }

        }

        [Test]
        public async Task TestOffsetWorksOk()
        {
            using (var temporaryTopic = testCluster.CreateTemporaryTopic())
            using (var connection = await KafkaConnectionFactory.CreateSimpleKafkaConnectionAsync(testCluster.CreateBrokerUris()[0]).ConfigureAwait(true))
            {
                var request = new OffsetRequest
                {
                    Offsets = new List<Offset>
                     {
                         new Offset
                         {
                              Topic = temporaryTopic.Name,
                               MaxOffsets = 1,
                               PartitionId = 0,
                               Time = -1
                         }
                     }
                };

                var response = await connection.SendRequestAsync(request, CancellationToken.None).ConfigureAwait(true);
                Assert.That(response, Has.Count.EqualTo(1));
                var first = response.First();
                Assert.That(first.Error, Is.EqualTo(ErrorResponseCode.NoError));
            }
        }

        [Test]
        public async Task TestMultipleOffsetWorksOk()
        {
            using (var temporaryTopic = testCluster.CreateTemporaryTopic(partitions:2))
            using (var connection = await KafkaConnectionFactory.CreateSimpleKafkaConnectionAsync(testCluster.CreateBrokerUris()[0]).ConfigureAwait(true))
            {
                var topic = temporaryTopic.Name;
                var request = new OffsetRequest
                {
                    Offsets = new List<Offset>
                     {
                         new Offset
                         {
                              Topic = topic,
                               MaxOffsets = 1,
                               PartitionId = 0,
                               Time = -1
                         },
                         new Offset
                         {
                              Topic = topic,
                               MaxOffsets = 1,
                               PartitionId = 1,
                               Time = -1
                         }

                     }
                };

                var response = await connection.SendRequestAsync(request, CancellationToken.None).ConfigureAwait(true);
                Assert.That(response, Has.Count.EqualTo(2));
                Assert.That(response[0].Error, Is.EqualTo(ErrorResponseCode.NoError));
                Assert.That(response[1].Error, Is.EqualTo(ErrorResponseCode.NoError));
            }
        }

        [Test]
        public async Task TestOffsetCommitWorksOk()
        {
            using (var temporaryTopic = testCluster.CreateTemporaryTopic())
            using (var connection = await KafkaConnectionFactory.CreateSimpleKafkaConnectionAsync(testCluster.CreateBrokerUris()[0]).ConfigureAwait(true))
            {
                var request = new OffsetCommitRequest
                {
                    OffsetCommits = new List<OffsetCommit>
                    {
                         new OffsetCommit
                         {
                              Topic = temporaryTopic.Name,
                              Offset = 0
                         }
                     }
                };

                var response = await connection.SendRequestAsync(request, CancellationToken.None).ConfigureAwait(true);
                Assert.That(response, Has.Count.EqualTo(1));
                Assert.That(response[0].Error, Is.EqualTo(ErrorResponseCode.Unknown));
            }
        }

        [Test]
        public async Task TestMultipleOffsetCommitsWorksOk()
        {
            using (var temporaryTopic = testCluster.CreateTemporaryTopic(partitions:2))
            using (var connection = await KafkaConnectionFactory.CreateSimpleKafkaConnectionAsync(testCluster.CreateBrokerUris()[0]).ConfigureAwait(true))
            {
                var request = new OffsetCommitRequest
                {
                    OffsetCommits = 
                        Enumerable
                            .Range(0, 2)
                            .Select(i => new OffsetCommit {
                                Topic = temporaryTopic.Name,
                                PartitionId = i,
                                Offset = 0
                            })
                            .ToList()
                };

                var response = await connection.SendRequestAsync(request, CancellationToken.None).ConfigureAwait(true);
                Assert.That(response, Has.Count.EqualTo(2));
                Assert.That(response[0].Error, Is.EqualTo(ErrorResponseCode.Unknown));
                Assert.That(response[1].Error, Is.EqualTo(ErrorResponseCode.Unknown));
            }
        }

        [Test]
        public async Task TestOffsetFetchWorksOk()
        {
            using (var temporaryTopic = testCluster.CreateTemporaryTopic())
            using (var connection = await KafkaConnectionFactory.CreateSimpleKafkaConnectionAsync(testCluster.CreateBrokerUris()[0]).ConfigureAwait(true))
            {
                var request = new OffsetFetchRequest
                {
                    Topics = new List<OffsetFetch>
                    {
                        new OffsetFetch {
                            Topic = temporaryTopic.Name,
                            PartitionId = 0
                        }
                     }
                };

                var response = await connection.SendRequestAsync(request, CancellationToken.None).ConfigureAwait(true);
                Assert.That(response, Has.Count.EqualTo(1));
                Assert.That(response[0].Error, Is.EqualTo(ErrorResponseCode.Unknown));
            }
        }

        [Test]
        public async Task TestMultipleOffsetFetchesWorkOk()
        {
            using (var temporaryTopic = testCluster.CreateTemporaryTopic(partitions:2))
            using (var connection = await KafkaConnectionFactory.CreateSimpleKafkaConnectionAsync(testCluster.CreateBrokerUris()[0]).ConfigureAwait(true))
            {
                var topic = temporaryTopic.Name;
                var request = new OffsetFetchRequest
                {
                    Topics =
                        Enumerable
                            .Range(0, 2)
                            .Select(i => new OffsetFetch
                            {
                                Topic = topic,
                                PartitionId = i
                            })
                            .ToList()
                };

                var response = await connection.SendRequestAsync(request, CancellationToken.None).ConfigureAwait(true);
                Assert.That(response, Has.Count.EqualTo(2));
                Assert.That(response[0].Error, Is.EqualTo(ErrorResponseCode.Unknown));
                Assert.That(response[1].Error, Is.EqualTo(ErrorResponseCode.Unknown));
            }
        }

        [Test]
        public async Task TestNewTopicProductionWorksOk()
        {
            using (var temporaryTopic = testCluster.CreateTemporaryTopic())
            using (var connection = await KafkaConnectionFactory.CreateSimpleKafkaConnectionAsync(testCluster.CreateBrokerUris()[0]).ConfigureAwait(true))
            {
                var topic = temporaryTopic.Name;
                {
                    var request = new MetadataRequest
                    {
                        Topics = new List<string>
                         {
                             topic
                         }
                    };
                    MetadataResponse response = null;
                    while (response == null)
                    {
                        response = await connection.SendRequestAsync(request, CancellationToken.None).ConfigureAwait(true);
                        if (response.Topics[0].ErrorCode == ErrorResponseCode.LeaderNotAvailable)
                        {
                            response = null;
                            await Task.Delay(1000);
                        }

                    }
                    Assert.That(response, Is.Not.Null);
                    var first = response;
                    Assert.That(first.Topics, Has.Length.EqualTo(1));

                    var firstTopic = first.Topics.First();
                    Assert.That(firstTopic.ErrorCode, Is.EqualTo(ErrorResponseCode.NoError));
                    Assert.That(firstTopic.Name, Is.EqualTo(topic));
                    Assert.That(firstTopic.Partitions, Has.Length.EqualTo(1));

                    var firstPartition = firstTopic.Partitions.First();
                    Assert.That(firstPartition.PartitionId, Is.EqualTo(0));
                }

                {
                    var request = new ProduceRequest
                    {
                        Acks = 1,
                        TimeoutMS = 10000,
                        Payload = new List<Payload>
                             {
                                 new Payload
                                 {
                                      Topic = topic,
                                      Partition = 0,
                                      Codec = MessageCodec.CodecNone,
                                      Messages = new List<Message>
                                      {
                                          new Message("Message 1"),
                                          new Message("Message 2"),
                                          new Message("Message 3"),
                                          new Message("Message 4"),
                                      }
                                 }
                             }
                    };

                    var response = await connection.SendRequestAsync(request, CancellationToken.None).ConfigureAwait(true);
                    Assert.That(response, Is.Not.Null);

                    var first = response.First();
                    Assert.That(first.Error, Is.EqualTo(ErrorResponseCode.NoError));
                    Assert.That(first.Topic, Is.EqualTo(topic));
                    Assert.That(first.PartitionId, Is.EqualTo(0));
                    Assert.That(first.Offset, Is.EqualTo(0));
                }

                {
                    var request = new FetchRequest
                    {
                        MinBytes = 0,
                        MaxWaitTime = 0,
                        Fetches = new List<Fetch>
                             {
                                 new Fetch
                                 {
                                    MaxBytes = 40,
                                    Offset = 0,
                                    PartitionId = 0,
                                    Topic = topic,
                                 }
                            }
                    };

                    var response = await connection.SendRequestAsync(request, CancellationToken.None).ConfigureAwait(true);
                    Assert.That(response, Has.Count.EqualTo(1));
                    var first = response.First();

                    Assert.That(first.Error, Is.EqualTo(ErrorResponseCode.NoError));
                    Assert.That(first.HighWaterMark, Is.EqualTo(4));
                    Assert.That(first.PartitionId, Is.EqualTo(0));
                    Assert.That(first.Topic, Is.EqualTo(topic));
                    Assert.That(first.Messages, Has.Count.EqualTo(1));

                    var firstMessage = first.Messages.First();
                    Assert.That(firstMessage.Meta.Offset, Is.EqualTo(0));
                    Assert.That(firstMessage.Meta.PartitionId, Is.EqualTo(0));
                    Assert.That(firstMessage.Attribute, Is.EqualTo(0));
                    Assert.That(firstMessage.Key, Is.Null);
                    Assert.That(firstMessage.MagicNumber, Is.EqualTo(0));
                    Assert.That(firstMessage.Value, Is.Not.Null);

                    var firstString = firstMessage.Value.ToUtf8String();
                    Assert.That(firstString, Is.EqualTo("Message 1"));
                }

                {
                    var request = new OffsetRequest
                    {
                        Offsets = new List<Offset>
                             {
                                 new Offset
                                 {
                                      MaxOffsets = 2,
                                      PartitionId = 0,
                                      Time = -1,
                                      Topic = topic
                                 }
                             }
                    };

                    var response = await connection.SendRequestAsync(request, CancellationToken.None).ConfigureAwait(true);
                    Assert.That(response, Has.Count.EqualTo(1));
                    var first = response.First();

                    Assert.That(first.Error, Is.EqualTo(ErrorResponseCode.NoError));
                    Assert.That(first.Topic, Is.EqualTo(topic));
                    Assert.That(first.PartitionId, Is.EqualTo(0));
                    Assert.That(first.Offsets, Has.Length.EqualTo(2));

                    Assert.That(first.Offsets[0], Is.EqualTo(4));
                    Assert.That(first.Offsets[1], Is.EqualTo(0));
                }

                {
                    var request = new ConsumerMetadataRequest
                    {
                        ConsumerGroup = topic
                    };
                    ConsumerMetadataResponse response = null;
                    while (response == null)
                    {
                        response = await connection.SendRequestAsync(request, CancellationToken.None).ConfigureAwait(true);
                        if (response.Error == ErrorResponseCode.ConsumerCoordinatorNotAvailableCode)
                        {
                            response = null;
                            await Task.Delay(1000);
                        }
                    }
                    Assert.That(response.Error, Is.EqualTo(ErrorResponseCode.NoError));
                    Console.WriteLine("Id = {0}, Host = {1}, Port = {2}", response.CoordinatorId, response.CoordinatorHost, response.CoordinatorPort);

                }

                {
                    var request = new OffsetFetchRequest
                    {
                        ConsumerGroup = topic,
                        Topics = new List<OffsetFetch>
                              {
                                  new OffsetFetch
                                  {
                                       PartitionId = 0,
                                       Topic = topic
                                  }
                              }
                    };

                    var response = await connection.SendRequestAsync(request, CancellationToken.None).ConfigureAwait(true);
                    Assert.That(response, Has.Count.EqualTo(1));
                    var first = response.First();

                    Assert.That(first.Error, Is.EqualTo(ErrorResponseCode.NoError));
                    Assert.That(first.Topic, Is.EqualTo(topic));
                    Assert.That(first.PartitionId, Is.EqualTo(0));
                    Assert.That(first.MetaData, Is.Empty);
                    Assert.That(first.Offset, Is.EqualTo(-1));
                }

                {
                    var request = new OffsetCommitRequest
                    {
                        ConsumerGroup = topic,
                        ConsumerGroupGenerationId = 1,
                        ConsumerId = "0",
                        OffsetCommits = new List<OffsetCommit>
                               {
                                   new OffsetCommit
                                   {
                                        Metadata = "Metadata 1",
                                        Offset = 0,
                                        PartitionId = 0,
                                        TimeStamp = -1,
                                        Topic = topic,
                                   }
                               }
                    };
                    var response = await connection.SendRequestAsync(request, CancellationToken.None).ConfigureAwait(true);
                    Assert.That(response, Has.Count.EqualTo(1));
                    var first = response.First();

                    Assert.That(first.Error, Is.EqualTo(ErrorResponseCode.NoError));
                    Assert.That(first.Topic, Is.EqualTo(topic));
                    Assert.That(first.PartitionId, Is.EqualTo(0));
                }

                {
                    var request = new OffsetFetchRequest
                    {
                        ConsumerGroup = topic,
                        Topics = new List<OffsetFetch>
                              {
                                  new OffsetFetch
                                  {
                                       PartitionId = 0,
                                       Topic = topic
                                  }
                              }
                    };

                    var response = await connection.SendRequestAsync(request, CancellationToken.None).ConfigureAwait(true);
                    Assert.That(response, Has.Count.EqualTo(1));
                    var first = response.First();

                    Assert.That(first.Error, Is.EqualTo(ErrorResponseCode.NoError));
                    Assert.That(first.Topic, Is.EqualTo(topic));
                    Assert.That(first.PartitionId, Is.EqualTo(0));
                    Assert.That(first.MetaData, Is.EqualTo("Metadata 1"));
                    Assert.That(first.Offset, Is.EqualTo(0));
                }

                {
                    var request = new FetchRequest
                    {
                        MinBytes = 0,
                        MaxWaitTime = 0,
                        Fetches = new List<Fetch>
                             {
                                 new Fetch
                                 {
                                    MaxBytes = 1024,
                                    Offset = 0 + 1,
                                    PartitionId = 0,
                                    Topic = topic,
                                 }
                            }
                    };

                    var response = await connection.SendRequestAsync(request, CancellationToken.None).ConfigureAwait(true);
                    Assert.That(response, Has.Count.EqualTo(1));
                    var first = response.First();

                    Assert.That(first.Error, Is.EqualTo(ErrorResponseCode.NoError));
                    Assert.That(first.HighWaterMark, Is.EqualTo(4));
                    Assert.That(first.PartitionId, Is.EqualTo(0));
                    Assert.That(first.Topic, Is.EqualTo(topic));
                    Assert.That(first.Messages, Has.Count.EqualTo(3));

                    var firstMessage = first.Messages.First();
                    Assert.That(firstMessage.Meta.Offset, Is.EqualTo(1));
                    Assert.That(firstMessage.Meta.PartitionId, Is.EqualTo(0));
                    Assert.That(firstMessage.Attribute, Is.EqualTo(0));
                    Assert.That(firstMessage.Key, Is.Null);
                    Assert.That(firstMessage.MagicNumber, Is.EqualTo(0));
                    Assert.That(firstMessage.Value, Is.Not.Null);

                    var firstString = firstMessage.Value.ToUtf8String();
                    Assert.That(firstString, Is.EqualTo("Message 2"));

                    var lastMessage = first.Messages.Last();
                    Assert.That(lastMessage.Meta.Offset, Is.EqualTo(3));
                    Assert.That(lastMessage.Meta.PartitionId, Is.EqualTo(0));
                    Assert.That(lastMessage.Attribute, Is.EqualTo(0));
                    Assert.That(lastMessage.Key, Is.Null);
                    Assert.That(lastMessage.MagicNumber, Is.EqualTo(0));
                    Assert.That(lastMessage.Value, Is.Not.Null);

                    var lastString = lastMessage.Value.ToUtf8String();
                    Assert.That(lastString, Is.EqualTo("Message 4"));


                }

                {
                    var request = new FetchRequest
                    {
                        MinBytes = 0,
                        MaxWaitTime = 0,
                        Fetches = new List<Fetch>
                             {
                                 new Fetch
                                 {
                                    MaxBytes = 1024,
                                    Offset = 3 + 1,
                                    PartitionId = 0,
                                    Topic = topic,
                                 }
                            }
                    };

                    var response = await connection.SendRequestAsync(request, CancellationToken.None).ConfigureAwait(true);
                    Assert.That(response, Has.Count.EqualTo(1));
                    var first = response.First();

                    Assert.That(first.Error, Is.EqualTo(ErrorResponseCode.NoError));
                    Assert.That(first.HighWaterMark, Is.EqualTo(4));
                    Assert.That(first.PartitionId, Is.EqualTo(0));
                    Assert.That(first.Topic, Is.EqualTo(topic));
                    Assert.That(first.Messages, Has.Count.EqualTo(0));
                }
            }
            Console.WriteLine("Test completed");
        }

        [Test]
        public async Task TestSimpleKafkaBrokerWorksOk()
        {
            using (var brokers = new KafkaBrokers(testCluster.CreateBrokerUris()))
            {
                await brokers.RefreshAsync(CancellationToken.None);
            }
        }

        [Test]
        public async Task TestSimpleProducerWorksOk()
        {
            var valueSerializer = new StringSerializer();

            using (var temporaryTopic = testCluster.CreateTemporaryTopic())
            using (var brokers = new KafkaBrokers(testCluster.CreateBrokerUris()))
            {
                var producer = KafkaProducer.Create(brokers, valueSerializer);

                await producer.SendAsync(KeyedMessage.Create(temporaryTopic.Name, "Message"), CancellationToken.None).ConfigureAwait(true);


            }
        }

    }
}
