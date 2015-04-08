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
    [Category("Unit")]
    class SimpleTests
    {
        [SetUp]
        public void Setup()
        {

        }

        [Test]
        public async Task TestProducingWorksOk()
        {
            using (var connection = await KafkaConnectionFactory.CreateSimpleKafkaConnectionAsync(IntegrationConfig.IntegrationUri).ConfigureAwait(true))
            {
                var request = new ProduceRequest
                {
                    Acks = 1,
                    TimeoutMS = 10000,
                    Payload = new List<Payload>
                     {
                         new Payload
                         {
                              Topic = IntegrationConfig.IntegrationTopic,
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
                Console.WriteLine(response);
            }
        }

        [Test]
        public async Task TestFetchingWorksOk()
        {
            using (var connection = await KafkaConnectionFactory.CreateSimpleKafkaConnectionAsync(IntegrationConfig.IntegrationUri).ConfigureAwait(true))
            {
                var request = new FetchRequest
                {
                    MaxWaitTime = 1000,
                    MinBytes = 1000,
                    Fetches = new List<Fetch>
                     {
                         new Fetch
                         {
                              Topic = IntegrationConfig.IntegrationTopic,
                              PartitionId = 0,
                              MaxBytes = 1024,
                              Offset = 0
                         }
                     }
                };

                var response = await connection.SendRequestAsync(request, CancellationToken.None).ConfigureAwait(true);
                Console.WriteLine(response);
            }
        }

        [Test]
        public async Task TestListingAllTopicsWorksOk()
        {
            using (var connection = await KafkaConnectionFactory.CreateSimpleKafkaConnectionAsync(IntegrationConfig.IntegrationUri).ConfigureAwait(true))
            {
                var request = new MetadataRequest { };
                var response = await connection.SendRequestAsync(request, CancellationToken.None).ConfigureAwait(true);
                Assert.That(response, Is.Not.Null);
                var first = response;
                {
                    foreach (var broker in first.Brokers)
                    {
                        Console.WriteLine("{0},{1},{2},{3}", broker.Address, broker.BrokerId, broker.Host, broker.Port);
                    }
                    foreach (var topic in first.Topics)
                    {
                        Console.WriteLine("{0},{1}", topic.ErrorCode, topic.Name);
                        foreach (var partition in topic.Partitions)
                        {
                            Console.WriteLine("{0},{1},{2},{3},{4}", partition.ErrorCode, partition.Isrs.Count, partition.LeaderId, partition.PartitionId, partition.Replicas.Count);
                        }
                    }
                }
            }

        }

        [Test]
        public async Task TestOffsetWorksOk()
        {
            using (var connection = await KafkaConnectionFactory.CreateSimpleKafkaConnectionAsync(IntegrationConfig.IntegrationUri).ConfigureAwait(true))
            {
                var request = new OffsetRequest
                {
                    Offsets = new List<Offset>
                     {
                         new Offset
                         {
                              Topic = IntegrationConfig.IntegrationTopic,
                               MaxOffsets = 1,
                               PartitionId = 0,
                               Time = -1
                         }
                     }
                };

                var response = await connection.SendRequestAsync(request, CancellationToken.None).ConfigureAwait(true);
                Console.WriteLine(response);
            }
        }

        [Test]
        public async Task TestNewTopicProductionWorksOk()
        {
            var topic = Guid.NewGuid().ToString();
            RunKafkaTopicsCommand("--topic", topic, "--create", "--partitions", "1", "--replication-factor", "1");
            try
            {
                using (var connection = await KafkaConnectionFactory.CreateSimpleKafkaConnectionAsync(IntegrationConfig.IntegrationUri).ConfigureAwait(true))
                {
                    {
                        var request = new MetadataRequest
                        {
                            Topics = new List<string>
                         {
                             topic
                         }
                        };
                        var response = await connection.SendRequestAsync(request, CancellationToken.None).ConfigureAwait(true);
                        Assert.That(response, Is.Not.Null);
                        var first = response;
                        Assert.That(first.Topics, Has.Count.EqualTo(1));

                        var firstTopic = first.Topics.First();
                        Assert.That(firstTopic.ErrorCode, Is.EqualTo((short)ErrorResponseCode.NoError));
                        Assert.That(firstTopic.Name, Is.EqualTo(topic));
                        Assert.That(firstTopic.Partitions, Has.Count.EqualTo(1));

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
                        Assert.That(first.Error, Is.EqualTo((short)ErrorResponseCode.NoError));
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
                        Assert.That(response, Is.Not.Null);
                        var first = response;

                        Assert.That(first.Error, Is.EqualTo((short)ErrorResponseCode.NoError));
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
                        Assert.That(response, Is.Not.Null);
                        var first = response;

                        Assert.That(first.Error, Is.EqualTo((short)ErrorResponseCode.NoError));
                        Assert.That(first.Topic, Is.EqualTo(topic));
                        Assert.That(first.PartitionId, Is.EqualTo(0));
                        Assert.That(first.Offsets, Has.Count.EqualTo(2));

                        Assert.That(first.Offsets[0], Is.EqualTo(4));
                        Assert.That(first.Offsets[1], Is.EqualTo(0));
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
                        Assert.That(response, Is.Not.Null);
                        var first = response;

                        Assert.That(first.Error, Is.EqualTo((short)ErrorResponseCode.NoError));
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
                        Assert.That(response, Is.Not.Null);
                        var first = response;
                        Assert.That(first.Error, Is.EqualTo((short)ErrorResponseCode.NoError));
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
                        Assert.That(response, Is.Not.Null);
                        var first = response;

                        Assert.That(first.Error, Is.EqualTo((short)ErrorResponseCode.NoError));
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
                        Assert.That(response, Is.Not.Null);
                        var first = response;

                        Assert.That(first.Error, Is.EqualTo((short)ErrorResponseCode.NoError));
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
                        Assert.That(response, Is.Not.Null);
                        var first = response;

                        Assert.That(first.Error, Is.EqualTo((short)ErrorResponseCode.NoError));
                        Assert.That(first.HighWaterMark, Is.EqualTo(4));
                        Assert.That(first.PartitionId, Is.EqualTo(0));
                        Assert.That(first.Topic, Is.EqualTo(topic));
                        Assert.That(first.Messages, Has.Count.EqualTo(0));
                    }
                    }
                    Console.WriteLine("Test completed");
            }
            finally
            {
                RunKafkaTopicsCommand("--topic", topic, "--delete");
            }

        }

        [Test]
        public async Task TestSimpleKafkaBrokerWorksOk()
        {
            using (var brokers = new KafkaBrokers(IntegrationConfig.IntegrationUri))
            {
                await brokers.RefreshAsync(CancellationToken.None);
                Console.WriteLine(brokers);

            }
        }

        [Test]
        public async Task TestSimpleProducerWorksOk()
        {
            var keySerializer = new NullSerializer<string>();
            var valueSerializer = new StringSerializer();
            var messagePartitioner = new LoadBalancedPartitioner<string, string>(1);

            using (var brokers = new KafkaBrokers(IntegrationConfig.IntegrationUri))
            {
                var producer = new KafkaProducer<string,string>(brokers, keySerializer, valueSerializer, messagePartitioner);

                await producer.SendAsync(new KafkaMessage<string, string>(IntegrationConfig.IntegrationTopic, null, "Message"), CancellationToken.None).ConfigureAwait(true);


            }
        }

        private static void RunKafkaTopicsCommand(params string[] args)
        {
            var cmd = "--host=tcp://server.home:2375 run --rm sceneskope/kafka:0.8.2.1 bin/kafka-topics.sh --zookeeper server.home ";
            var arguments = cmd + String.Join(" ", args);

            var info = new ProcessStartInfo
            {
                Arguments = arguments,
                CreateNoWindow = true,
                FileName = @"c:\users\nick\bin\docker.exe",
                UseShellExecute = false,
                RedirectStandardOutput = true,
            };
            var process = Process.Start(info);
            var stdout = process.StandardOutput.ReadToEnd();
            process.WaitForExit();
            Console.WriteLine(stdout);
        }
    }
}
