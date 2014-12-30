using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Text;
using KafkaNet.Common;
using KafkaNet.Protocol;
using NUnit.Framework;


namespace kafka_tests.Unit
{
    [TestFixture]
    [Category("Unit")]
    public class ProtocolBaseRequestTests
    {
        [Test]
        public void EnsureHeaderShouldPackCorrectByteLengths()
        {
            var result = BaseRequest.EncodeHeader(new FetchRequest { ClientId = "test", CorrelationId = 123456789 });

            Assert.That(result.Length, Is.EqualTo(14));
            Assert.That(result, Is.EqualTo(new byte[] { 0, 1, 0, 0, 7, 91, 205, 21, 0, 4, 116, 101, 115, 116 }));
        }

        [Test]
        public void EnsureBothMessageRequest()
        {
            var message = new Message { Key = "james".ToBytes(), Value = "james".ToBytes() };


            Assert.That(Message.EncodeMessage(message), Is.EqualTo(Message.EncodeMessage2(message)));

        }
             [Test]
        public void EnsureBothMessageSetRequest()
            {
                var messages = new[]
                {
                    new Message {Key = "james".ToBytes(), Value = "james".ToBytes()},
                    new Message {Key = "james".ToBytes(), Value = "james".ToBytes()},
                    new Message {Key = "james".ToBytes(), Value = "james".ToBytes()}
                };

                 var k = new KafkaMessagePacker();
                 var e = Message.EncodeMessageSet2(messages);
                 Assert.That(Message.EncodeMessageSet(messages), Is.EqualTo(e));
        }
        
        [Test]
        public void EnsureBothProduceRequest()
        {
            var message = new ProduceRequest()
            {
                ClientId = "test",
                CorrelationId = 1,
               Acks = 1,
               TimeoutMS = 1,
                Payload = new List<Payload>
                {
                    new Payload{Codec = MessageCodec.CodecGzip, Topic = "333", Partition = 1, Messages = new List<Message>{ new Message{Key = "james".ToBytes(), Value = "james".ToBytes()}}},
                    new Payload{Codec = MessageCodec.CodecGzip, Topic = "333", Partition = 1, Messages = new List<Message>{ new Message{Key = "james".ToBytes(), Value = "james".ToBytes()}}},
                    new Payload{Codec = MessageCodec.CodecGzip, Topic = "333", Partition = 1, Messages = new List<Message>{ new Message{Key = "james".ToBytes(), Value = "james".ToBytes()}}},
                    new Payload{Codec = MessageCodec.CodecGzip, Topic = "333", Partition = 1, Messages = new List<Message>{ new Message{Key = "james".ToBytes(), Value = "james".ToBytes()}}},
                    new Payload{Codec = MessageCodec.CodecGzip, Topic = "333", Partition = 1, Messages = new List<Message>{ new Message{Key = "james".ToBytes(), Value = "james".ToBytes()}}},
                    new Payload{Codec = MessageCodec.CodecGzip, Topic = "333", Partition = 1, Messages = new List<Message>{ new Message{Key = "james".ToBytes(), Value = "james".ToBytes()}}},
                    new Payload{Codec = MessageCodec.CodecNone, Topic = "333", Partition = 1, Messages = new List<Message>{ new Message{Key = "james".ToBytes(), Value = "james".ToBytes()}}},
                    new Payload{Codec = MessageCodec.CodecNone, Topic = "333", Partition = 1, Messages = new List<Message>{ new Message{Key = "james".ToBytes(), Value = "james".ToBytes()}}},
                    new Payload{Codec = MessageCodec.CodecNone, Topic = "333", Partition = 1, Messages = new List<Message>{ new Message{Key = "james".ToBytes(), Value = "james".ToBytes()}}},
                    new Payload{Codec = MessageCodec.CodecNone, Topic = "333", Partition = 1, Messages = new List<Message>{ new Message{Key = "james".ToBytes(), Value = "james".ToBytes()}}},
                    new Payload{Codec = MessageCodec.CodecNone, Topic = "333", Partition = 1, Messages = new List<Message>{ new Message{Key = "james".ToBytes(), Value = "james".ToBytes()}}}
                }
            };

            Assert.That(message.Encode(), Is.EqualTo(message.Encode2()));
        }

        [Test]
        public void EnsureFaster()
        {
            var message = new ProduceRequest()
            {
                ClientId = "test",
                CorrelationId = 1,
                Acks = 1,
                TimeoutMS = 1,
                Payload = new List<Payload>
                {
                    new Payload{Codec = MessageCodec.CodecGzip, Topic = "333", Partition = 1, Messages = new List<Message>{ new Message{Key = "james".ToBytes(), Value = "james".ToBytes()}}},
                    new Payload{Codec = MessageCodec.CodecGzip, Topic = "333", Partition = 1, Messages = new List<Message>{ new Message{Key = "james".ToBytes(), Value = "james".ToBytes()}}},
                    new Payload{Codec = MessageCodec.CodecGzip, Topic = "333", Partition = 1, Messages = new List<Message>{ new Message{Key = "james".ToBytes(), Value = "james".ToBytes()}}},
                    new Payload{Codec = MessageCodec.CodecGzip, Topic = "333", Partition = 1, Messages = new List<Message>{ new Message{Key = "james".ToBytes(), Value = "james".ToBytes()}}},
                    new Payload{Codec = MessageCodec.CodecGzip, Topic = "333", Partition = 1, Messages = new List<Message>{ new Message{Key = "james".ToBytes(), Value = "james".ToBytes()}}},
                    new Payload{Codec = MessageCodec.CodecGzip, Topic = "333", Partition = 1, Messages = new List<Message>{ new Message{Key = "james".ToBytes(), Value = "james".ToBytes()}}},
                    new Payload{Codec = MessageCodec.CodecNone, Topic = "333", Partition = 1, Messages = new List<Message>{ new Message{Key = "james".ToBytes(), Value = "james".ToBytes()}}},
                    new Payload{Codec = MessageCodec.CodecNone, Topic = "333", Partition = 1, Messages = new List<Message>{ new Message{Key = "james".ToBytes(), Value = "james".ToBytes()}}},
                    new Payload{Codec = MessageCodec.CodecNone, Topic = "333", Partition = 1, Messages = new List<Message>{ new Message{Key = "james".ToBytes(), Value = "james".ToBytes()}}},
                    new Payload{Codec = MessageCodec.CodecNone, Topic = "333", Partition = 1, Messages = new List<Message>{ new Message{Key = "james".ToBytes(), Value = "james".ToBytes()}}},
                    new Payload{Codec = MessageCodec.CodecNone, Topic = "333", Partition = 1, Messages = new List<Message>{ new Message{Key = "james".ToBytes(), Value = "james".ToBytes()}}}
                }
            };

            var sw = Stopwatch.StartNew();
            for (int i = 0; i < 100000; i++)
            {
                message.Encode();
            }

            var time1 = sw.ElapsedMilliseconds;
            sw.Restart();

            for (int i = 0; i < 100000; i++)
            {
                message.Encode2();
            }
            var time2 = sw.ElapsedMilliseconds;

            Console.WriteLine("Time1: {0}, Time2:{1}", time1, time2);
            Assert.That(time2, Is.LessThan(time1));
        }


  [Test]
        public void EnsureBothOffsetRequest()
        {
            var message = new OffsetRequest()
            {
                ClientId = "test",
                CorrelationId = 1,
                Offsets = new List<Offset> { 
                    new Offset{PartitionId = 1,MaxOffsets = 1, Time = 1, Topic = "test1"},
                    new Offset{PartitionId = 0,MaxOffsets = 2, Time = 2, Topic = "test2"}
                }
            };

            Assert.That(message.Encode(), Is.EqualTo(message.Encode2()));
        }
        [Test]
        public void EnsureBothOffsetFetchRequest()
        {
            var message = new OffsetFetchRequest()
            {
                ClientId = "test",
                CorrelationId = 1,
                ConsumerGroup = "tests",
                Topics = new List<OffsetFetch> { 
                new OffsetFetch{PartitionId = 1, Topic = "topic1"},
                new OffsetFetch{PartitionId = 0, Topic = "topic0"}
                }
            };

            Assert.That(message.Encode(), Is.EqualTo(message.Encode2()));
        }

        [Test]
        public void EnsureBothCommitReponseTheSame()
        {
            var message = new ConsumerMetadataRequest()
            {
                ClientId = "test",
                CorrelationId = 1,
                ConsumerGroup = "tests"
            };

            Assert.That(message.Encode(), Is.EqualTo(message.Encode2()));
        }

        [Test]
        public void EnsureBothCommitTheSame()
        {
            var message = new OffsetCommitRequest()
            {
                ClientId = "test",
                CorrelationId = 1,
                ConsumerGroup = "tests",
                OffsetCommits = new List<OffsetCommit> { 
                 new OffsetCommit{Metadata = "22", Offset = 22, PartitionId = 1, TimeStamp = 123241242, Topic = "test"},
                 new OffsetCommit{Metadata = "222", Offset = 222, PartitionId = 12, TimeStamp = 123241242, Topic = "test2"}
                }
            };

            Assert.That(message.Encode(), Is.EqualTo(message.Encode2()));
        }

        [Test]
        public void EnsureBothMetadataAreTheSame()
        {
            var message = new MetadataRequest()
            {
                ClientId = "test",
                CorrelationId = 1,
                Topics = new List<string>
                {
                    "topic1",
                    "topic2"
                }
            };

            Assert.That(message.Encode(), Is.EqualTo(message.Encode2()));
        }

        [Test]
        public void EnsureBothEncodesAreTheSame()
        {
            var message = new FetchRequest
            {
                ClientId = "test",
                CorrelationId = 1,
                Fetches = new List<Fetch>
                {
                    new Fetch
                    {
                        MaxBytes = 4800,
                        Offset = 7777,
                        PartitionId = 1,
                        Topic = "test"
                    }
                }
            };

           Assert.That(message.Encode(), Is.EqualTo(message.Encode2()));
        }

        [Test]
        public void EnsureEncodeSpeed()
        {
            var sw = Stopwatch.StartNew();
            for (int i = 0; i < 100000; i++)
            {
                var bytes = new FetchRequest
                {
                    ClientId = "test",
                    CorrelationId = 1,
                    Fetches = new List<Fetch>
                    {
                        new Fetch
                        {
                            MaxBytes = 4800,
                            Offset = 7777,
                            PartitionId = 1,
                            Topic = "test"
                        }
                    }
                }.Encode();
            }

            var time1 = sw.ElapsedMilliseconds;
            sw.Restart();

            for (int i = 0; i < 100000; i++)
            {
                var bytes = new FetchRequest
                {
                    ClientId = "test",
                    CorrelationId = 1,
                    Fetches = new List<Fetch>
                    {
                        new Fetch
                        {
                            MaxBytes = 4800,
                            Offset = 7777,
                            PartitionId = 1,
                            Topic = "test"
                        }
                    }
                }.Encode2();
            }
            var time2 = sw.ElapsedMilliseconds;

            Console.WriteLine("Time1: {0}, Time2:{1}", time1, time2);
            Assert.That(time2, Is.LessThan(time1));
        }
    }
}
