using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using KafkaNet;
using KafkaNet.Model;
using KafkaNet.Protocol;
using NUnit.Framework;

namespace kafka_tests.Integration
{
    [TestFixture]
    [Category("Integration")]
    public class ManualConsumerTests
    {
        private const string KafkaUrl = "http://192.168.10.27:9092";
        private readonly KafkaOptions _options;
        private readonly Uri _kafkaUri;
        private const int DefaultMaxMessageSetSize = 4096*8;
        private string _topic = "ManualConsumerTestTopic";
        private string _defaultConsumerGroup = "ManualConsumerTestsConsumerGroup";

        public ManualConsumerTests()
        {
            _kafkaUri = new Uri(KafkaUrl);
            _options = new KafkaOptions(_kafkaUri);
        }

        [Test]
        public async Task FetchMessagesSimpleTest()
        {
            // Creating a broker router and a protocol gateway for the producer and consumer
            var brokerRouter = new BrokerRouter(_options);
            var protocolGateway = new ProtocolGateway(_kafkaUri);
            var partitionId = 1;
            var topic = "ManualConsumerTestTopic";

            Producer producer = new Producer(brokerRouter);
            ManualConsumer consumer = new ManualConsumer(partitionId, topic, protocolGateway, "TestClient", DefaultMaxMessageSetSize);

            var offset = await consumer.FetchLastOffset();

            // Creating 5 messages
            List<Message> messages = CreateTestMessages(5, 1);

            await producer.SendMessageAsync(topic, messages, partition: partitionId, timeout:TimeSpan.FromSeconds(3));

            // Now let's consume
            var result = (await consumer.FetchMessages(5, offset)).ToList();

            CheckMessages(messages, result);
        }

        [Test]
        public async Task FetchMessagesCacheContainsAllRequestTest()
        {
            // Creating a broker router and a protocol gateway for the producer and consumer
            var brokerRouter = new BrokerRouter(_options);
            var protocolGateway = new ProtocolGateway(_kafkaUri);
            var partitionId = 1;            

            Producer producer = new Producer(brokerRouter);
            ManualConsumer consumer = new ManualConsumer(partitionId, _topic, protocolGateway, "TestClient", DefaultMaxMessageSetSize);

            var offset = await consumer.FetchLastOffset();

            // Creating 5 messages
            List<Message> messages = CreateTestMessages(10, 1);

            await producer.SendMessageAsync(_topic, messages, partition: partitionId, timeout: TimeSpan.FromSeconds(3));

            // Now let's consume
            var result = (await consumer.FetchMessages(5, offset)).ToList();

            CheckMessages(messages.Take(5).ToList(), result);

            // Now let's consume again
            result = (await consumer.FetchMessages(5, offset + 5)).ToList();

            CheckMessages(messages.Skip(5).ToList(), result);
        }

        [Test]
        public async Task FetchMessagesCacheContainsNoneOfRequestTest()
        {
            // Creating a broker router and a protocol gateway for the producer and consumer
            var brokerRouter = new BrokerRouter(_options);
            var protocolGateway = new ProtocolGateway(_kafkaUri);
            var partitionId = 1;

            Producer producer = new Producer(brokerRouter);
            ManualConsumer consumer = new ManualConsumer(partitionId, _topic, protocolGateway, "TestClient", DefaultMaxMessageSetSize);

            var offset = await consumer.FetchLastOffset();

            // Creating 5 messages
            List<Message> messages = CreateTestMessages(10, 4096);

            await producer.SendMessageAsync(_topic, messages, partition: partitionId, timeout: TimeSpan.FromSeconds(3));

            // Now let's consume
            var result = (await consumer.FetchMessages(7, offset)).ToList();

            CheckMessages(messages.Take(7).ToList(), result);

            // Now let's consume again
            result = (await consumer.FetchMessages(2, offset + 8)).ToList();

            CheckMessages(messages.Skip(8).ToList(), result);
        }

        [Test]
        public async Task FetchMessagesCacheContainsPartOfRequestTest()
        {
            // Creating a broker router and a protocol gateway for the producer and consumer
            var brokerRouter = new BrokerRouter(_options);
            var protocolGateway = new ProtocolGateway(_kafkaUri);
            var partitionId = 1;

            Producer producer = new Producer(brokerRouter);
            ManualConsumer consumer = new ManualConsumer(partitionId, _topic, protocolGateway, "TestClient", DefaultMaxMessageSetSize);

            var offset = await consumer.FetchLastOffset();

            // Creating 5 messages
            List<Message> messages = CreateTestMessages(10, 4096);

            await producer.SendMessageAsync(_topic, messages, partition: partitionId, timeout: TimeSpan.FromSeconds(3));

            // Now let's consume
            var result = (await consumer.FetchMessages(5, offset)).ToList();

            CheckMessages(messages.Take(5).ToList(), result);

            // Now let's consume again
            result = (await consumer.FetchMessages(5, offset + 5)).ToList();

            CheckMessages(messages.Skip(5).ToList(), result);
        }

        [Test]
        public async Task FetchMessagesNoNewMessagesInQueueTest()
        {
            // Creating a broker router and a protocol gateway for the producer and consumer
            var protocolGateway = new ProtocolGateway(_kafkaUri);
            var partitionId = 1;

            ManualConsumer consumer = new ManualConsumer(partitionId, _topic, protocolGateway, "TestClient", DefaultMaxMessageSetSize);

            var offset = await consumer.FetchLastOffset();

            // Now let's consume
            var result = (await consumer.FetchMessages(5, offset)).ToList();

            Assert.AreEqual(0, result.Count, "Should not get any messages");
        }

        [Test]
        [ExpectedException(typeof(KafkaApplicationException), ExpectedMessage = "FetchResponse returned error condition.  ErrorCode:1")]
        public async Task FetchMessagesOffsetBiggerThanLastOffsetInQueueTest()
        {
            // Creating a broker router and a protocol gateway for the producer and consumer
            var protocolGateway = new ProtocolGateway(_kafkaUri);
            var partitionId = 1;

            ManualConsumer consumer = new ManualConsumer(partitionId, _topic, protocolGateway, "TestClient", DefaultMaxMessageSetSize);

            var offset = await consumer.FetchLastOffset();

            // Now let's consume
            var result = (await consumer.FetchMessages(5, offset + 1)).ToList();

            Assert.AreEqual(0, result.Count, "Should not get any messages");
        }

        [Test]
        [ExpectedException(typeof(ArgumentOutOfRangeException))]
        public async Task FetchMessagesInvalidOffsetTest()
        {
            // Creating a broker router and a protocol gateway for the producer and consumer
            var protocolGateway = new ProtocolGateway(_kafkaUri);
            var partitionId = 1;

            ManualConsumer consumer = new ManualConsumer(partitionId, _topic, protocolGateway, "TestClient", DefaultMaxMessageSetSize);

            var offset = -1;

            // Now let's consume
            await consumer.FetchMessages(5, offset);
        }

        [Test]
        [Ignore]
        public async Task FetchMessagesTopicDoesntExist()
        {
            // Creating a broker router and a protocol gateway for the producer and consumer
            var protocolGateway = new ProtocolGateway(_kafkaUri);
            var partitionId = 1;
            var topic = _topic + Guid.NewGuid();

            ManualConsumer consumer = new ManualConsumer(partitionId, topic, protocolGateway, "TestClient", DefaultMaxMessageSetSize * 2);

            var offset = await consumer.FetchLastOffset();

            // Now let's consume
            var result = (await consumer.FetchMessages(5, offset)).ToList();

            Assert.AreEqual(0, result.Count);
        }

        [Test]
        [ExpectedException(typeof(InvalidPartitionException))]
        public async Task FetchMessagesPartitionDoesntExist()
        {
            // Creating a broker router and a protocol gateway for the producer and consumer
            var protocolGateway = new ProtocolGateway(_kafkaUri);
            var partitionId = 100;
            var topic = _topic;

            ManualConsumer consumer = new ManualConsumer(partitionId, topic, protocolGateway, "TestClient", DefaultMaxMessageSetSize * 2);

            var offset = 0;

            await consumer.FetchMessages(5, offset);
        }

        [Test]
        [ExpectedException(typeof(BufferUnderRunException))]
        public async Task FetchMessagesBufferUnderRunTest()
        {
            // Creating a broker router and a protocol gateway for the producer and consumer
            var brokerRouter = new BrokerRouter(_options);
            var protocolGateway = new ProtocolGateway(_kafkaUri);
            var partitionId = 1;
            var smallMessageSet = 4096/2;

            Producer producer = new Producer(brokerRouter);
            ManualConsumer consumer = new ManualConsumer(partitionId, _topic, protocolGateway, "TestClient", smallMessageSet);

            var offset = await consumer.FetchLastOffset();

            // Creating 5 messages
            List<Message> messages = CreateTestMessages(10, 4096);

            await producer.SendMessageAsync(_topic, messages, partition: partitionId, timeout: TimeSpan.FromSeconds(3));

            // Now let's consume
            var result = (await consumer.FetchMessages(5, offset)).ToList();

            CheckMessages(messages.Take(5).ToList(), result);
        }

        [Test]
        [ExpectedException(typeof(KafkaApplicationException), ExpectedMessage = "FetchResponse returned error condition.  ErrorCode:3")]
        public async Task FetchOffsetConsumerGroupDoesntExistTest()
        {
            // Creating a broker router and a protocol gateway for the producer and consumer
            var protocolGateway = new ProtocolGateway(_kafkaUri);
            var partitionId = 0;
            var consumerGroup = Guid.NewGuid().ToString();

            ManualConsumer consumer = new ManualConsumer(partitionId, _topic, protocolGateway, "TestClient", DefaultMaxMessageSetSize);

            await consumer.FetchOffset(consumerGroup);
        }

        [Test]
        [ExpectedException(typeof(InvalidPartitionException))]
        public async Task FetchOffsetPartitionDoesntExistTest()
        {
            // Creating a broker router and a protocol gateway for the producer and consumer
            var protocolGateway = new ProtocolGateway(_kafkaUri);
            var partitionId = 100;
            var consumerGroup = _defaultConsumerGroup;

            ManualConsumer consumer = new ManualConsumer(partitionId, _topic, protocolGateway, "TestClient", DefaultMaxMessageSetSize);

            await consumer.FetchOffset(consumerGroup);
        }

        [Test]
        [ExpectedException(typeof(KafkaApplicationException), ExpectedMessage = "FetchResponse returned error condition.  ErrorCode:3")]
        public async Task FetchOffsetTopicDoesntExistTest()
        {
            // Creating a broker router and a protocol gateway for the producer and consumer
            var protocolGateway = new ProtocolGateway(_kafkaUri);
            var partitionId = 0;
            var topic = _topic + Guid.NewGuid();
            var consumerGroup = _defaultConsumerGroup;

            ManualConsumer consumer = new ManualConsumer(partitionId, topic, protocolGateway, "TestClient", DefaultMaxMessageSetSize);

            await consumer.FetchOffset(consumerGroup);
        }

        [Test]        
        public async Task FetchOffsetConsumerGroupExistsTest()
        {
            // Creating a broker router and a protocol gateway for the producer and consumer
            var protocolGateway = new ProtocolGateway(_kafkaUri);
            var partitionId = 0;
            var consumerGroup = _defaultConsumerGroup;

            ManualConsumer consumer = new ManualConsumer(partitionId, _topic, protocolGateway, "TestClient", DefaultMaxMessageSetSize);

            var offest = 5;

            await consumer.UpdateOrCreateOffset(consumerGroup, offest);
            var res = await consumer.FetchOffset(consumerGroup);

            Assert.AreEqual(offest, res);
        }

        [Test]
        [ExpectedException(typeof(ArgumentNullException))]
        public async Task FetchOffsetConsumerGroupIsNullTest()            
        {
            // Creating a broker router and a protocol gateway for the producer and consumer
            var protocolGateway = new ProtocolGateway(_kafkaUri);
            var partitionId = 0;
            var consumerGroup = _defaultConsumerGroup;

            ManualConsumer consumer = new ManualConsumer(partitionId, _topic, protocolGateway, "TestClient", DefaultMaxMessageSetSize);

            var offest = 5;

            await consumer.UpdateOrCreateOffset(consumerGroup, offest);
            await consumer.FetchOffset(null);
        }

        [Test]
        [ExpectedException(typeof(ArgumentNullException))]
        public async Task FetchOffsetConsumerGroupIsEmptyTest()
        {
            // Creating a broker router and a protocol gateway for the producer and consumer
            var protocolGateway = new ProtocolGateway(_kafkaUri);
            var partitionId = 0;
            var consumerGroup = _defaultConsumerGroup;

            ManualConsumer consumer = new ManualConsumer(partitionId, _topic, protocolGateway, "TestClient", DefaultMaxMessageSetSize);

            var offest = 5;

            await consumer.UpdateOrCreateOffset(consumerGroup, offest);
            await consumer.FetchOffset(string.Empty);
        }

        [Test]
        public async Task UpdateOrCreateOffsetConsumerGroupDoesntExistTest()
        {
            // Creating a broker router and a protocol gateway for the producer and consumer
            var protocolGateway = new ProtocolGateway(_kafkaUri);
            var partitionId = 0;
            var consumerGroup = Guid.NewGuid().ToString();

            ManualConsumer consumer = new ManualConsumer(partitionId, _topic, protocolGateway, "TestClient", DefaultMaxMessageSetSize);

            var offest = 5;

            await consumer.UpdateOrCreateOffset(consumerGroup, offest);
            var res = await consumer.FetchOffset(consumerGroup);

            Assert.AreEqual(offest, res);            
        }

        [Test]
        public async Task UpdateOrCreateOffsetConsumerGroupExistsTest()
        {
            // Creating a broker router and a protocol gateway for the producer and consumer
            var protocolGateway = new ProtocolGateway(_kafkaUri);
            var partitionId = 0;
            var consumerGroup = _defaultConsumerGroup + Guid.NewGuid();

            ManualConsumer consumer = new ManualConsumer(partitionId, _topic, protocolGateway, "TestClient", DefaultMaxMessageSetSize);

            var offest = 5;
            var newOffset = 10;

            await consumer.UpdateOrCreateOffset(consumerGroup, offest);
            var res = await consumer.FetchOffset(consumerGroup);
            Assert.AreEqual(offest, res);

            await consumer.UpdateOrCreateOffset(consumerGroup, newOffset);

            res = await consumer.FetchOffset(consumerGroup);

            Assert.AreEqual(newOffset, res);
        }

        [Test]
        [ExpectedException(typeof (InvalidPartitionException))]
        public async Task UpdateOrCreateOffsetPartitionDoesntExistTest()
        {
            // Creating a broker router and a protocol gateway for the producer and consumer
            var protocolGateway = new ProtocolGateway(_kafkaUri);
            var partitionId = 100;
            var consumerGroup = Guid.NewGuid().ToString();

            ManualConsumer consumer = new ManualConsumer(partitionId, _topic, protocolGateway, "TestClient", DefaultMaxMessageSetSize);

            var offest = 5;

            await consumer.UpdateOrCreateOffset(consumerGroup, offest);
        }

        [Test]
        [Ignore("This test is currently faulty, can't have UpdateOrCreateOffset behave differently than FetchOffset")]
        public async Task UpdateOrCreateOffsetTopicDoesntExistTest()
        {
            // Creating a broker router and a protocol gateway for the producer and consumer
            var protocolGateway = new ProtocolGateway(_kafkaUri);
            var partitionId = 0;
            var topic = _topic + Guid.NewGuid();
            var consumerGroup = _defaultConsumerGroup;

            ManualConsumer consumer = new ManualConsumer(partitionId, topic, protocolGateway, "TestClient", DefaultMaxMessageSetSize);

            var offest = 5;

            await consumer.UpdateOrCreateOffset(consumerGroup, offest);
        }

        [Test]
        [ExpectedException(typeof(ArgumentNullException))]
        public async Task UpdateOrCreateOffsetConsumerGroupNullTest()
        {
            // Creating a broker router and a protocol gateway for the producer and consumer
            var protocolGateway = new ProtocolGateway(_kafkaUri);
            var partitionId = 0;
            var topic = _topic + Guid.NewGuid();

            ManualConsumer consumer = new ManualConsumer(partitionId, topic, protocolGateway, "TestClient", DefaultMaxMessageSetSize);

            var offest = 5;

            await consumer.UpdateOrCreateOffset(null, offest);
        }

        [Test]
        [ExpectedException(typeof(ArgumentNullException))]
        public async Task UpdateOrCreateOffsetConsumerGroupEmptyTest()
        {
            // Creating a broker router and a protocol gateway for the producer and consumer
            var protocolGateway = new ProtocolGateway(_kafkaUri);
            var partitionId = 0;
            var topic = _topic + Guid.NewGuid();

            ManualConsumer consumer = new ManualConsumer(partitionId, topic, protocolGateway, "TestClient", DefaultMaxMessageSetSize);

            var offest = 5;

            await consumer.UpdateOrCreateOffset(string.Empty, offest);
        }

        [Test]
        [ExpectedException(typeof(ArgumentOutOfRangeException))]
        public async Task UpdateOrCreateOffsetNegativeOffsetTest()
        {
            // Creating a broker router and a protocol gateway for the producer and consumer
            var protocolGateway = new ProtocolGateway(_kafkaUri);
            var partitionId = 0;
            var topic = _topic + Guid.NewGuid();
            var consumerGroup = _defaultConsumerGroup;

            ManualConsumer consumer = new ManualConsumer(partitionId, topic, protocolGateway, "TestClient", DefaultMaxMessageSetSize);

            var offest = -5;

            await consumer.UpdateOrCreateOffset(consumerGroup, offest);
        }

        [Test]
        public async Task FetchLastOffsetSimpleTest()
        {
            // Creating a broker router and a protocol gateway for the producer and consumer
            var protocolGateway = new ProtocolGateway(_kafkaUri);
            var partitionId = 1;
            var topic = _topic;

            ManualConsumer consumer = new ManualConsumer(partitionId, topic, protocolGateway, "TestClient", DefaultMaxMessageSetSize * 2);

            var offset = await consumer.FetchLastOffset();

            Assert.AreNotEqual(-1, offset);
        }

        [Test]
        [ExpectedException(typeof(InvalidPartitionException))]
        public async Task FetchLastOffsetPartitionDoesntExistTest()
        {
            // Creating a broker router and a protocol gateway for the producer and consumer
            var protocolGateway = new ProtocolGateway(_kafkaUri);
            var partitionId = 100;
            var topic = _topic;

            ManualConsumer consumer = new ManualConsumer(partitionId, topic, protocolGateway, "TestClient", DefaultMaxMessageSetSize * 2);

            await consumer.FetchLastOffset();
        }

        [Test]
        [Ignore]
        public async Task FetchLastOffsetTopicDoesntExistTest()
        {
            // Creating a broker router and a protocol gateway for the producer and consumer
            var protocolGateway = new ProtocolGateway(_kafkaUri);
            var partitionId = 1;
            var topic = _topic + Guid.NewGuid();

            ManualConsumer consumer = new ManualConsumer(partitionId, topic, protocolGateway, "TestClient", DefaultMaxMessageSetSize * 2);

            var res = await consumer.FetchLastOffset();

            Assert.AreEqual(0, res);
        }

        private void CheckMessages(List<Message> expected, List<Message> actual)
        {
            Assert.AreEqual(expected.Count(), actual.Count(), "Didn't get all messages");

            foreach (var message in expected)
            {
                Assert.IsTrue(actual.Any(m => m.Value.SequenceEqual(message.Value)), "Didn't get the same messages");
            }
        }

        private List<Message> CreateTestMessages(int amount, int messageSize)
        {
            List<Message> messages = new List<Message>();

            for (int i = 1; i <= amount; i++)
            {
                List<byte> payload = new List<byte>(messageSize);

                for (int j = 0; j < messageSize; j++)
                {
                    payload.Add(Convert.ToByte(1));
                }

                messages.Add(new Message() { Value = payload.ToArray() });
            }

            return messages;
        }
    }
}
