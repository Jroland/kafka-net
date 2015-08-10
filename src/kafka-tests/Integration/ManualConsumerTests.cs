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

        public ManualConsumerTests()
        {
            _kafkaUri = new Uri(KafkaUrl);
            _options = new KafkaOptions(_kafkaUri);
        }

        [Test]
        public async Task SimpleGetMessages()
        {
            // Creating a broker router and a protocol gateway for the producer and consumer
            var brokerRouter = new BrokerRouter(_options);
            var protocolGateway = new ProtocolGateway(_kafkaUri);
            var partitionId = 1;
            var topic = "ManualConsumerTestTopic";

            Producer producer = new Producer(brokerRouter);
            ManualConsumer consumer = new ManualConsumer(partitionId, topic, protocolGateway, "TestClient");

            var offset = await consumer.GetLastOffset();

            // Creating 5 messages
            List<Message> messages = CreateTestMessages(5, 1);

            await producer.SendMessageAsync(topic, messages, partition: partitionId, timeout:TimeSpan.FromSeconds(3));

            // Now let's consume
            var result = (await consumer.GetMessages(5, offset)).ToList();

            CheckMessages(messages, result);
        }

        [Test]
        public async Task GetMessagesSecondFromCache()
        {
            // Creating a broker router and a protocol gateway for the producer and consumer
            var brokerRouter = new BrokerRouter(_options);
            var protocolGateway = new ProtocolGateway(_kafkaUri);
            var partitionId = 1;
            var topic = "ManualConsumerTestTopic";

            Producer producer = new Producer(brokerRouter);
            ManualConsumer consumer = new ManualConsumer(partitionId, topic, protocolGateway, "TestClient");

            var offset = await consumer.GetLastOffset();

            // Creating 5 messages
            List<Message> messages = CreateTestMessages(10, 1);

            await producer.SendMessageAsync(topic, messages, partition: partitionId, timeout: TimeSpan.FromSeconds(3));

            // Now let's consume
            var result = (await consumer.GetMessages(5, offset)).ToList();

            CheckMessages(messages.Take(5).ToList(), result);

            // Now let's consume again
            result = (await consumer.GetMessages(5, offset + 5)).ToList();

            CheckMessages(messages.Skip(5).ToList(), result);
        }

        [Test]
        public async Task GetMessagesMakeTwoFetchRequests()
        {
            // Creating a broker router and a protocol gateway for the producer and consumer
            var brokerRouter = new BrokerRouter(_options);
            var protocolGateway = new ProtocolGateway(_kafkaUri);
            var partitionId = 1;
            var topic = "ManualConsumerTestTopic";

            Producer producer = new Producer(brokerRouter);
            ManualConsumer consumer = new ManualConsumer(partitionId, topic, protocolGateway, "TestClient");

            var offset = await consumer.GetLastOffset();

            // Creating 5 messages
            List<Message> messages = CreateTestMessages(10, 4096);

            await producer.SendMessageAsync(topic, messages, partition: partitionId, timeout: TimeSpan.FromSeconds(3));

            // Now let's consume
            var result = (await consumer.GetMessages(5, offset)).ToList();

            CheckMessages(messages.Take(5).ToList(), result);

            // Now let's consume again
            result = (await consumer.GetMessages(5, offset + 5)).ToList();

            CheckMessages(messages.Skip(5).ToList(), result);
        }

        [Test]
        public async Task GetMessagesNoNewMessagesInQueue()
        {
            // Creating a broker router and a protocol gateway for the producer and consumer
            var protocolGateway = new ProtocolGateway(_kafkaUri);
            var partitionId = 1;
            var topic = "ManualConsumerTestTopic";

            ManualConsumer consumer = new ManualConsumer(partitionId, topic, protocolGateway, "TestClient");

            var offset = await consumer.GetLastOffset();

            // Now let's consume
            var result = (await consumer.GetMessages(5, offset)).ToList();

            Assert.AreEqual(0, result.Count, "Should not get any messages");
        }

        [Test]
        [ExpectedException(typeof(KafkaApplicationException))]
        public async Task GetOffsetConsumerGroupDoesntExist()
        {
            // Creating a broker router and a protocol gateway for the producer and consumer
            var protocolGateway = new ProtocolGateway(_kafkaUri);
            var partitionId = 0;
            var topic = "ManualConsumerTestTopic";
            var consumerGroup = Guid.NewGuid().ToString();

            ManualConsumer consumer = new ManualConsumer(partitionId, topic, protocolGateway, "TestClient");

            var offset = await consumer.GetOffset(consumerGroup);

            // Now let's consume            
            Assert.AreEqual(0, offset, "Should not get any messages");
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
                    payload.Add(Convert.ToByte(i));
                }

                messages.Add(new Message() { Value = payload.ToArray() });
            }

            return messages;
        }
    }
}
