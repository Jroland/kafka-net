using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
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
        private KafkaOptions _options;
        private Uri _kafkaUri;

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
            ManualConsumer consumer = new ManualConsumer(partitionId, topic, protocolGateway);

            var offset = await consumer.GetLastOffset();

            // Creating 5 messages
            List<Message> messages = CreateTestMessages(5);

            await producer.SendMessageAsync(topic, messages, partition: partitionId, timeout:TimeSpan.FromSeconds(3));

            // Now let's consume
            var result = (await consumer.GetMessages(5, offset, TimeSpan.FromSeconds(3))).ToList();

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
            ManualConsumer consumer = new ManualConsumer(partitionId, topic, protocolGateway);

            var offset = await consumer.GetLastOffset();

            // Creating 5 messages
            List<Message> messages = CreateTestMessages(10);

            await producer.SendMessageAsync(topic, messages, partition: partitionId, timeout: TimeSpan.FromSeconds(3));                       

            // Now let's consume
            var result = (await consumer.GetMessages(5, offset, TimeSpan.FromSeconds(3))).ToList();

            CheckMessages(messages.Take(5).ToList(), result);

            // Now let's consume again
            result = (await consumer.GetMessages(5, offset + 5, TimeSpan.FromSeconds(3))).ToList();

            CheckMessages(messages.Skip(5).ToList(), result);
        }

        private void CheckMessages(List<Message> expected, List<Message> actual)
        {
            Assert.AreEqual(expected.Count(), actual.Count(), "Didn't get all messages");

            foreach (var message in expected)
            {
                Assert.IsTrue(actual.Any(m => m.Value[0] == message.Value[0]), "Didn't get the same messages");
            }
        }

        private List<Message> CreateTestMessages(int amount)
        {
            List<Message> messages = new List<Message>();

            for (int i = 1; i <= amount; i++)
            {
                messages.Add(new Message() { Value = new byte[] { Convert.ToByte(i) } });
            }

            return messages;
        }
    }
}
