using kafka_tests.Helpers;
using KafkaNet;
using KafkaNet.Model;
using KafkaNet.Protocol;
using NUnit.Framework;
using System;
using System.Linq;
using System.Threading.Tasks;

namespace kafka_tests.Integration
{
    [TestFixture]
    [Category("Integration")]
    public class KafkaMetadataProviderUnitTests
    {
        private readonly KafkaOptions _options = new KafkaOptions(IntegrationConfig.IntegrationUri);

        private KafkaConnection GetKafkaConnection()
        {
            var endpoint = new DefaultKafkaConnectionFactory().Resolve(_options.KafkaServerUri.First(), _options.Log);
            return new KafkaConnection(new KafkaTcpSocket(new DefaultTraceLog(), endpoint), _options.ResponseTimeoutMs, _options.Log);
        }

        [Test, Repeat(IntegrationConfig.NumberOfRepeat)]
        public async Task NewlyCreatedTopicShouldRetryUntilBrokerIsAssigned()
        {
            var expectedTopic = Guid.NewGuid().ToString();
            var repo = new KafkaMetadataProvider(_options.Log);
            var response = repo.Get(new[] { GetKafkaConnection() }, new[] { expectedTopic });
            var topic = (await response).Topics.FirstOrDefault();

            Assert.That(topic, Is.Not.Null);
            Assert.That(topic.Name, Is.EqualTo(expectedTopic));
            Assert.That(topic.ErrorCode, Is.EqualTo((int)ErrorResponseCode.NoError));
        }
    }
}