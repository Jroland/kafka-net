using System;
using System.Collections.Generic;
using System.Configuration;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using KafkaNet;
using KafkaNet.Model;
using NUnit.Framework;

namespace kafka_tests.Integration
{
    [TestFixture]
    public class KafkaClientTests
    {
        private KafkaClient _client;

        [SetUp]
        public void Setup()
        {
            var connection = new Uri(ConfigurationManager.AppSettings["IntegrationKafkaServerUrl"]);
            _client = new KafkaClient(connection);
        }

        [Test]
        [Ignore]
        public void EnsureRequestCanRecoverFromDisconnection()
        {
            
        }

        [Test]
        public void EnsureTwoRequestsCanCallOneAfterAnother()
        {
            var result1 = _client.SendAsync(new MetadataRequest { CorrelationId = 1 }).Result;
            var result2 = _client.SendAsync(new MetadataRequest { CorrelationId = 2 }).Result;
            Assert.That(result1.CorrelationId, Is.EqualTo(1));
            Assert.That(result2.CorrelationId, Is.EqualTo(2));
        }

        [Test]
        public void EnsureAsyncRequestsResponsesCorrelate()
        {
            var result1 = _client.SendAsync(new MetadataRequest { CorrelationId = 1 });
            var result2 = _client.SendAsync(new MetadataRequest { CorrelationId = 2 });
            var result3 = _client.SendAsync(new MetadataRequest { CorrelationId = 3 });
            
            Assert.That(result1.Result.CorrelationId, Is.EqualTo(1));
            Assert.That(result2.Result.CorrelationId, Is.EqualTo(2));
            Assert.That(result3.Result.CorrelationId, Is.EqualTo(3));
        }

        [Test]
        public void EnsureMultipleAsyncRequestsCanReadResponses()
        {
            var requests = new List<Task<MetadataResponse>>();
            var singleResult = _client.SendAsync(new MetadataRequest{CorrelationId = 99}).Result;
            Assert.That(singleResult.CorrelationId, Is.EqualTo(99));

            for (int i = 0; i < 20; i++)
            {
                requests.Add(_client.SendAsync(new MetadataRequest { CorrelationId = i }));
            }

            var results = requests.Select(x => x.Result).ToList();
            Assert.That(results.Count, Is.EqualTo(20));
         }
    }
}
