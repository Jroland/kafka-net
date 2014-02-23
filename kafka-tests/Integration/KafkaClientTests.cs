//using System;
//using System.Collections.Generic;
//using System.Configuration;
//using System.Linq;
//using System.Threading.Tasks;
//using KafkaNet;
//using KafkaNet.Model;
//using NUnit.Framework;

//namespace kafka_tests.Integration
//{
//    /// <summary>
//    /// Note these integration tests require an actively running kafka server defined in the app.config file.
//    /// </summary>
//    [TestFixture]
//    public class KafkaClientTests
//    {
//        private KafkaClient _client;

//        [SetUp]
//        public void Setup()
//        {
//            var connection = new Uri(ConfigurationManager.AppSettings["IntegrationKafkaServerUrl"]);
//            _client = new KafkaClient(connection);
//        }

//        [Test]
//        [Ignore]
//        public void EnsureRequestCanRecoverFromDisconnection()
//        {

//        }

//        [Test]
//        public void EnsureTwoRequestsCanCallOneAfterAnother()
//        {
//            var result1 = _client.SendAsync(new MetadataRequest()).Result;
//            var result2 = _client.SendAsync(new MetadataRequest()).Result;
//            Assert.That(result1.CorrelationId, Is.EqualTo(1));
//            Assert.That(result2.CorrelationId, Is.EqualTo(2));
//        }

//        [Test]
//        public void EnsureAsyncRequestResponsesCorrelate()
//        {
//            var result1 = _client.SendAsync(new MetadataRequest());
//            var result2 = _client.SendAsync(new MetadataRequest());
//            var result3 = _client.SendAsync(new MetadataRequest());

//            Assert.That(result1.Result.CorrelationId, Is.EqualTo(1));
//            Assert.That(result2.Result.CorrelationId, Is.EqualTo(2));
//            Assert.That(result3.Result.CorrelationId, Is.EqualTo(3));
//        }

//        [Test]
//        public void EnsureMultipleAsyncRequestsCanReadResponses()
//        {
//            var requests = new List<Task<MetadataResponse>>();
//            var singleResult = _client.SendAsync(new MetadataRequest()).Result;
//            Assert.That(singleResult.Topics.Count, Is.GreaterThan(0));

//            for (int i = 0; i < 20; i++)
//            {
//                requests.Add(_client.SendAsync(new MetadataRequest()));
//            }

//            var results = requests.Select(x => x.Result).ToList();
//            Assert.That(results.Count, Is.EqualTo(20));
//        }

//        [Test]
//        public void EnsureDifferentTypesOfResponsesCanBeReadAsync()
//        {
//            const string ExpectedTopic = "UnitTest";
            
//            //just ensure the topic exists for this test
//            var ensureTopic = _client.SendAsync(new MetadataRequest {Topics = new List<string>(new[] {ExpectedTopic})}).Result;
//            Assert.That(ensureTopic.Topics.Count, Is.EqualTo(1));
//            Assert.That(ensureTopic.Topics.First().Name == ExpectedTopic, Is.True, "ProduceRequest did not return expected topic.");
            
//            var result1 = _client.SendAsync(RequestFactory.CreateProduceRequest(ExpectedTopic, "test"));
//            var result2 = _client.SendAsync(new MetadataRequest());
//            var result3 = _client.SendAsync(RequestFactory.CreateOffsetRequest(ExpectedTopic));
//            var result4 = _client.SendAsync(RequestFactory.CreateFetchRequest(ExpectedTopic, 0));

//            Assert.That(result1.Result.Count, Is.EqualTo(1));
//            Assert.That(result1.Result.First().Topic == ExpectedTopic, Is.True, "ProduceRequest did not return expected topic.");

//            Assert.That(result2.Result.Topics.Any(x => x.Name == ExpectedTopic), Is.True, "MetadataRequest did not return expected topic.");

//            Assert.That(result3.Result.Count, Is.EqualTo(1));
//            Assert.That(result3.Result.First().Topic == ExpectedTopic, Is.True, "OffsetRequest did not return expected topic.");

//            Assert.That(result4.Result.Count, Is.EqualTo(1));
//            Assert.That(result4.Result.First().Topic == ExpectedTopic, Is.True, "FetchRequest did not return expected topic.");

//        }
//    }
//}
