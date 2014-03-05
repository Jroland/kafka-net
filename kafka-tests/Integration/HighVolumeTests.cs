using System;
using System.Collections.Generic;
using System.Configuration;
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
    public class HighVolumeTests
    {
        private BrokerRouter _router;

        [SetUp]
        public void Setup()
        {
            var options = new KafkaOptions(new Uri(ConfigurationManager.AppSettings["IntegrationKafkaServerUrl"]));

            _router = new BrokerRouter(options);
        }

        [Test]
        public void SendAsyncShouldHandle100KMessages()
        {
            var tasks = new Task<List<ProduceResponse>>[100000];
            var producer = new Producer(_router);

            for (var i = 0; i < 100000; i++)
            {
                tasks[i] = producer.SendMessageAsync("LoadTest", new Message[] {new Message {Value = Guid.NewGuid().ToString()}});
            }

            var results = tasks.SelectMany(x => x.Result).ToList();

            Assert.That(results.Count, Is.EqualTo(100000));
            Assert.That(results.Any(x => x.Error != 0), Is.False);
        }
    }
}
