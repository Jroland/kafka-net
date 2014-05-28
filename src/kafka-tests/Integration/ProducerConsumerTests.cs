using System;
using System.Collections.Generic;
using System.Configuration;
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
    public class ProducerConsumerTests
    {
        private BrokerRouter _router;

        [SetUp]
        public void Setup()
        {
            var brokers = ConfigurationManager.AppSettings["IntegrationKafkaServerUrl"]
                .Split(',')
                .Select(b => new Uri(b))
                .ToArray();

            var options = new KafkaOptions(brokers);

            _router = new BrokerRouter(options);
        }

        [Test]
        [TestCase(10, -1)]
        [TestCase(100, -1)]
        [TestCase(1000, -1)]
        [TestCase(10000, 100)]

        public void SendAsyncShouldHandleHighVolumeOfMessages(int amount, int maxAsync)
        {
            var tasks = new Task<List<ProduceResponse>>[amount];
            var producer = new Producer(_router, maxAsync);

            for (var i = 0; i < amount; i++)
            {
                tasks[i] = producer.SendMessageAsync("LoadTest", new Message[] { new Message { Value = Guid.NewGuid().ToString() } });
            }

            var results = tasks.SelectMany(x => x.Result).ToList();

            Assert.That(results.Count, Is.EqualTo(amount));
            Assert.That(results.Any(x => x.Error != 0), Is.False);
        }

        [Test]
        public void ConsumerShouldConsumeInSameOrderAsProduced()
        {
            var producer = new Producer(_router);

            var offsets = producer.GetTopicOffsetAsync("LoadTest").Result;

            var consumer = new Consumer(new ConsumerOptions("LoadTest", _router),
                offsets.Select(x => new OffsetPosition(x.PartitionId, x.Offsets.Max())).ToArray());

            var tasks = new List<Task<List<ProduceResponse>>>();
            for (int i = 0; i < 20; i++)
            {
                tasks.Add(producer.SendMessageAsync("LoadTest", new[] { new Message { Value = i.ToString(), Key = "1" } }));
            }
            Task.WaitAll(tasks.ToArray());

            var results = consumer.Consume().Take(20).ToList();

            for (int i = 0; i < 20; i++)
            {
                Assert.That(results[i].Value == i.ToString());
            }
        }

        [Test]
        public void ConsumerShouldBeAbleToSeekBackToEarlierOffset()
        {
            var producer = new Producer(_router);

            var offsets = producer.GetTopicOffsetAsync("LoadTest").Result
                .Select(x => new OffsetPosition(x.PartitionId, x.Offsets.Max())).ToArray();

            var consumer = new Consumer(new ConsumerOptions("LoadTest", _router), offsets);

            var tasks = new List<Task<List<ProduceResponse>>>();
            for (int i = 0; i < 20; i++)
            {
                tasks.Add(producer.SendMessageAsync("LoadTest", new[] { new Message { Value = i.ToString(), Key = "1" } }));
            }
            Task.WaitAll(tasks.ToArray());

            var results = consumer.Consume().Take(20).ToList();

            //ensure the produced messages arrived
            for (int i = 0; i < 20; i++)
            {
                Assert.That(results[i].Value == i.ToString());
            }

            //seek back to initial offset
            consumer.SetOffsetPosition(offsets);

            //ensure all produced messages arrive again
            for (int i = 0; i < 20; i++)
            {
                Assert.That(results[i].Value == i.ToString());
            }
        }

        [Test]
        public void ConsumerShouldBeAbleToGetCurrentOffsetInformation()
        {
            var producer = new Producer(_router);

            var startOffsets = producer.GetTopicOffsetAsync("LoadTest").Result
                .Select(x => new OffsetPosition(x.PartitionId, x.Offsets.Max())).ToArray();

            var consumer = new Consumer(new ConsumerOptions("LoadTest", _router), startOffsets);

            var tasks = new List<Task<List<ProduceResponse>>>();
            for (int i = 0; i < 20; i++)
            {
                tasks.Add(producer.SendMessageAsync("LoadTest", new[] { new Message { Value = i.ToString(), Key = "1" } }));
            }
            Task.WaitAll(tasks.ToArray());

            var results = consumer.Consume().Take(20).ToList();

            //ensure the produced messages arrived
            for (int i = 0; i < 20; i++)
            {
                Assert.That(results[i].Value == i.ToString());
            }

            //the current offsets should be 20 positions higher than start
            var currentOffsets = consumer.GetOffsetPosition();         
            Assert.That(currentOffsets.Sum(x => x.Offset) - startOffsets.Sum(x => x.Offset), Is.EqualTo(20));
        }
    }
}

