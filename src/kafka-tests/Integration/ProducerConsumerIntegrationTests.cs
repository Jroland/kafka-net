using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using KafkaNet;
using KafkaNet.Model;
using KafkaNet.Protocol;
using NUnit.Framework;
using kafka_tests.Helpers;

namespace kafka_tests.Integration
{
    [TestFixture]
    [Category("Integration")]
    public class ProducerConsumerTests
    {
        [Test]
        [TestCase(10, -1)]
        [TestCase(100, -1)]
        [TestCase(1000, -1)]
        public void SendAsyncShouldHandleHighVolumeOfMessages(int amount, int maxAsync)
        {
            using (var router = new BrokerRouter(new KafkaOptions(IntegrationConfig.IntegrationUri)))
            using (var producer = new Producer(router, maxAsync))
            {
                var tasks = new Task<List<ProduceResponse>>[amount];

                for (var i = 0; i < amount; i++)
                {
                    tasks[i] = producer.SendMessageAsync(IntegrationConfig.IntegrationTopic, new[] { new Message { Value = Guid.NewGuid().ToString() } });
                }

                var results = tasks.SelectMany(x => x.Result).ToList();

                Assert.That(results.Count, Is.EqualTo(amount));
                Assert.That(results.Any(x => x.Error != 0), Is.False);
            }
        }

        [Test]
        public void ConsumerShouldConsumeInSameOrderAsProduced()
        {
            var expected = new List<string> { "0", "1", "2", "3", "4", "5", "6", "7", "8", "9", "10", "11", "12", "13", "14", "15", "16", "17", "18", "19" };
            var testId = Guid.NewGuid().ToString();

            using (var router = new BrokerRouter(new KafkaOptions(IntegrationConfig.IntegrationUri)))
            using (var producer = new Producer(router))
            {

                var offsets = producer.GetTopicOffsetAsync(IntegrationConfig.IntegrationTopic).Result;

                using (var consumer = new Consumer(new ConsumerOptions(IntegrationConfig.IntegrationTopic, router),
                    offsets.Select(x => new OffsetPosition(x.PartitionId, x.Offsets.Max())).ToArray()))
                {

                    for (int i = 0; i < 20; i++)
                    {
                        producer.SendMessageAsync(IntegrationConfig.IntegrationTopic, new[] { new Message { Value = i.ToString(), Key = testId } }).Wait();
                    }

                    var results = consumer.Consume().Take(20).ToList();

                    //ensure the produced messages arrived
                    Console.WriteLine("Message order:  {0}", string.Join(", ", results.Select(x => x.Value).ToList()));

                    Assert.That(results.Count, Is.EqualTo(20));
                    Assert.That(results.Select(x => x.Value).ToList(), Is.EqualTo(expected));
                    Assert.That(results.Any(x => x.Key != testId), Is.False);
                }
            }
        }

        [Test]
        public void ConsumerShouldBeAbleToSeekBackToEarlierOffset()
        {
            var expected = new List<string> { "0", "1", "2", "3", "4", "5", "6", "7", "8", "9", "10", "11", "12", "13", "14", "15", "16", "17", "18", "19" };
            var testId = Guid.NewGuid().ToString();

            using (var router = new BrokerRouter(new KafkaOptions(IntegrationConfig.IntegrationUri)))
            using (var producer = new Producer(router))
            {
                var offsets = producer.GetTopicOffsetAsync(IntegrationConfig.IntegrationTopic).Result
                    .Select(x => new OffsetPosition(x.PartitionId, x.Offsets.Max())).ToArray();

                using (var consumer = new Consumer(new ConsumerOptions(IntegrationConfig.IntegrationTopic, router), offsets))
                {
                    for (int i = 0; i < 20; i++)
                    {
                        producer.SendMessageAsync(IntegrationConfig.IntegrationTopic, new[] { new Message { Value = i.ToString(), Key = testId } }).Wait();
                    }

                    var sentMessages = consumer.Consume().Take(20).ToList();

                    //ensure the produced messages arrived
                    Console.WriteLine("Message order:  {0}", string.Join(", ", sentMessages.Select(x => x.Value).ToList()));

                    Assert.That(sentMessages.Count, Is.EqualTo(20));
                    Assert.That(sentMessages.Select(x => x.Value).ToList(), Is.EqualTo(expected));
                    Assert.That(sentMessages.Any(x => x.Key != testId), Is.False);

                    //seek back to initial offset
                    consumer.SetOffsetPosition(offsets);

                    var resetPositionMessages = consumer.Consume().Take(20).ToList();

                    //ensure all produced messages arrive again
                    Console.WriteLine("Message order:  {0}", string.Join(", ", resetPositionMessages.Select(x => x.Value).ToList()));

                    Assert.That(resetPositionMessages.Count, Is.EqualTo(20));
                    Assert.That(resetPositionMessages.Select(x => x.Value).ToList(), Is.EqualTo(expected));
                    Assert.That(resetPositionMessages.Any(x => x.Key != testId), Is.False);
                }
            }
        }

        [Test]
        public void ConsumerShouldBeAbleToGetCurrentOffsetInformation()
        {
            using (var router = new BrokerRouter(new KafkaOptions(IntegrationConfig.IntegrationUri)))
            using (var producer = new Producer(router))
            {

                var startOffsets = producer.GetTopicOffsetAsync(IntegrationConfig.IntegrationTopic).Result
                    .Select(x => new OffsetPosition(x.PartitionId, x.Offsets.Max())).ToArray();

                using (var consumer = new Consumer(new ConsumerOptions(IntegrationConfig.IntegrationTopic, router), startOffsets))
                {

                    for (int i = 0; i < 20; i++)
                    {
                        producer.SendMessageAsync(IntegrationConfig.IntegrationTopic, new[] { new Message { Value = i.ToString(), Key = "1" } }).Wait();
                    }

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

        [Test]
        public void ConsumerShouldNotLoseMessageWhenBlocked()
        {
            var testId = Guid.NewGuid().ToString();

            using (var router = new BrokerRouter(new KafkaOptions(IntegrationConfig.IntegrationUri)))
            using (var producer = new Producer(router))
            {
                var offsets = producer.GetTopicOffsetAsync(IntegrationConfig.IntegrationTopic).Result;

                //create consumer with buffer size of 1 (should block upstream)
                using (var consumer = new Consumer(new ConsumerOptions(IntegrationConfig.IntegrationTopic, router) { ConsumerBufferSize = 1 },
                    offsets.Select(x => new OffsetPosition(x.PartitionId, x.Offsets.Max())).ToArray()))
                {

                    for (int i = 0; i < 20; i++)
                    {
                        producer.SendMessageAsync(IntegrationConfig.IntegrationTopic, new[] { new Message { Value = i.ToString(), Key = testId } }).Wait();
                    }

                    for (int i = 0; i < 20; i++)
                    {
                        var result = consumer.Consume().Take(1).First();
                        Assert.That(result.Key, Is.EqualTo(testId));
                        Assert.That(result.Value, Is.EqualTo(i.ToString()));
                    }
                }
            }
        }


        [Test]
        public void ConsumerShouldMoveToNextAvailableOffsetWhenQueryingForNextMessage()
        {
            using (var router = new BrokerRouter(new KafkaOptions(IntegrationConfig.IntegrationUri)))
            using (var producer = new Producer(router))
            {
                var offsets = producer.GetTopicOffsetAsync(IntegrationConfig.IntegrationTopic).Result;
                Assert.That(offsets.Count, Is.EqualTo(2), "This test requires there to be exactly two paritions.");
                Assert.That(offsets.Count(x => x.Offsets.Max(o => o) > 1000), Is.EqualTo(2), "Need more than 1000 messages in each topic for this test to work.");

                //set offset 1000 messages back on one partition.  We should be able to get all 1000 messages over multiple calls.
                using (var consumer = new Consumer(new ConsumerOptions(IntegrationConfig.IntegrationTopic, router),
                     offsets.Select(x => new OffsetPosition(x.PartitionId, x.Offsets.Max() - 1000)).ToArray()))
                {
                    var data = new List<Message>();

                    var stream = consumer.Consume();

                    var takeTask = Task.Factory.StartNew(() => data.AddRange(stream.Take(2000)));

                    takeTask.Wait(TimeSpan.FromSeconds(10));

                    var consumerOffset = consumer.GetOffsetPosition().OrderBy(x => x.Offset).ToList();
                    var serverOffset = offsets.Select(x => new OffsetPosition(x.PartitionId, x.Offsets.Max())).OrderBy(x => x.Offset).ToList();

                    Assert.That(consumerOffset, Is.EqualTo(serverOffset), "The consumerOffset position should match the server offset position.");
                    Assert.That(data.Count, Is.EqualTo(2000), "We should have received 2000 messages from the server.");

                }
            }
        }
    }
}
