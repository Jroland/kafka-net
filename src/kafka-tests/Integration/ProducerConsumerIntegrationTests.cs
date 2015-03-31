using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using KafkaNet;
using KafkaNet.Common;
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
        [TestCase(10, 1000)]
        [TestCase(100, 1000)]
        [TestCase(1000, 1000)]
        public void SendAsyncShouldHandleHighVolumeOfMessages(int amount, int maxAsync)
        {
            using (var router = new BrokerRouter(new KafkaOptions(IntegrationConfig.IntegrationUri)))
            using (var producer = new Producer(router, maxAsync) { BatchSize = amount / 2 })
            {
                var tasks = new Task<List<ProduceResponse>>[amount];

                for (var i = 0; i < amount; i++)
                {
                    tasks[i] = producer.SendMessageAsync(IntegrationConfig.IntegrationTopic, new[] { new Message(Guid.NewGuid().ToString()) });
                }

                var results = tasks.SelectMany(x => x.Result).ToList();

                //Because of how responses are batched up and sent to servers, we will usually get multiple responses per requested message batch
                //So this assertion will never pass
                //Assert.That(results.Count, Is.EqualTo(amount));

                Assert.That(results.Any(x => x.Error != 0), Is.False, "Should not have received any results as failures.");
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
                        producer.SendMessageAsync(IntegrationConfig.IntegrationTopic, new[] { new Message(i.ToString(), testId) }).Wait();
                    }

                    var results = consumer.Consume().Take(20).ToList();

                    //ensure the produced messages arrived
                    Console.WriteLine("Message order:  {0}", string.Join(", ", results.Select(x => x.Value.ToUtf8String()).ToList()));

                    Assert.That(results.Count, Is.EqualTo(20));
                    Assert.That(results.Select(x => x.Value.ToUtf8String()).ToList(), Is.EqualTo(expected), "Expected the message list in the correct order.");
                    Assert.That(results.Any(x => x.Key.ToUtf8String() != testId), Is.False);
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
                        producer.SendMessageAsync(IntegrationConfig.IntegrationTopic, new[] { new Message(i.ToString(), testId) }).Wait();
                    }

                    var sentMessages = consumer.Consume().Take(20).ToList();

                    //ensure the produced messages arrived
                    Console.WriteLine("Message order:  {0}", string.Join(", ", sentMessages.Select(x => x.Value.ToUtf8String()).ToList()));

                    Assert.That(sentMessages.Count, Is.EqualTo(20));
                    Assert.That(sentMessages.Select(x => x.Value.ToUtf8String()).ToList(), Is.EqualTo(expected));
                    Assert.That(sentMessages.Any(x => x.Key.ToUtf8String() != testId), Is.False);

                    //seek back to initial offset
                    consumer.SetOffsetPosition(offsets);

                    var resetPositionMessages = consumer.Consume().Take(20).ToList();

                    //ensure all produced messages arrive again
                    Console.WriteLine("Message order:  {0}", string.Join(", ", resetPositionMessages.Select(x => x.Value).ToList()));

                    Assert.That(resetPositionMessages.Count, Is.EqualTo(20));
                    Assert.That(resetPositionMessages.Select(x => x.Value.ToUtf8String()).ToList(), Is.EqualTo(expected));
                    Assert.That(resetPositionMessages.Any(x => x.Key.ToUtf8String() != testId), Is.False);
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
                        producer.SendMessageAsync(IntegrationConfig.IntegrationTopic, new[] { new Message(i.ToString(), "1") }).Wait();
                    }

                    var results = consumer.Consume().Take(20).ToList();

                    //ensure the produced messages arrived
                    for (int i = 0; i < 20; i++)
                    {
                        Assert.That(results[i].Value.ToUtf8String(), Is.EqualTo(i.ToString()));
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
                        producer.SendMessageAsync(IntegrationConfig.IntegrationTopic, new[] { new Message(i.ToString(), testId) }).Wait();
                    }

                    for (int i = 0; i < 20; i++)
                    {
                        var result = consumer.Consume().Take(1).First();
                        Assert.That(result.Key.ToUtf8String(), Is.EqualTo(testId));
                        Assert.That(result.Value.ToUtf8String(), Is.EqualTo(i.ToString()));
                    }
                }
            }
        }


        [Test]
        public async void ConsumerShouldMoveToNextAvailableOffsetWhenQueryingForNextMessage()
        {
            const int expectedCount = 1000;
            var options = new KafkaOptions(IntegrationConfig.IntegrationUri) { Log = new ConsoleLog() };

            using (var producerRouter = new BrokerRouter(options))
            using (var producer = new Producer(producerRouter))
            {
                //get current offset and reset consumer to top of log
                var offsets = await producer.GetTopicOffsetAsync(IntegrationConfig.IntegrationTopic).ConfigureAwait(false);

                using (var consumerRouter = new BrokerRouter(options))
                using (var consumer = new Consumer(new ConsumerOptions(IntegrationConfig.IntegrationTopic, consumerRouter),
                     offsets.Select(x => new OffsetPosition(x.PartitionId, x.Offsets.Max())).ToArray()))
                {
                    Console.WriteLine("Sending {0} test messages", expectedCount);
                    var response = await producer.SendMessageAsync(IntegrationConfig.IntegrationTopic, 
                        Enumerable.Range(0, expectedCount).Select(x => new Message(x.ToString())));

                    Assert.That(response.Any(x => x.Error != (int)ErrorResponseCode.NoError), Is.False, "Error occured sending test messages to server.");

                    var stream = consumer.Consume();

                    Console.WriteLine("Reading message back out from consumer.");
                    var data = stream.Take(expectedCount).ToList();

                    var consumerOffset = consumer.GetOffsetPosition().OrderBy(x => x.Offset).ToList();
                    var serverOffset = await producer.GetTopicOffsetAsync(IntegrationConfig.IntegrationTopic).ConfigureAwait(false);
                    var positionOffset = serverOffset.Select(x => new OffsetPosition(x.PartitionId, x.Offsets.Max()))
                        .OrderBy(x => x.Offset)
                        .ToList();

                    Assert.That(consumerOffset, Is.EqualTo(positionOffset), "The consumerOffset position should match the server offset position.");
                    Assert.That(data.Count, Is.EqualTo(expectedCount), "We should have received 2000 messages from the server.");

                }
            }
        }
    }
}
