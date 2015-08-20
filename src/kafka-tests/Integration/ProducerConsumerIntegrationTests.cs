using kafka_tests.Helpers;
using KafkaNet;
using KafkaNet.Common;
using KafkaNet.Model;
using KafkaNet.Protocol;
using NUnit.Framework;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading.Tasks;

namespace kafka_tests.Integration
{
    [TestFixture]
    [Category("Integration")]
    public class ProducerConsumerTests
    {
        [Test, Repeat(IntegrationConfig.NumberOfRepeat)]
        [TestCase(10, 1000)]
        [TestCase(100, 1000)]
        [TestCase(1000, 1000)]
        public async Task SendAsyncShouldHandleHighVolumeOfMessages(int amount, int maxAsync)
        {
            using (var router = new BrokerRouter(new KafkaOptions(IntegrationConfig.IntegrationUri)))
            using (var producer = new Producer(router, maxAsync) { BatchSize = amount / 2 })
            {
                var tasks = new Task<List<ProduceResponse>>[amount];

                for (var i = 0; i < amount; i++)
                {
                    tasks[i] = producer.SendMessageAsync(IntegrationConfig.IntegrationTopic,
                        new[] { new Message(Guid.NewGuid().ToString()) });
                }
                var results = await Task.WhenAll(tasks.ToArray());

                //Because of how responses are batched up and sent to servers, we will usually get multiple responses per requested message batch
                //So this assertion will never pass
                //Assert.That(results.Count, Is.EqualTo(amount));

                Assert.That(results.Any(x => x.Any(y => y.Error != 0)), Is.False,
                    "Should not have received any results as failures.");
            }
        }

        [Test, Repeat(IntegrationConfig.NumberOfRepeat)]
        public void ConsumerShouldConsumeInSameOrderAsProduced()
        {
            var expected = new List<string> { "0", "1", "2", "3", "4", "5", "6", "7", "8", "9", "10", "11", "12", "13", "14", "15", "16", "17", "18", "19" };
            var testId = Guid.NewGuid().ToString();

            using (var router = new BrokerRouter(new KafkaOptions(IntegrationConfig.IntegrationUri) { Log = IntegrationConfig.NoDebugLog }))
            using (var producer = new Producer(router))
            {
                var offsets = producer.GetTopicOffsetAsync(IntegrationConfig.IntegrationTopic).Result;

                using (var consumer = new Consumer(new ConsumerOptions(IntegrationConfig.IntegrationTopic, router) { MaxWaitTimeForMinimumBytes = TimeSpan.Zero },
                    offsets.Select(x => new OffsetPosition(x.PartitionId, x.Offsets.Max())).ToArray()))
                {
                    for (int i = 0; i < 20; i++)
                    {
                        producer.SendMessageAsync(IntegrationConfig.IntegrationTopic, new[] { new Message(i.ToString(), testId) }).Wait();
                    }

                    var results = consumer.Consume().Take(20).ToList();

                    //ensure the produced messages arrived
                    IntegrationConfig.NoDebugLog.InfoFormat("Message order:  {0}", string.Join(", ", results.Select(x => x.Value.ToUtf8String()).ToList()));

                    Assert.That(results.Count, Is.EqualTo(20));
                    Assert.That(results.Select(x => x.Value.ToUtf8String()).ToList(), Is.EqualTo(expected), "Expected the message list in the correct order.");
                    Assert.That(results.Any(x => x.Key.ToUtf8String() != testId), Is.False);
                }
            }
        }

        /// <summary>
        /// order Should remain in the same ack leve and partition
        /// </summary>
        /// <returns></returns>
        public async Task ConsumerShouldConsumeInSameOrderAsAsyncProduced()
        {
            int partition = 0;
            int numberOfMessage = 200;
            IntegrationConfig.NoDebugLog.InfoFormat(IntegrationConfig.Highlight("create BrokerRouter"));
            var router = new BrokerRouter(new KafkaOptions(IntegrationConfig.IntegrationUri));
            int causesRaceConditionOldVersion = 2;
            var producer = new Producer(router, causesRaceConditionOldVersion) { BatchDelayTime = TimeSpan.Zero };//this is slow on purpose
            //this is not slow  var producer = new Producer(router);
            IntegrationConfig.NoDebugLog.InfoFormat(IntegrationConfig.Highlight("create producer"));
            List<OffsetResponse> offsets = await producer.GetTopicOffsetAsync(IntegrationConfig.IntegrationTopic);
            IntegrationConfig.NoDebugLog.InfoFormat(IntegrationConfig.Highlight("request Offset"));
            List<Task> sendList = new List<Task>(numberOfMessage);
            for (int i = 0; i < numberOfMessage; i++)
            {
                var sendTask = producer.SendMessageAsync(IntegrationConfig.IntegrationTopic, new[] { new Message(i.ToString()) }, 1, null, MessageCodec.CodecNone, partition);
                sendList.Add(sendTask);
            }

            await Task.WhenAll(sendList.ToArray());
            IntegrationConfig.NoDebugLog.InfoFormat(IntegrationConfig.Highlight("done send"));

            IntegrationConfig.NoDebugLog.InfoFormat(IntegrationConfig.Highlight("create Consumer"));
            ConsumerOptions consumerOptions = new ConsumerOptions(IntegrationConfig.IntegrationTopic, router);
            consumerOptions.PartitionWhitelist = new List<int> { partition };

            Consumer consumer = new Consumer(consumerOptions, offsets.Select(x => new OffsetPosition(x.PartitionId, x.Offsets.Max())).ToArray());

            int expected = 0;
            IntegrationConfig.NoDebugLog.InfoFormat(IntegrationConfig.Highlight("start Consume"));
            await Task.Run((() =>
            {
                var results = consumer.Consume().Take(numberOfMessage).ToList();
                Assert.IsTrue(results.Count() == numberOfMessage, "not Consume all ,messages");
                IntegrationConfig.NoDebugLog.InfoFormat(IntegrationConfig.Highlight("done Consume"));

                foreach (Message message in results)
                {
                    Assert.That(message.Value.ToUtf8String(), Is.EqualTo(expected.ToString()),
                        "Expected the message list in the correct order.");
                    expected++;
                }
            }));
            IntegrationConfig.NoDebugLog.InfoFormat(IntegrationConfig.Highlight("start producer Dispose"));
            producer.Dispose();
            IntegrationConfig.NoDebugLog.InfoFormat(IntegrationConfig.Highlight("start consumer Dispose"));
            consumer.Dispose();
            IntegrationConfig.NoDebugLog.InfoFormat(IntegrationConfig.Highlight("start router Dispose"));
            router.Dispose();
        }

        [Test, Repeat(IntegrationConfig.NumberOfRepeat)]
        [TestCase(1000, 70)]
        [TestCase(30000, 600)]
        [TestCase(50000, 950)]
        [TestCase(200000, 7300)]
        public async Task ConsumerShouldConsumeInSameOrderAsAsyncProduced_dataLoad(int numberOfMessage, int timeoutInMs)
        {
            int partition = 0;
            Stopwatch stopwatch = new Stopwatch();
            stopwatch.Start();

            IntegrationConfig.NoDebugLog.InfoFormat(IntegrationConfig.Highlight("create BrokerRouter ,time Milliseconds:{0}", stopwatch.ElapsedMilliseconds));
            var router = new BrokerRouter(new KafkaOptions(IntegrationConfig.IntegrationUri) { Log = IntegrationConfig.NoDebugLog });
            stopwatch.Restart();
            var producer = new Producer(router) { BatchDelayTime = TimeSpan.FromMilliseconds(10), BatchSize = numberOfMessage / 10 };
            IntegrationConfig.NoDebugLog.InfoFormat(IntegrationConfig.Highlight("create producer ,time Milliseconds:{0}", stopwatch.ElapsedMilliseconds));
            stopwatch.Restart();
            List<OffsetResponse> offsets = await producer.GetTopicOffsetAsync(IntegrationConfig.IntegrationTopic);
            IntegrationConfig.NoDebugLog.InfoFormat(IntegrationConfig.Highlight("request Offset,time Milliseconds:{0}", stopwatch.ElapsedMilliseconds));
            stopwatch.Restart();
            List<Task> sendList = new List<Task>(numberOfMessage);
            for (int i = 0; i < numberOfMessage; i++)
            {
                var sendTask = producer.SendMessageAsync(IntegrationConfig.IntegrationTopic, new[] { new Message(i.ToString()) }, 1, null, MessageCodec.CodecNone, partition);
                sendList.Add(sendTask);
            }
            TimeSpan maxTimeToRun = TimeSpan.FromMilliseconds(timeoutInMs);
            var doneSend = Task.WhenAll(sendList.ToArray());
            await Task.WhenAny(doneSend, Task.Delay(maxTimeToRun));
            Assert.IsTrue(doneSend.IsCompleted, "not done to send in time");

            IntegrationConfig.NoDebugLog.InfoFormat(IntegrationConfig.Highlight("done send ,time Milliseconds:{0}", stopwatch.ElapsedMilliseconds));
            stopwatch.Restart();

            ConsumerOptions consumerOptions = new ConsumerOptions(IntegrationConfig.IntegrationTopic, router);
            consumerOptions.PartitionWhitelist = new List<int> { partition };
            consumerOptions.MaxWaitTimeForMinimumBytes = TimeSpan.Zero;
            Consumer consumer = new Consumer(consumerOptions, offsets.Select(x => new OffsetPosition(x.PartitionId, x.Offsets.Max())).ToArray());

            int expected = 0;
            IntegrationConfig.NoDebugLog.InfoFormat(IntegrationConfig.Highlight("start Consume ,time Milliseconds:{0}", stopwatch.ElapsedMilliseconds));

            IEnumerable<Message> messages = null;
            var doneConsume = Task.Run((() =>
             {
                 stopwatch.Restart();
                 messages = consumer.Consume().Take(numberOfMessage).ToArray();
                 IntegrationConfig.NoDebugLog.InfoFormat(IntegrationConfig.Highlight("done Consume ,time Milliseconds:{0}", stopwatch.ElapsedMilliseconds));
                 stopwatch.Restart();
             }));

            await Task.WhenAny(doneConsume, Task.Delay(maxTimeToRun));

            Assert.IsTrue(doneConsume.IsCompleted, "not done to Consume in time");
            Assert.IsTrue(messages.Count() == numberOfMessage, "not Consume all ,messages");

            foreach (Message message in messages)
            {
                Assert.That(message.Value.ToUtf8String(), Is.EqualTo(expected.ToString()),
                    "Expected the message list in the correct order.");
                expected++;
            }
            stopwatch.Restart();
            producer.Dispose();
            IntegrationConfig.NoDebugLog.InfoFormat(IntegrationConfig.Highlight("start producer Dispose ,time Milliseconds:{0}", stopwatch.ElapsedMilliseconds));

            consumer.Dispose();
            IntegrationConfig.NoDebugLog.InfoFormat(IntegrationConfig.Highlight("start consumer Dispose ,time Milliseconds:{0}", stopwatch.ElapsedMilliseconds));
            stopwatch.Restart();

            router.Dispose();
            IntegrationConfig.NoDebugLog.InfoFormat(IntegrationConfig.Highlight("start router Dispose,time Milliseconds:{0}", stopwatch.ElapsedMilliseconds));
        }

        [Test, Repeat(IntegrationConfig.NumberOfRepeat)]
        public void ConsumerShouldBeAbleToSeekBackToEarlierOffset()
        {
            var expected = new List<string> { "0", "1", "2", "3", "4", "5", "6", "7", "8", "9", "10", "11", "12", "13", "14", "15", "16", "17", "18", "19" };
            var testId = Guid.NewGuid().ToString();

            using (var router = new BrokerRouter(new KafkaOptions(IntegrationConfig.IntegrationUri)))
            using (var producer = new Producer(router))
            {
                var offsets = producer.GetTopicOffsetAsync(IntegrationConfig.IntegrationTopic).Result
                    .Select(x => new OffsetPosition(x.PartitionId, x.Offsets.Max())).ToArray();

                using (var consumer = new Consumer(new ConsumerOptions(IntegrationConfig.IntegrationTopic, router) { MaxWaitTimeForMinimumBytes = TimeSpan.Zero }, offsets))
                {
                    for (int i = 0; i < 20; i++)
                    {
                        producer.SendMessageAsync(IntegrationConfig.IntegrationTopic, new[] { new Message(i.ToString(), testId) }).Wait();
                    }

                    var sentMessages = consumer.Consume().Take(20).ToList();

                    //ensure the produced messages arrived
                    IntegrationConfig.NoDebugLog.InfoFormat(IntegrationConfig.Highlight("Message order:  {0}", string.Join(", ", sentMessages.Select(x => x.Value.ToUtf8String()).ToList())));

                    Assert.That(sentMessages.Count, Is.EqualTo(20));
                    Assert.That(sentMessages.Select(x => x.Value.ToUtf8String()).ToList(), Is.EqualTo(expected));
                    Assert.That(sentMessages.Any(x => x.Key.ToUtf8String() != testId), Is.False);

                    //seek back to initial offset
                    consumer.SetOffsetPosition(offsets);

                    var resetPositionMessages = consumer.Consume().Take(20).ToList();

                    //ensure all produced messages arrive again
                    IntegrationConfig.NoDebugLog.InfoFormat(IntegrationConfig.Highlight("Message order:  {0}", string.Join(", ", resetPositionMessages.Select(x => x.Value).ToList())));

                    Assert.That(resetPositionMessages.Count, Is.EqualTo(20));
                    Assert.That(resetPositionMessages.Select(x => x.Value.ToUtf8String()).ToList(), Is.EqualTo(expected));
                    Assert.That(resetPositionMessages.Any(x => x.Key.ToUtf8String() != testId), Is.False);
                }
            }
        }

        [Test, Repeat(IntegrationConfig.NumberOfRepeat)]
        public void ConsumerShouldBeAbleToGetCurrentOffsetInformation()
        {
            using (var router = new BrokerRouter(new KafkaOptions(IntegrationConfig.IntegrationUri)))
            using (var producer = new Producer(router))
            {
                var startOffsets = producer.GetTopicOffsetAsync(IntegrationConfig.IntegrationTopic).Result
                    .Select(x => new OffsetPosition(x.PartitionId, x.Offsets.Max())).ToArray();

                using (var consumer = new Consumer(new ConsumerOptions(IntegrationConfig.IntegrationTopic, router) { MaxWaitTimeForMinimumBytes = TimeSpan.Zero }, startOffsets))
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

        [Test, Repeat(IntegrationConfig.NumberOfRepeat)]
        public void ProducerShouldUsePartitionIdInsteadOfMessageKeyToChoosePartition()
        {
            using (var router = new BrokerRouter(new KafkaOptions(IntegrationConfig.IntegrationUri)))
            using (var producer = new Producer(router))
            {
                var offsets = producer.GetTopicOffsetAsync(IntegrationConfig.IntegrationTopic).Result;
                int partitionId = 0;
                string key = GetKey_PartitionIdChosenByKeyIsNotEqualToPartitionId(router, partitionId);

                //message should send to PartitionId and not use the key to Select Broker Route !!
                for (int i = 0; i < 20; i++)
                {
                    producer.SendMessageAsync(IntegrationConfig.IntegrationTopic, new[] { new Message(i.ToString(), key) }, 1, null, MessageCodec.CodecNone, partitionId).Wait();
                }

                //consume form partitionId to verify that date is send to currect partion !!.
                using (var consumer = new Consumer(new ConsumerOptions(IntegrationConfig.IntegrationTopic, router) { PartitionWhitelist = { partitionId } },
                    offsets.Select(x => new OffsetPosition(x.PartitionId, x.Offsets.Max())).ToArray()))
                {
                    for (int i = 0; i < 20; i++)
                    {
                        var result = consumer.Consume().Take(1).First();
                        Assert.That(result.Key.ToUtf8String(), Is.EqualTo(key));
                        Assert.That(result.Value.ToUtf8String(), Is.EqualTo(i.ToString()));
                    }
                }
            }
        }

        private static string GetKey_PartitionIdChosenByKeyIsNotEqualToPartitionId(BrokerRouter router,
            int partitionId)
        {
            string key = null;
            bool partitionEqualsPartitionIdChoosesByKey = true;
            while (partitionEqualsPartitionIdChoosesByKey)
            {
                var guidKey = Guid.NewGuid();
                key = guidKey.ToString();
                int partitionIdChoosesByKey = router.SelectBrokerRouteFromLocalCache(IntegrationConfig.IntegrationTopic, guidKey.ToByteArray()).PartitionId;
                partitionEqualsPartitionIdChoosesByKey = partitionId == partitionIdChoosesByKey;
            }
            return key;
        }

        [Test, Repeat(IntegrationConfig.NumberOfRepeat)]
        public void ConsumerShouldNotLoseMessageWhenBlocked()
        {
            var testId = Guid.NewGuid().ToString();

            using (var router = new BrokerRouter(new KafkaOptions(IntegrationConfig.IntegrationUri)))
            using (var producer = new Producer(router))
            {
                var offsets = producer.GetTopicOffsetAsync(IntegrationConfig.IntegrationTopic).Result;

                //create consumer with buffer size of 1 (should block upstream)
                using (var consumer = new Consumer(new ConsumerOptions(IntegrationConfig.IntegrationTopic, router) { ConsumerBufferSize = 1, MaxWaitTimeForMinimumBytes = TimeSpan.Zero },
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

        [Test, Repeat(IntegrationConfig.NumberOfRepeat)]
        public async void ConsumerShouldMoveToNextAvailableOffsetWhenQueryingForNextMessage()
        {
            const int expectedCount = 1000;
            var options = new KafkaOptions(IntegrationConfig.IntegrationUri) { Log = new DefaultTraceLog() };

            using (var producerRouter = new BrokerRouter(options))
            using (var producer = new Producer(producerRouter))
            {
                //get current offset and reset consumer to top of log
                var offsets = await producer.GetTopicOffsetAsync(IntegrationConfig.IntegrationTopic).ConfigureAwait(false);

                using (var consumerRouter = new BrokerRouter(options))
                using (var consumer = new Consumer(new ConsumerOptions(IntegrationConfig.IntegrationTopic, consumerRouter) { MaxWaitTimeForMinimumBytes = TimeSpan.Zero },
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