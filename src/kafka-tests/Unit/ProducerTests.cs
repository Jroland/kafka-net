using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using KafkaNet;
using KafkaNet.Protocol;
using NSubstitute;
using NUnit.Framework;
using System.Threading;
using kafka_tests.Helpers;

namespace kafka_tests.Unit
{
    [TestFixture]
    [Category("Unit")]
    public class ProducerTests
    {
        #region SendMessageAsync Tests...
        [Test]
        public void ProducerShouldGroupMessagesByBroker()
        {
            var routerProxy = new FakeBrokerRouter();
            var router = routerProxy.Create();
            using (var producer = new Producer(router))
            {
                var messages = new List<Message>
                {
                    new Message("1"), new Message("2")
                };

                var response = producer.SendMessageAsync("UnitTest", messages).Result;

                Assert.That(response.Count, Is.EqualTo(2));
                Assert.That(routerProxy.BrokerConn0.ProduceRequestCallCount, Is.EqualTo(1));
                Assert.That(routerProxy.BrokerConn1.ProduceRequestCallCount, Is.EqualTo(1));
            }
        }

        [Test]
        public void ShouldSendAsyncToAllConnectionsEvenWhenExceptionOccursOnOne()
        {
            var routerProxy = new FakeBrokerRouter();
            routerProxy.BrokerConn1.ProduceResponseFunction = () => { throw new ApplicationException("some exception"); };
            var router = routerProxy.Create();

            using (var producer = new Producer(router))
            {
                var messages = new List<Message>
                {
                    new Message("1"), new Message("2")
                };

				Assert.Throws<KafkaApplicationException>(async () =>
				{
					await producer.SendMessageAsync("UnitTest", messages).ConfigureAwait(false);
				});

                //Assert.That(t.IsFaulted, Is.True);
                //Assert.That(t.Exception, Is.Not.Null);
                //Assert.That(t.Exception.ToString(), Is.StringContaining("ApplicationException"));
                Assert.That(routerProxy.BrokerConn0.ProduceRequestCallCount, Is.EqualTo(1));
                Assert.That(routerProxy.BrokerConn1.ProduceRequestCallCount, Is.EqualTo(1));
            }
        }

        [Test]
        public void SendAsyncShouldBlockWhenMaximumAsyncQueueReached()
        {
            int count = 0;
            var semaphore = new SemaphoreSlim(0);
            var routerProxy = new FakeBrokerRouter();
            //block the second call returning from send message async
            routerProxy.BrokerConn0.ProduceResponseFunction = () => { semaphore.Wait(); return new ProduceResponse(); };

            var router = routerProxy.Create();
            using (var producer = new Producer(router, 1))
            {
                var messages = new List<Message>
                {
                    new Message("1"), new Message("2")
                };

				Task.Factory.StartNew(async () =>
				{
					var t = producer.SendMessageAsync(BrokerRouterProxy.TestTopic, messages);
					count++;
					await t;

					t = producer.SendMessageAsync(BrokerRouterProxy.TestTopic, messages);
					count++;
					await t;
				});

				TaskTest.WaitFor(() => count > 0);
                Assert.That(count, Is.EqualTo(1), "Only one SendMessageAsync should continue.");

				//This doesn't work, because the message has been pulled out of the NagleCollection's internal BlockingCollection (which is where this count comes from)
				//Instead it's sitting inside a local List<T> inside NagleBlockingCollection.TakeBatch()
				//Assert.That(producer.ActiveCount, Is.EqualTo(1), "One async call shoud be active."); 

                semaphore.Release();
                TaskTest.WaitFor(() => count > 1);
                Assert.That(count, Is.EqualTo(2), "The second SendMessageAsync should continue after semaphore is released.");
            }
        }

        [Test]
        [Ignore("is there a way to communicate back which client failed and which succeeded.")]
        public void ConnectionExceptionOnOneShouldCommunicateBackWhichMessagesFailed()
        {
            //TODO is there a way to communicate back which client failed and which succeeded.
            var routerProxy = new FakeBrokerRouter();
            routerProxy.BrokerConn1.ProduceResponseFunction = () => { throw new ApplicationException("some exception"); };

            var router = routerProxy.Create();
            using (var producer = new Producer(router))
            {
                var messages = new List<Message>
                {
                    new Message("1"), new Message("2")
                };

                //this will produce an exception, but message 1 succeeded and message 2 did not.  
                //should we return a ProduceResponse with an error and no error for the other messages?
                //at this point though the client does not know which message is routed to which server.  
                //the whole batch of messages would need to be returned.
                var test = producer.SendMessageAsync("UnitTest", messages).Result;
            }
        }
        #endregion

        #region Nagle Tests...
        [Test]
        public async void ProducesShouldBatchAndOnlySendOneProduceRequest()
        {
            var routerProxy = new FakeBrokerRouter();
            var producer = new Producer(routerProxy.Create()) { BatchSize = 2 };
            using (producer)
            {
                var calls = new[]
                {
                    producer.SendMessageAsync(FakeBrokerRouter.TestTopic, new[] {new Message()}),
                    producer.SendMessageAsync(FakeBrokerRouter.TestTopic, new[] {new Message()})
                };

                await Task.WhenAll(calls);

                Assert.That(routerProxy.BrokerConn0.ProduceRequestCallCount, Is.EqualTo(1));
                Assert.That(routerProxy.BrokerConn1.ProduceRequestCallCount, Is.EqualTo(1));
            }

        }

        [Test]
        public async void ProducesShouldSendOneProduceRequestForEachBatchSize()
        {
            var routerProxy = new FakeBrokerRouter();
            var producer = new Producer(routerProxy.Create()) { BatchSize = 2 };
            using (producer)
            {
                var calls = new[]
                {
                    producer.SendMessageAsync(FakeBrokerRouter.TestTopic, new[] {new Message()}),
                    producer.SendMessageAsync(FakeBrokerRouter.TestTopic, new[] {new Message()}),
                    producer.SendMessageAsync(FakeBrokerRouter.TestTopic, new[] {new Message()}),
                    producer.SendMessageAsync(FakeBrokerRouter.TestTopic, new[] {new Message()})
                };

                await Task.WhenAll(calls);

                Assert.That(routerProxy.BrokerConn0.ProduceRequestCallCount, Is.EqualTo(2));
                Assert.That(routerProxy.BrokerConn1.ProduceRequestCallCount, Is.EqualTo(2));
            }

        }

        [Test]
        [TestCase(1, 2, 100, 100, 2)]
        [TestCase(1, 1, 100, 200, 2)]
        [TestCase(1, 1, 100, 100, 1)]
        public async void ProducesShouldSendExpectedProduceRequestForEachAckLevelAndTimeoutCombination(short ack1, short ack2, int time1, int time2, int expected)
        {
            var routerProxy = new FakeBrokerRouter();
            var producer = new Producer(routerProxy.Create()) { BatchSize = 100 };
            using (producer)
            {
                var calls = new[]
                {
                    producer.SendMessageAsync(FakeBrokerRouter.TestTopic, new[] {new Message(), new Message()}, acks:ack1, timeout: TimeSpan.FromMilliseconds(time1)),
                    producer.SendMessageAsync(FakeBrokerRouter.TestTopic, new[] {new Message(), new Message()}, acks:ack2, timeout: TimeSpan.FromMilliseconds(time2))
                };

                await Task.WhenAll(calls);

                Assert.That(routerProxy.BrokerConn0.ProduceRequestCallCount, Is.EqualTo(expected));
                Assert.That(routerProxy.BrokerConn1.ProduceRequestCallCount, Is.EqualTo(expected));
            }

        }

        [Test]
        [TestCase(MessageCodec.CodecGzip, MessageCodec.CodecNone, 2)]
        [TestCase(MessageCodec.CodecGzip, MessageCodec.CodecGzip, 1)]
        [TestCase(MessageCodec.CodecNone, MessageCodec.CodecNone, 1)]
        public async void ProducesShouldSendExpectedProduceRequestForEachCodecCombination(MessageCodec codec1, MessageCodec codec2, int expected)
        {
            var routerProxy = new FakeBrokerRouter();
            var producer = new Producer(routerProxy.Create()) { BatchSize = 100 };
            using (producer)
            {
                var calls = new[]
                {
                    producer.SendMessageAsync(FakeBrokerRouter.TestTopic, new[] {new Message(), new Message()}, codec: codec1),
                    producer.SendMessageAsync(FakeBrokerRouter.TestTopic, new[] {new Message(), new Message()}, codec: codec2)
                };

                await Task.WhenAll(calls);

                Assert.That(routerProxy.BrokerConn0.ProduceRequestCallCount, Is.EqualTo(expected));
                Assert.That(routerProxy.BrokerConn1.ProduceRequestCallCount, Is.EqualTo(expected));
            }

        }

        #endregion

        #region Dispose Tests...
        [Test]
        [ExpectedException(typeof(ObjectDisposedException))]
        public void SendingMessageWhenDisposedShouldThrow()
        {
            var router = Substitute.For<IBrokerRouter>();
            var producer = new Producer(router);
            using (producer) { }
            producer.SendMessageAsync("Test", new[] { new Message() });
        }

		[Test]
		[ExpectedException(typeof(ObjectDisposedException))]
		public void SendingMessageWhenStoppedShouldThrow()
		{
			var router = Substitute.For<IBrokerRouter>();
			using (var producer = new Producer(router))
			{
				producer.Stop(false);
				producer.SendMessageAsync("Test", new[] { new Message() });
			}
		}

        [Test]
        public void StopShouldWaitUntilCollectionEmpty()
        {
            var router = Substitute.For<IBrokerRouter>();
			using (var producer = new Producer(router) { BatchDelayTime = TimeSpan.FromMilliseconds(100) })
			{

				producer.SendMessageAsync("Test", new[] { new Message() });
				Assert.That(producer.ActiveCount, Is.EqualTo(1));

				producer.Stop(true);

				Assert.That(producer.ActiveCount, Is.EqualTo(0));
			}
        }


        [Test]
        public void EnsureProducerDisposesRouter()
        {
            var router = Substitute.For<IBrokerRouter>();

            var producer = new Producer(router);
            using (producer) { }
            router.Received(1).Dispose();
        }

        [Test]
        public void ProducerShouldInterruptWaitOnEmptyCollection()
        {
            //use the fake to actually cause loop to execute
            var router = new FakeBrokerRouter().Create();

            var producer = new Producer(router);
            using (producer) { }
        }
        #endregion
    }
}
