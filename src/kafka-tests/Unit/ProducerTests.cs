using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using KafkaNet;
using KafkaNet.Model;
using KafkaNet.Protocol;
using Moq;
using NUnit.Framework;
using Ninject.MockingKernel.Moq;
using kafka_tests.Helpers;

namespace kafka_tests.Unit
{
    [TestFixture]
    [Category("Unit")]
    public class ProducerTests
    {
        [SetUp]
        public void Setup()
        {
            _kernel = new MoqMockingKernel();
            _routerProxy = new BrokerRouterProxy(_kernel);
        }

        private MoqMockingKernel _kernel;
        private BrokerRouterProxy _routerProxy;

        [Test]
        [Ignore("is there a way to communicate back which client failed and which succeeded.")]
        public void ConnectionExceptionOnOneShouldCommunicateBackWhichMessagesFailed()
        {
            //TODO is there a way to communicate back which client failed and which succeeded.
            var routerProxy = new BrokerRouterProxy(_kernel);
            routerProxy.BrokerConn1.ProduceResponseFunction = () => { throw new ApplicationException("some exception"); };

            var router = routerProxy.Create();
            using (var producer = new Producer(router, new KafkaOptions
                {
                    Hosts = new List<Uri> {new Uri("http://localhost:1"), new Uri("http://localhost:2")},
                }))
            {
                var messages = new List<Message>
                    {
                        new Message {Value = "1"}, new Message {Value = "2"}
                    };

                //this will produce an exception, but message 1 succeeded and message 2 did not.  
                //should we return a ProduceResponse with an error and no error for the other messages?
                //at this point though the client does not know which message is routed to which server.  
                //the whole batch of messages would need to be returned.
                var test = producer.SendMessageAsync("UnitTest", messages).Result;
            }
        }

        [Test]
        public void EnsureProducerDisposesRouter()
        {
            var router = _kernel.GetMock<IBrokerRouter>();
            var producer = new Producer(router.Object, new KafkaOptions
                {
                    Hosts = new List<Uri> {new Uri("http://localhost:1"), new Uri("http://localhost:2")},
                });
            using (producer)
            {
            }
            router.Verify(x => x.Dispose(), Times.Once());
        }

        [Test]
        public void ProducerShouldGroupMessagesByBroker()
        {
            var router = _routerProxy.Create();
            using (var producer = new Producer(router, new KafkaOptions
                {
                    Hosts = new List<Uri> {new Uri("http://localhost:1"), new Uri("http://localhost:2")},
                }))
            {
                var messages = new List<Message>
                    {
                        new Message {Value = "1"}, new Message {Value = "2"}
                    };

                var response = producer.SendMessageAsync("UnitTest", messages).Result;

                Assert.That(response.Count, Is.EqualTo(2));
                Assert.That(_routerProxy.BrokerConn0.ProduceRequestCallCount, Is.EqualTo(1));
                Assert.That(_routerProxy.BrokerConn1.ProduceRequestCallCount, Is.EqualTo(1));
            }
        }

        [Test]
        public void SendAsyncShouldBlockWhenMaximumAsyncQueueReached()
        {
            var count = 0;
            var semaphore = new SemaphoreSlim(0);
            var routerProxy = new BrokerRouterProxy(_kernel);
            //block the second call returning from send message async
            routerProxy.BrokerConn0.ProduceResponseFunction = () =>
                {
                    semaphore.Wait();
                    return new ProduceResponse();
                };

            var router = routerProxy.Create();
            var options = new KafkaOptions
                {
                    Hosts = new List<Uri> {new Uri("http://localhost:1"), new Uri("http://localhost:2")}, 
                    QueueSize = 1
                };
            using (var producer = new Producer(router, options))
            {
                var messages = new List<Message>
                    {
                        new Message {Value = "1"}, new Message {Value = "2"}
                    };

                Task.Factory.StartNew(() =>
                    {
                        producer.SendMessageAsync(BrokerRouterProxy.TestTopic, messages);
                        count++;
                        producer.SendMessageAsync(BrokerRouterProxy.TestTopic, messages);
                        count++;
                    });

                TaskTest.WaitFor(() => count > 0);
                Assert.That(count, Is.EqualTo(1), "Only one SendMessageAsync should continue.");
                Assert.That(producer.ActiveCount, Is.EqualTo(1), "One async call shoud be active.");
                semaphore.Release();
                TaskTest.WaitFor(() => count > 1);
                Assert.That(Thread.VolatileRead(ref count), Is.EqualTo(2), "The second SendMessageAsync should continue after semaphore is released.");
            }
        }

        [Test]
        public void ShouldSendAsyncToAllConnectionsEvenWhenExceptionOccursOnOne()
        {
            var routerProxy = new BrokerRouterProxy(_kernel);
            routerProxy.BrokerConn1.ProduceResponseFunction = () => { throw new ApplicationException("some exception"); };

            var router = routerProxy.Create();
            using (var producer = new Producer(router, new KafkaOptions
            {
                Hosts = new List<Uri> { new Uri("http://localhost:1"), new Uri("http://localhost:2") },
            }))
            {
                var messages = new List<Message>
                    {
                        new Message {Value = "1"}, new Message {Value = "2"}
                    };

                producer.SendMessageAsync("UnitTest", messages).ContinueWith(t =>
                    {
                        Assert.That(t.IsFaulted, Is.True);
                        Assert.That(t.Exception, Is.Not.Null);
                        Assert.That(t.Exception.ToString(), Is.StringContaining("ApplicationException"));
                        Assert.That(routerProxy.BrokerConn0.ProduceRequestCallCount, Is.EqualTo(1));
                        Assert.That(routerProxy.BrokerConn1.ProduceRequestCallCount, Is.EqualTo(1));
                    }).Wait(TimeSpan.FromSeconds(10));
            }
        }
    }
}