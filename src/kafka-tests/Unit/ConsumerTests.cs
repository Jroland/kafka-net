using System;
using System.Threading.Tasks;
using KafkaNet;
using KafkaNet.Model;
using KafkaNet.Protocol;
using Moq;
using Ninject.MockingKernel.Moq;
using NUnit.Framework;
using System.Collections.Generic;
using System.Linq;
using System.Threading;

namespace kafka_tests.Unit
{
    [TestFixture]
    [Category("Unit")]
    public class ConsumerTests
    {
        private MoqMockingKernel _kernel;

        [SetUp]
        public void Setup()
        {
            _kernel = new MoqMockingKernel();
        }

        [Test]
         public void CancellationShouldInterruptConsumption()
         {
             var routerProxy = new BrokerRouterProxy(_kernel);
             routerProxy.BrokerConn0.FetchResponseFunction = () => { while (true) Thread.Yield(); };
 
             var router = routerProxy.Create();
 
             var options = CreateOptions(router);
 
             var consumer = new Consumer(options);
 
             var tokenSrc = new CancellationTokenSource();
 
             var consumeTask = Task.Run(() => consumer.Consume(tokenSrc.Token).FirstOrDefault());
 
             if (consumeTask.Wait(TimeSpan.FromSeconds(3)))
                 Assert.Fail();
             
             tokenSrc.Cancel();
 
             Assert.That(
                 Assert.Throws<AggregateException>(consumeTask.Wait).InnerException,
                 Is.TypeOf<OperationCanceledException>());
         }

        [Test]
        public void ConsumerWhitelistShouldOnlyConsumeSpecifiedPartition()
        {
            var routerProxy = new BrokerRouterProxy(_kernel);
            routerProxy.BrokerConn0.FetchResponseFunction = () => { return new FetchResponse(); };
            var router = routerProxy.Create();
            var options = CreateOptions(router);
            options.PartitionWhitelist = new List<int> { 0 };
            var consumer = new Consumer(options);

            var test = consumer.Consume().Take(1);
            while (consumer.ConsumerTaskCount <= 0)
            {
                Thread.Sleep(100);
            }

            Assert.That(routerProxy.BrokerConn0.FetchRequestCallCount, Is.GreaterThanOrEqualTo(1));
            Assert.That(routerProxy.BrokerConn1.FetchRequestCallCount, Is.EqualTo(0));
        }

        [Test]
        public void ConsumerWithEmptyWhitelistShouldConsumeAllPartition()
        {
            var routerProxy = new BrokerRouterProxy(_kernel);
            routerProxy.BrokerConn0.FetchResponseFunction = () => { return new FetchResponse(); };
            var router = routerProxy.Create();
            var options = CreateOptions(router);
            options.PartitionWhitelist = new List<int>();

            var consumer = new Consumer(options);
            var test = consumer.Consume().Take(1);

            while (consumer.ConsumerTaskCount <= 1)
            {
                Thread.Sleep(100);
            }

            Assert.That(routerProxy.BrokerConn0.FetchRequestCallCount, Is.GreaterThanOrEqualTo(1), "BrokerConn0 not sent FetchRequest");
            Assert.That(routerProxy.BrokerConn1.FetchRequestCallCount, Is.GreaterThanOrEqualTo(1), "BrokerConn1 not sent FetchRequest");
        }

        [Test]
        public void ConsumerShouldCreateTaskForEachBroker()
        {
            var routerProxy = new BrokerRouterProxy(_kernel);
            routerProxy.BrokerConn0.FetchResponseFunction = () => { return new FetchResponse(); };
            var router = routerProxy.Create();
            var options = CreateOptions(router);
            options.PartitionWhitelist = new List<int>();
            var consumer = new Consumer(options);

            var test = consumer.Consume().Take(1);
            while (consumer.ConsumerTaskCount <= 0)
            {
                Thread.Sleep(100);
            }

            Assert.That(consumer.ConsumerTaskCount, Is.EqualTo(2));
        }


        [Test]
        public void ConsumerShouldReturnOffset()
        {
            var routerProxy = new BrokerRouterProxy(_kernel);
            routerProxy.BrokerConn0.FetchResponseFunction = () => { return new FetchResponse(); };
            var router = routerProxy.Create();
            var options = CreateOptions(router);
            options.PartitionWhitelist = new List<int>();
            var consumer = new Consumer(options);

            var test = consumer.Consume().Take(1);
            while (consumer.ConsumerTaskCount <= 0)
            {
                Thread.Sleep(100);
            }

            Assert.That(consumer.ConsumerTaskCount, Is.EqualTo(2));
        }




        [Test]
        public void EnsureConsumerDisposesRouter()
        {
            var router = _kernel.GetMock<IBrokerRouter>();
            var consumer = new Consumer(CreateOptions(router.Object));
            using (consumer) { }
            router.Verify(x => x.Dispose(), Times.Once());
        }

        private ConsumerOptions CreateOptions(IBrokerRouter router)
        {
            return new ConsumerOptions(BrokerRouterProxy.TestTopic, router);
        }
    }
}
