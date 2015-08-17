using kafka_tests.Helpers;
using KafkaNet;
using KafkaNet.Model;
using KafkaNet.Protocol;
using Moq;
using Ninject.MockingKernel.Moq;
using NUnit.Framework;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace kafka_tests.Unit
{
    [TestFixture]
    [Category("Unit")]
    public class ConsumerTests
    {
        [Test, Repeat(IntegrationConfig.NumberOfRepeat)]
        public void CancellationShouldInterruptConsumption()
        {
            var routerProxy = new BrokerRouterProxy(new MoqMockingKernel());
            routerProxy.BrokerConn0.FetchResponseFunction = async () => { return new FetchResponse(); };

            var router = routerProxy.Create();

            var options = CreateOptions(router);

            using (var consumer = new Consumer(options))
            {
                var tokenSrc = new CancellationTokenSource();

                var consumeTask = Task.Run(() => consumer.Consume(tokenSrc.Token).FirstOrDefault());

                //wait until the fake broker is running and requesting fetches
                TaskTest.WaitFor(() => routerProxy.BrokerConn0.FetchRequestCallCount > 10);

                tokenSrc.Cancel();

                Assert.That(
                    Assert.Throws<AggregateException>(consumeTask.Wait).InnerException,
                    Is.TypeOf<OperationCanceledException>());
            }
        }

        [Test, Repeat(IntegrationConfig.NumberOfRepeat)]
        public async Task ConsumerWhitelistShouldOnlyConsumeSpecifiedPartition()
        {
            var routerProxy = new BrokerRouterProxy(new MoqMockingKernel());
            routerProxy.BrokerConn0.FetchResponseFunction = async () => { return new FetchResponse(); };
            var router = routerProxy.Create();
            var options = CreateOptions(router);
            options.PartitionWhitelist = new List<int> { 0 };
            using (var consumer = new Consumer(options))
            {
                var test = consumer.Consume();

                await TaskTest.WaitFor(() => consumer.ConsumerTaskCount > 0);
                await TaskTest.WaitFor(() => routerProxy.BrokerConn0.FetchRequestCallCount > 0);

                Assert.That(consumer.ConsumerTaskCount, Is.EqualTo(1),
                    "Consumer should only create one consuming thread for partition 0.");
                Assert.That(routerProxy.BrokerConn0.FetchRequestCallCount, Is.GreaterThanOrEqualTo(1));
                Assert.That(routerProxy.BrokerConn1.FetchRequestCallCount, Is.EqualTo(0));
            }
        }

        [Test, Repeat(IntegrationConfig.NumberOfRepeat)]
        public async Task ConsumerWithEmptyWhitelistShouldConsumeAllPartition()
        {
            var routerProxy = new BrokerRouterProxy(new MoqMockingKernel());

            var router = routerProxy.Create();
            var options = CreateOptions(router);
            options.PartitionWhitelist = new List<int>();

            using (var consumer = new Consumer(options))
            {
                var test = consumer.Consume();

                await TaskTest.WaitFor(() => consumer.ConsumerTaskCount > 0);
                await TaskTest.WaitFor(() => routerProxy.BrokerConn0.FetchRequestCallCount > 0);
                await TaskTest.WaitFor(() => routerProxy.BrokerConn1.FetchRequestCallCount > 0);

                Assert.That(consumer.ConsumerTaskCount, Is.EqualTo(2),
                    "Consumer should create one consuming thread for each partition.");
                Assert.That(routerProxy.BrokerConn0.FetchRequestCallCount, Is.GreaterThanOrEqualTo(1),
                    "BrokerConn0 not sent FetchRequest");
                Assert.That(routerProxy.BrokerConn1.FetchRequestCallCount, Is.GreaterThanOrEqualTo(1),
                    "BrokerConn1 not sent FetchRequest");
            }
        }

        [Test, Repeat(IntegrationConfig.NumberOfRepeat)]
        public void ConsumerShouldCreateTaskForEachBroker()
        {
            var routerProxy = new BrokerRouterProxy(new MoqMockingKernel());
            routerProxy.BrokerConn0.FetchResponseFunction = async () => { return new FetchResponse(); };
            var router = routerProxy.Create();
            var options = CreateOptions(router);
            options.PartitionWhitelist = new List<int>();
            using (var consumer = new Consumer(options))
            {
                var test = consumer.Consume();
                TaskTest.WaitFor(() => consumer.ConsumerTaskCount >= 2);

                Assert.That(consumer.ConsumerTaskCount, Is.EqualTo(2));
            }
        }

        [Test, Repeat(IntegrationConfig.NumberOfRepeat)]
        public void ConsumerShouldReturnOffset()
        {
            var routerProxy = new BrokerRouterProxy(new MoqMockingKernel());
            routerProxy.BrokerConn0.FetchResponseFunction = async () => { return new FetchResponse(); };
            var router = routerProxy.Create();
            var options = CreateOptions(router);
            options.PartitionWhitelist = new List<int>();
            using (var consumer = new Consumer(options))
            {
                var test = consumer.Consume();
                TaskTest.WaitFor(() => consumer.ConsumerTaskCount >= 2);

                Assert.That(consumer.ConsumerTaskCount, Is.EqualTo(2));
            }
        }

        [Test, Repeat(IntegrationConfig.NumberOfRepeat)]
        public void EnsureConsumerDisposesRouter()
        {
            var router = new MoqMockingKernel().GetMock<IBrokerRouter>();
            var consumer = new Consumer(CreateOptions(router.Object));
            using (consumer) { }
            router.Verify(x => x.Dispose(), Times.Once());
        }

        [Test, Repeat(IntegrationConfig.NumberOfRepeat)]
        public void EnsureConsumerDisposesAllTasks()
        {
            var routerProxy = new BrokerRouterProxy(new MoqMockingKernel());
            routerProxy.BrokerConn0.FetchResponseFunction = async () => { return new FetchResponse(); };
            var router = routerProxy.Create();
            var options = CreateOptions(router);
            options.PartitionWhitelist = new List<int>();

            var consumer = new Consumer(options);
            using (consumer)
            {
                var test = consumer.Consume();
                TaskTest.WaitFor(() => consumer.ConsumerTaskCount >= 2);
            }

            TaskTest.WaitFor(() => consumer.ConsumerTaskCount <= 0);
            Assert.That(consumer.ConsumerTaskCount, Is.EqualTo(0));
        }

        private ConsumerOptions CreateOptions(IBrokerRouter router)
        {
            return new ConsumerOptions(BrokerRouterProxy.TestTopic, router);
        }
    }
}