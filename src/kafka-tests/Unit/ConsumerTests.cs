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
using kafka_tests.Helpers;

namespace kafka_tests.Unit
{
	[TestFixture]
	[Category("Unit")]
	public class ConsumerTests
	{
		private MoqMockingKernel _kernel;
		static object lockObj = new object();

		[SetUp]
		public void Setup()
		{
			_kernel = new MoqMockingKernel();
		}

		[TearDown]
		public void CancellationShouldInterruptConsumption()
		{
			var routerProxy = new BrokerRouterProxy(_kernel);
			routerProxy.BrokerConn0.FetchResponseFunction = () => { return new FetchResponse(); };
			
			var router = routerProxy.Create();
			
			var options = CreateOptions(router);
			
			var consumer = new Consumer(options);
			
			var tokenSrc = new CancellationTokenSource();
			
			var consumeTask = Task.Run(() => consumer.Consume(tokenSrc.Token).FirstOrDefault());

			//wait until the fake broker is running and requesting fetches
			TaskTest.WaitFor(() => routerProxy.BrokerConn0.FetchRequestCallCount > 10);
			tokenSrc.Cancel();
			
			Assert.That(
				Assert.Throws<AggregateException>(consumeTask.Wait).InnerException,
				Is.TypeOf<OperationCanceledException>());
			using (consumer){}
			
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
			TaskTest.WaitFor(() => consumer.ConsumerTaskCount > 0);
			
			Assert.That(routerProxy.BrokerConn0.FetchRequestCallCount, Is.GreaterThanOrEqualTo(1));
			Assert.That(routerProxy.BrokerConn1.FetchRequestCallCount, Is.EqualTo(0));
		}

		[Test]
		public void ConsumerWithEmptyWhitelistShouldConsumeAllPartition()
		{
			var routerProxy = new BrokerRouterProxy(_kernel);
			
			var router = routerProxy.Create();
			var options = CreateOptions(router);
			options.PartitionWhitelist = new List<int>();
			
			var consumer = new Consumer(options);
			
			var test = consumer.Consume().Take(1);
			
			
			TaskTest.WaitFor(() => consumer.ConsumerTaskCount <= 1);
			TaskTest.WaitFor(() => routerProxy.BrokerConn0.FetchRequestCallCount > 0);
			TaskTest.WaitFor(() => routerProxy.BrokerConn1.FetchRequestCallCount > 0);

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
			TaskTest.WaitFor(() => consumer.ConsumerTaskCount >= 2);

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
			TaskTest.WaitFor(() => consumer.ConsumerTaskCount >= 2);
			
			Assert.That(consumer.ConsumerTaskCount, Is.EqualTo(2));
		}




		[Test]
		public void EnsureConsumerDisposesRouter()
		{
			lock(lockObj){
				var router = _kernel.GetMock<IBrokerRouter>();
				var consumer = new Consumer(CreateOptions(router.Object));
				using (consumer) { }
				router.Verify(x => x.Dispose(), Times.Once());
			}
		}

		private ConsumerOptions CreateOptions(IBrokerRouter router)
		{
			lock(new object()){
				return new ConsumerOptions(BrokerRouterProxy.TestTopic, router);
			}
		}
	}
}
