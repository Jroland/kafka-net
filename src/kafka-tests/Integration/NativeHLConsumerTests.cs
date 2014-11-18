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

namespace kafka_tests.Integration
{
	[TestFixture]
	[Category("Integration")]
	public class NativeHLConsumerTests {
		private KafkaOptions Options ;
		private string cgroup = "test";
		private string topic = "integration_test";
		
		[TestFixtureSetUpAttribute]
		public void SetupFixture()
		{
			Options = new KafkaOptions(IntegrationConfig.IntegrationUri);
			var amount = 200;
			using (var router = new BrokerRouter(Options)){
				using (var producer = new Producer(router, -1))
				{
					var tasks = new Task<List<ProduceResponse>>[amount];

					for (var i = 0; i < amount; i++)
					{
						Console.WriteLine("produce test message... ");
						tasks[i] = producer.SendMessageAsync(topic, new[] { new Message(Guid.NewGuid().ToString()) });
					}

					var results = tasks.SelectMany(x => x.Result).ToList();

					Assert.That(results.Count, Is.EqualTo(amount));
					Assert.That(results.Any(x => x.Error != 0), Is.False, "Should not have received any results as failures.");
				}
			}
		}
		
		[Test]
		public void ConsumerShouldConsumeCorrectNumberOfResults()
		{
			using (var router = new BrokerRouter(Options)){
				var nativeConsumer = new NativeHLConsumer(new ConsumerOptions(topic, router),
				                                          cgroup);
				for (int i = 0; i < 10; i++) {
					var result = nativeConsumer.Consume(i);
					Assert.AreEqual(result.Count(), i);
				}
				nativeConsumer.Dispose();
				Assert.AreEqual(nativeConsumer.ConsumerTaskCount , 0);
			}
		}
		
		[Test]
		public void ConsumeMultipleTimesShouldHaveCorrectOffset()
		{
			int num = 30;
			using (var router = new BrokerRouter(Options)){
				using (var nativeConsumer = new NativeHLConsumer(new ConsumerOptions(topic, router),
				                                                 "multiconsume") )
				{
					
					var result = nativeConsumer.Consume(num);
					Assert.AreEqual(result.Count(), num);
					
					var result2 = nativeConsumer.Consume(num);
					Assert.AreEqual(result.Count(), num);
					
					var dic1 = from r in result
						group r by r.Meta.PartitionId into g
						select new {pid = g.Key, offset = g.Max(y => y.Meta.Offset)};
					var dic2 = from r in result2
						group r by r.Meta.PartitionId into g
						select new {pid = g.Key, offset = g.Max(y => y.Meta.Offset)};
					var shouldempty = from d in dic1
						from d2 in dic2
						where d.pid == d2.pid && d.offset >= d2.offset
						select d;
					
					Assert.AreEqual(shouldempty.Count(), 0);
					Assert.True(dic1.All(x => dic2.Single(y => y.pid == x.pid).offset == x.offset+num));
					
					using (var consumer2 = new NativeHLConsumer(new ConsumerOptions(topic, router),
					                                            "multiconsume")) {
						var result3 = consumer2.Consume(num);
						Assert.AreEqual(result3.Count(), num);
						var dic3 = from r in result3
							group r by r.Meta.PartitionId into g
							select new { pid = g.Key, offset = g.Max(y => y.Meta.Offset)};
						shouldempty = from d2 in dic2
							from d3 in dic3
							where d2.pid == d3.pid && d2.offset >= d3.offset
							select d2;
						Assert.AreEqual(shouldempty.Count(), 0);
						Assert.True(dic2.All(x => {
						                     	if(dic3.Any(y => y.pid != x.pid))
						                     		return true;
						                     	var lager = dic3.Single(y => y.pid == x.pid);
						                     	return lager.offset == x.offset + num ;
						                     }) );
					}
				}
			}
		}
		
		[Test]
		public void ConsumerShouldCommitOffsetOnSuccess()
		{
			using (var router = new BrokerRouter(Options)){
				using (var nativeConsumer = new NativeHLConsumer(new ConsumerOptions(topic, router),
				                                                 "multiconsume") )
				{
					
					var result = nativeConsumer.Consume(10);
					
				}
			}
		}
		
		[Test]
		public void ConsumerShouldNotCommitOffsetOnFail()
		{
			//TODO
		}
		
		[Test]
		public void ConsumerShouldCancelTaskAfterConsume()
		{
			//TODO
		}
	}
}