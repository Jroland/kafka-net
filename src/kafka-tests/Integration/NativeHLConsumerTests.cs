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
					
					Assert.True(dic1.All(x => {
					                     	var a = dic2.SingleOrDefault(y => y.pid == x.pid);
					                     	if(a==null) return true;
					                     	Console.WriteLine(a + " : " +x);
					                     	return a.offset >= x.offset;
					                     }));
					
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
						                     	return lager.offset >= x.offset;
						                     }) );
					}
				}
			}
		}
		
		[Test]
		public void ConsumerShouldCommitOffsetOnSuccess()
		{
			IEnumerable<dynamic> dic1 = null;
			using (var router = new BrokerRouter(Options)){
				using (var nativeConsumer = new NativeHLConsumer(new ConsumerOptions(topic, router),
				                                                 "multiconsume") )
				{
					
					var result = nativeConsumer.Consume(10);
					dic1 = from r in result group r by r.Meta.PartitionId into g select new {pid = g.Key, offset = g.Max(x => x.Meta.Offset)};
				}
			}
			
			using (var router = new BrokerRouter(Options)){
				using (var nativeConsumer = new NativeHLConsumer(new ConsumerOptions(topic, router),
				                                                 "multiconsume") )
				{
					
					var result = nativeConsumer.Consume(10);
					var dic2 = from r in result group r by r.Meta.PartitionId into g select new {pid = g.Key, offset = g.Max(x => x.Meta.Offset)};

					Assert.True(dic2.All(d2 => {
					                     	var a = dic1.SingleOrDefault(x => x.pid == d2.pid);
					                     	if(a == null) return true;
					                     	return a.offset + 10 == d2.offset;
					                     }));
				}
			}
		}
		
		
		[Test]
		public void ConsumerShouldCancelTaskAfterConsume()
		{
			using (var router = new BrokerRouter(Options)){
				var nativeConsumer = new NativeHLConsumer(new ConsumerOptions(topic, router), cgroup);
				nativeConsumer.Consume(10);
				
				nativeConsumer.Dispose();

				Assert.AreEqual(nativeConsumer.ConsumerTaskCount, 0);
			}
		}
		
		[Test]
		public void ConsumerShouldNotBlockInfinitly()
		{
			int timeout = 1000;
			using (var router = new BrokerRouter(Options)){
				using (var consumer = new NativeHLConsumer(new ConsumerOptions("nonexistTopic", router), cgroup)) {
					var task = Task.Factory.StartNew(() => consumer.Consume(10, timeout));
					Thread.Sleep(timeout + 100);
					Assert.True(task.IsCompleted);
				}
			}
		}
		
		[Test]
		public void ParallelConsumersShouldNotDuplicateConsume()
		{
			List<Task<IEnumerable<Message>>> tasks = new List<Task<IEnumerable<Message>>>();
			for (int i = 0; i < 3; i++) {
				tasks.Add(new Task<IEnumerable<Message>>(
					() => {
						using (var router = new BrokerRouter(Options)){
							using (var nativeConsumer = new NativeHLConsumer(new ConsumerOptions(topic, router), cgroup)) {
								return nativeConsumer.Consume(3,10000);
							}
						}
					}));
			}
			tasks.ForEach(x => x.Start());
			
			List<List<Message>>  results = new List<List<Message>>();
			tasks.ForEach(x => {x.Wait(); results.Add(x.Result.ToList()); });
			
			List<Message> duplicated = new List<Message>();
			
			var shouldnotnull =
				results.Aggregate(
					(final, next) =>
					{
						if(final != null)
							final.ForEach(x => Console.WriteLine(x.Meta.PartitionId + " : " +x.Meta.Offset));
						if(next != null)
							next.ForEach(y => Console.WriteLine("next-- " + y.Meta.PartitionId + " : " + y.Meta.Offset));
						if(final == null)
							return null;
						final.ForEach(
							x => {
								if(next.SingleOrDefault(
									y =>
									(y.Meta.PartitionId==x.Meta.PartitionId && y.Meta.Offset == x.Meta.Offset) ) != null ){
									duplicated.Add(x);
								}
							}
						);
						next.ForEach(x =>
						             {
						             	if(final.FirstOrDefault(y => y.Meta.PartitionId==x.Meta.PartitionId) == null)
						             		final.Add(x);
						             }
						            );
						return final;
					});
			Assert.IsNotNull(shouldnotnull);
			duplicated.ForEach(d => Console.WriteLine("duplicated: " + d.Meta.PartitionId + " : " + d.Meta.Offset));

			//TODO: When offset tracking for multiple consumer in the same group supported, uncomment this.
//			Assert.AreEqual(duplicated.Count, 0);
		}
		
	}
	
}