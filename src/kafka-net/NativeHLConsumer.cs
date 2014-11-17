/*
 * Created by SharpDevelop.
 * User: peng.zang
 * Date: 11/11/2014
 * Time: 10:49 AM
 * 
 * To change this template use Tools | Options | Coding | Edit Standard Headers.
 */
using System;
using System.Linq;
using System.Threading;
using System.Collections.Generic;

using KafkaNet.Protocol;
using KafkaNet.Model;

namespace KafkaNet
{
	/// <summary>
	/// Description of NativeHLConsumer. Should only be used per
	/// </summary>
	public class NativeHLConsumer : Consumer
	{

		protected string _consumerGroup;

		public NativeHLConsumer(ConsumerOptions options, string consumerGroup, params OffsetPosition[] positions)
			: base(options, positions)
		{
			if (_topic == null || _topic.Name != _options.Topic)
				_topic = _metadataQueries.GetTopic(_options.Topic);
			_consumerGroup = consumerGroup;
			RefreshOffsets();
		}

		public void RefreshOffsets()
		{
			var actualOffsets = _metadataQueries.GetTopicOffsetAsync(_options.Topic).Result;
			var maxminGroups = actualOffsets.Select(x => new { pid = x.PartitionId, min = x.Offsets.Min(), max = x.Offsets.Max() });

			_topic.Partitions.ForEach(
				partition =>
				{
					var conn = _options.Router.SelectBrokerRoute(_topic.Name, partition.PartitionId);
					conn.Connection
						.SendAsync(CreateOffsetFetchRequest(_consumerGroup, partition.PartitionId))
						.Result.ForEach(
							offsetResp =>
							{
								Console.WriteLine("fetch offset: " + offsetResp.ToString());

								if (actualOffsets.Any(x => x.PartitionId == partition.PartitionId))
								{
									var actual = maxminGroups.First(x => x.pid == partition.PartitionId);
									if (actual.min > offsetResp.Offset || actual.max < offsetResp.Offset)
									{
										offsetResp.Offset = actual.min;
									}
								}
								_partitionOffsetIndex.AddOrUpdate(partition.PartitionId, i => offsetResp.Offset, (i, l) => offsetResp.Offset);
							});
				});


		}

		public IEnumerable<Message> Consume(int num)
		{
			var cancellationToken = new CancellationTokenSource();
			
			var result = base.Consume(cancellationToken.Token).Take(num).ToList();
			
			if(_fetchResponseQueue.Count < num){
				
			}
			var maxgroups = from r in result
				group r by r.Meta.PartitionId into g
				select new { pid = g.Key, offset = g.Max(m => m.Meta.Offset) + 1 };

			maxgroups.ToList().ForEach(x => Console.WriteLine(x.pid + " : " + x.offset));

			foreach (var pos in maxgroups)
			{

				if (!CommitOffset(pos.pid, pos.offset))
				{
					var mingroup = from r in result
						group r by r.Meta.PartitionId into g
						select new { pid = g.Key, offset = g.Min(m => m.Meta.Offset) };
					mingroup.ToList().ForEach(x => CommitOffset(x.pid, x.offset));
					return null;
				}
			}
			cancellationToken.Cancel();
			return result;
		}

		protected bool CommitOffset(int pid, long offset)
		{
			Console.WriteLine("*** Committing partition: " + pid + ", offset: " + offset);
			var conn = _options.Router.SelectBrokerRoute(_topic.Name, pid);
			var resp = conn.Connection
				.SendAsync(CreateOffsetCommitRequest(_consumerGroup, pid, offset)).Result.FirstOrDefault();
			if (resp != null && ((int)resp.Error) == (int)ErrorResponseCode.NoError)
				return true;
			else
			{
				Console.WriteLine(resp.Error + " topic name: " + _topic.Name);
				return false;
			}
		}

		protected OffsetFetchRequest CreateOffsetFetchRequest(string consumerGroup, int partitionId)
		{
			var request = new OffsetFetchRequest
			{
				ConsumerGroup = consumerGroup,
				Topics = new List<OffsetFetch>
				{
					new OffsetFetch
					{
						PartitionId = partitionId,
						Topic = _options.Topic
					}
				}
			};

			return request;
		}

		protected OffsetCommitRequest CreateOffsetCommitRequest(string consumerGroup, int partitionId, long offset, string metadata = null)
		{
			var commit = new OffsetCommitRequest
			{
				ConsumerGroup = consumerGroup,
				OffsetCommits = new List<OffsetCommit>
				{
					new OffsetCommit
					{
						PartitionId = partitionId,
						Topic = _topic.Name,
						Offset = offset,
						Metadata = metadata
					}
				}
			};

			return commit;
		}
	}
}
