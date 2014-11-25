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
using KafkaNet.Common;

namespace KafkaNet
{
	/// <summary>
	/// A High level API with consumer group support. Automatic commits the offset for the group, and will return a non-blocking
	/// message list to client.
	/// TODO: Make sure offset tracking works in parallel (right now it will consume in a "at least once" manner)
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

		/// <summary>
		/// Refresh offset by fetching the offset from kafka server for this._consumerGroup; also check if the offset is within the range of
		/// min-max offset in current topic, if not, set to minimum offset.
		/// </summary>
		public void RefreshOffsets()
		{
			var actualOffsets = _metadataQueries.GetTopicOffsetAsync(_options.Topic).Result;
			var maxminGroups = actualOffsets.Select(x => new { pid = x.PartitionId, min = x.Offsets.Min(), max = x.Offsets.Max() });

			_topic.Partitions.ForEach(
				partition =>
				{
					_options.Router.SelectBrokerRoute(_topic.Name, partition.PartitionId).Connection
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
				}
			);
			
		}

		/// <summary>
		/// One time consuming certain num of messages specified, and stop consuming more at the end of call. It'll automatically increase
		/// the offset by num and commit it. If fail to commit offset, it'll return null result.
		/// </summary>
		/// <param name="num"></param>
		/// <returns></returns>
		public IEnumerable<Message> Consume(int num, int timeout=1000)
		{
			List<Message> result = new List<Message>();
			
			_options.Log.DebugFormat("Consumer: Beginning consumption of topic: {0}", _options.Topic);
			_topicPartitionQueryTimer.Begin();
			
			while (result.Count < num) {
				Message temp = null;
				if(!_fetchResponseQueue.TryTake(out temp, timeout)){
					return null;
				}
				
				if(temp != null){
					var conn = _options.Router.SelectBrokerRoute(_topic.Name, temp.Meta.PartitionId).Connection;
					var offsets = conn.SendAsync(CreateOffsetFetchRequest(_consumerGroup, temp.Meta.PartitionId )).Result;
					var x = offsets.FirstOrDefault();
					
					if(x != null && x.PartitionId == temp.Meta.PartitionId){
						if(x.Offset > temp.Meta.Offset)
							_options.Log.DebugFormat("GET Duplicated message");
						else {
							if(CommitOffset(conn, temp.Meta.PartitionId, temp.Meta.Offset+1))
								result.Add(temp);
						}
					}
				}
			}
			return result;
		}

		protected bool CommitOffset(IKafkaConnection conn, int pid, long offset)
		{
			var resp = conn.SendAsync(CreateOffsetCommitRequest(_consumerGroup, pid, offset)).Result.FirstOrDefault();
			if (resp != null && ((int)resp.Error) == (int)ErrorResponseCode.NoError)
				return true;
			else
			{
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
