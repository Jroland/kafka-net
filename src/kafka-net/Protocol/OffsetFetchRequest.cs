/*
 * Created by SharpDevelop.
 * User: peng.zang
 * Date: 7/1/2014
 * Time: 3:15 PM
 * 
 * To change this template use Tools | Options | Coding | Edit Standard Headers.
 */
using System;
using System.Collections.Generic;
using System.Linq;

using KafkaNet.Common;

namespace KafkaNet.Protocol
{
	/// <summary>
	/// Description of OffsetFetchRequest.
	/// </summary>
	public class OffsetFetchRequest : BaseRequest, IKafkaRequest<OffsetFetchResponse>
	{
		public OffsetFetchRequest(string consumerGroup){
			ConsumerGroup = consumerGroup;
		}
		
		public ApiKeyRequestType ApiKey {
			get{
				return ApiKeyRequestType.OffsetFetch;
			}
		}
		public string ConsumerGroup { get; set; }
		public List<Offset> Topics {
			get{
				return this._topics;
			}
			set{
				this._topics = value;
			}
		}
		
		protected List<Offset> _topics = new List<Offset>();
		
		public byte[] Encode(){
			return EncodeOffsetFetchRequest(this);
		}
		
		protected byte[] EncodeOffsetFetchRequest(OffsetFetchRequest request){
			var message = new WriteByteStream();
			if (request.Topics == null) request.Topics = new List<Offset>();

			message.Pack(EncodeHeader(request));

			var topicGroups = request.Topics.GroupBy(x => x.Topic).ToList();

			message.Pack(ConsumerGroup.ToInt16SizedBytes(), topicGroups.Count.ToBytes());

			foreach (var topicGroup in topicGroups)
			{
				var partitions = topicGroup.GroupBy(x => x.PartitionId).ToList();
				message.Pack(topicGroup.Key.ToInt16SizedBytes(), partitions.Count.ToBytes());

				foreach (var partition in partitions)
				{
					foreach (var offset in partition)
					{
						message.Pack( offset.PartitionId.ToBytes());
					}
				}
			}

			message.Prepend(message.Length().ToBytes());

			return message.Payload();
		}
		
		public IEnumerable<OffsetFetchResponse> Decode(byte[] payload){
			return DecodeOffsetFetchResponse(payload);
		}
		
		
		protected IEnumerable<OffsetFetchResponse> DecodeOffsetFetchResponse(byte[] data){
			var stream = new ReadByteStream(data);
			Console.WriteLine("******* READ FROM OFFSET FETCH RESPONSE: " + data.ToString());
			var correlationId = stream.ReadInt();

			var topicCount = stream.ReadInt();
			for (int i = 0; i < topicCount; i++)
			{
				var topic = stream.ReadInt16String();

				var partitionCount = stream.ReadInt();
				for (int j = 0; j < partitionCount; j++)
				{
					var response = new OffsetFetchResponse()
					{
						topicName = topic,
						partitionID = stream.ReadInt(),
						offset = stream.ReadLong(),
						metaData = stream.ReadInt16String(),
						errorCode = stream.ReadInt16()
					};
					yield return response;
				}
			}
		}
		
	}
	
	public class OffsetFetchResponse{
		public string topicName;
		public Int32 partitionID;
		public Int64 offset;
		public string metaData;
		public Int16 errorCode;
	}
}
