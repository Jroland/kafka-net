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
		public OffsetFetchRequest()
		{
		}
		
		public ApiKeyRequestType ApiKey { 
			get{
				return ApiKeyRequestType.OffsetFetch;
			}
		}
		
		public byte[] Encode(){
			//TODO:
			throw new NotImplementedException();
			return new byte[]{};
		}
		
		public IEnumerable<OffsetFetchResponse> Decode(byte[] payload){
			//TODO:
			throw new NotImplementedException();
			return null;
		}
		
	}
	
	public class OffsetFetchResponse{
		//TODO:
	}
}
