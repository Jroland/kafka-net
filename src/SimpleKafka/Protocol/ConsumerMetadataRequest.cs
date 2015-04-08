﻿using System;
using System.Collections.Generic;
using SimpleKafka.Common;

namespace SimpleKafka.Protocol
{
    /// <summary>
    /// https://cwiki.apache.org/confluence/display/KAFKA/A+Guide+To+The+Kafka+Protocol#AGuideToTheKafkaProtocol-OffsetFetchRequest
    /// The offsets for a given consumer group is maintained by a specific broker called the offset coordinator. i.e., a consumer needs 
    /// to issue its offset commit and fetch requests to this specific broker. It can discover the current offset coordinator by issuing a consumer metadata request.
    /// </summary>
    public class ConsumerMetadataRequest : BaseRequest, IKafkaRequest<ConsumerMetadataResponse>
    {
        public ApiKeyRequestType ApiKey { get { return ApiKeyRequestType.ConsumerMetadataRequest; } }
        public string ConsumerGroup { get; set; }

        public byte[] Encode()
        {
            return EncodeConsumerMetadataRequest(this);
        }


        public IEnumerable<ConsumerMetadataResponse> Decode(byte[] payload)
        {
            return DecodeConsumerMetadataResponse(payload);
        }

        private byte[] EncodeConsumerMetadataRequest(ConsumerMetadataRequest request)
        {
            using (var message = EncodeHeader(request).Pack(request.ConsumerGroup, StringPrefixEncoding.Int16))
            {
                return message.Payload();
            }
        }

        private IEnumerable<ConsumerMetadataResponse> DecodeConsumerMetadataResponse(byte[] data)
        {
            using (var stream = new BigEndianBinaryReader(data))
            {
                var correlationId = stream.ReadInt32();

                var response = new ConsumerMetadataResponse
                    {
                        Error = stream.ReadInt16(),
                        CoordinatorId = stream.ReadInt32(),
                        CoordinatorHost = stream.ReadInt16String(),
                        CoordinatorPort = stream.ReadInt32()
                    };

                yield return response;
            }
        }
    }

    public class ConsumerMetadataResponse
    {
        /// <summary>
        /// Error code of exception that occured during the request.  Zero if no error.
        /// </summary>
        public Int16 Error;

        public int CoordinatorId;
        public string CoordinatorHost;
        public int CoordinatorPort;
    }
}
