using System;
using System.Collections.Generic;
using System.Linq;
using KafkaNet.Common;
using KafkaNet.Model;


namespace KafkaNet
{
    public enum ApiKeyRequestType
    {
        Produce = 0,
        Fetch = 1,
        Offset = 2,
        MetaData = 3,
        LeaderAndIsr = 4,
        StopReplica = 5,
        OffsetCommit = 6,
        OffsetFetch = 7
    }

    /// <summary>
    /// Kafka Protocol implementation:
    /// https://cwiki.apache.org/confluence/display/KAFKA/A+Guide+To+The+Kafka+Protocol
    /// </summary>
    public class Protocol
    {
        /// <summary>
        /// From Documentation: 
        /// The replica id indicates the node id of the replica initiating this request. Normal client consumers should always specify this as -1 as they have no node id. 
        /// Other brokers set this to be their own node id. The value -2 is accepted to allow a non-broker to issue fetch requests as if it were a replica broker for debugging purposes.
        /// </summary>
        private const int ReplicaId = -1;
        private const Int16 ApiVersion = 0;
        private static readonly Crc32 Crc32 = new Crc32();

        /// <summary>
        /// Encode the common head for kafka request.
        /// </summary>
        /// <param name="request"></param>
        /// <returns></returns>
        /// <remarks>Format: (hhihs) </remarks>
        public byte[] EncodeHeader(IKafkaRequest request)
        {
            var message = new WriteByteStream();

            message.Pack(((Int16)request.ApiKey).ToBytes(),
                          ApiVersion.ToBytes(),
                          request.CorrelationId.ToBytes(),
                          request.ClientId.ToInt16SizedBytes());

            return message.Payload();
        }

        /// <summary>
        /// Encodes a collection of messages into one byte[].  Encoded in order of list.
        /// </summary>
        /// <param name="messages">The collection of messages to encode together.</param>
        /// <returns>Encoded byte[] representing the collection of messages.</returns>
        public byte[] EncodeMessageSet(IEnumerable<Message> messages)
        {
            var messageSet = new WriteByteStream();

            foreach (var message in messages)
            {
                var encodedMessage = EncodeMessage(message);
                messageSet.Pack(((long)0).ToBytes(), encodedMessage.Length.ToBytes(), encodedMessage);
            }

            return messageSet.Payload();
        }

        public IEnumerable<Message> DecodeMessageSet(byte[] messageSet)
        {
            var stream = new ReadByteStream(messageSet);
            while (stream.HasData)
            {
                var offset = stream.ReadLong();
                foreach (var message in DecodeMessage(offset, stream.ReadIntPrefixedBytes()))
                {
                    yield return message;
                }
            }
        }

        /// <summary>
        /// Encodes a message object to byte[]
        /// </summary>
        /// <param name="message">Message data to encode.</param>
        /// <returns>Encoded byte[] representation of the message object.</returns>
        /// <remarks>
        /// Format:
        /// Crc (Int32), MagicByte (Byte), Attribute (Byte), Key (Byte[]), Value (Byte[])
        /// </remarks>
        public byte[] EncodeMessage(Message message)
        {
            var body = new WriteByteStream();

            body.Pack(new[] { message.MagicNumber },
                      new[] { message.Attribute },
                      message.Key.ToIntSizedBytes(),
                      message.Value.ToIntSizedBytes());

            var crc = Crc32.ComputeHash(body.Payload());
            body.Prepend(crc);

            return body.Payload();
        }

        public IEnumerable<Message> DecodeMessage(long offset, byte[] payload)
        {
            var crc = payload.Take(4);
            var stream = new ReadByteStream(payload.Skip(4));

            if (crc.SequenceEqual(Crc32.ComputeHash(stream.Payload)) == false)
                throw new FailCrcCheckException("Payload did not match CRC validation.");

            var message = new Message
                {
                    Offset = offset,
                    MagicNumber = stream.ReadByte(),
                    Attribute = stream.ReadByte(),
                    Key = stream.ReadIntString(),
                    Value = stream.ReadIntString()
                };

            //TODO if message compressed then expand more messages
            //use attribute to determine compression

            yield return message;
        }

        public byte[] EncodeProduceRequest(ProduceRequest request)
        {
            var message = new WriteByteStream();

            if (request.Payload == null) request.Payload = new List<Payload>();

            var topicGroups = request.Payload.GroupBy(x => x.Topic).ToList();

            message.Pack(EncodeHeader(request)); //header
            message.Pack(request.Acks.ToBytes(), request.TimeoutMS.ToBytes(), topicGroups.Count.ToBytes()); //metadata

            foreach (var topicGroup in topicGroups)
            {
                var partitions = topicGroup.GroupBy(x => x.Partition).ToList();
                message.Pack(topicGroup.Key.ToInt16SizedBytes(), partitions.Count.ToBytes());

                foreach (var partition in partitions)
                {
                    var messageSet = EncodeMessageSet(partition.SelectMany(x => x.Messages));
                    message.Pack(partition.Key.ToBytes(), messageSet.Count().ToBytes(), messageSet);
                }
            }

            //prepend final messages size and return
            message.Prepend(message.Length().ToBytes());

            return message.Payload();
        }

        public IEnumerable<ProduceResponse> DecodeProduceResponse(byte[] data)
        {
            var stream = new ReadByteStream(data);

            var correlationId = stream.ReadInt();

            var topicCount = stream.ReadInt();
            for (int i = 0; i < topicCount; i++)
            {
                var topic = stream.ReadInt16String();

                var partitionCount = stream.ReadInt();
                for (int j = 0; j < partitionCount; j++)
                {
                    var response = new ProduceResponse()
                    {
                        Topic = topic,
                        PartitionId = stream.ReadInt(),
                        Error = stream.ReadInt16(),
                        Offset = stream.ReadLong()
                    };

                    yield return response;
                }
            }
        }

        /// <summary>
        /// Encode a request for metadata about topic and broker information.
        /// </summary>
        /// <param name="request">The MetaDataRequest to encode.</param>
        /// <returns>Encoded byte[] representing the request.</returns>
        /// <remarks>Format: (MessageSize), Header, ix(hs)</remarks>
        public byte[] EncodeMetadataRequest(MetadataRequest request)
        {
            var message = new WriteByteStream();

            if (request.Topics == null) request.Topics = new List<string>();

            message.Pack(EncodeHeader(request)); //header
            message.Pack(request.Topics.Count.ToBytes());
            message.Pack(request.Topics.Select(x => x.ToInt16SizedBytes()).ToArray());
            message.Prepend(message.Length().ToBytes());

            return message.Payload();
        }

        /// <summary>
        /// Decode the metadata response from kafka server.
        /// </summary>
        /// <param name="data"></param>
        /// <returns></returns>
        public MetadataResponse DecodeMetadataResponse(byte[] data)
        {
            var stream = new ReadByteStream(data);
            var response = new MetadataResponse();
            response.CorrelationId = stream.ReadInt();

            var brokerCount = stream.ReadInt();
            for (var i = 0; i < brokerCount; i++)
            {
                response.Brokers.Add(Broker.FromStream(stream));
            }

            var topicCount = stream.ReadInt();
            for (var i = 0; i < topicCount; i++)
            {
                response.Topics.Add(Topic.FromStream(stream));
            }

            return response;
        }

        public byte[] EncodeFetchRequest(FetchRequest request)
        {
            var message = new WriteByteStream();
            if (request.Fetches == null) request.Fetches = new List<Fetch>();

            message.Pack(EncodeHeader(request));

            var topicGroups = request.Fetches.GroupBy(x => x.Topic).ToList();
            message.Pack(ReplicaId.ToBytes(), request.MaxWaitTime.ToBytes(), request.MinBytes.ToBytes(), topicGroups.Count.ToBytes());

            foreach (var topicGroup in topicGroups)
            {
                var partitions = topicGroup.GroupBy(x => x.PartitionId).ToList();
                message.Pack(topicGroup.Key.ToInt16SizedBytes(), partitions.Count.ToBytes());

                foreach (var partition in partitions)
                {
                    foreach (var fetch in partition)
                    {
                        message.Pack(partition.Key.ToBytes(), fetch.Offset.ToBytes(), fetch.MaxBytes.ToBytes());
                    }
                }
            }

            message.Prepend(message.Length().ToBytes());

            return message.Payload();
        }

        public IEnumerable<FetchResponse> DecodeFetchResponse(byte[] data)
        {
            var stream = new ReadByteStream(data);

            var correlationId = stream.ReadInt();

            var topicCount = stream.ReadInt();
            for (int i = 0; i < topicCount; i++)
            {
                var topic = stream.ReadInt16String();

                var partitionCount = stream.ReadInt();
                for (int j = 0; j < partitionCount; j++)
                {
                    var response = new FetchResponse
                        {
                            Topic = topic,
                            PartitionId = stream.ReadInt(),
                            Error = stream.ReadInt16(),
                            HighWaterMark = stream.ReadLong()
                        };
                    response.Messages = DecodeMessageSet(stream.ReadIntPrefixedBytes()).ToList();
                    yield return response;
                }
            }
        }

        public byte[] EncodeOffsetRequest(OffsetRequest request)
        {
            var message = new WriteByteStream();
            if (request.Offsets == null) request.Offsets = new List<Offset>();

            message.Pack(EncodeHeader(request));

            var topicGroups = request.Offsets.GroupBy(x => x.Topic).ToList();
            message.Pack(ReplicaId.ToBytes(), topicGroups.Count.ToBytes());

            foreach (var topicGroup in topicGroups)
            {
                var partitions = topicGroup.GroupBy(x => x.PartitionId).ToList();
                message.Pack(topicGroup.Key.ToInt16SizedBytes(), partitions.Count.ToBytes());

                foreach (var partition in partitions)
                {
                    foreach (var offset in partition)
                    {
                        message.Pack(partition.Key.ToBytes(), offset.Time.ToBytes(), offset.MaxOffsets.ToBytes());
                    }
                }
            }

            message.Prepend(message.Length().ToBytes());

            return message.Payload();
        }

        public IEnumerable<OffsetResponse> DecodeOffsetResponse(byte[] data)
        {
            var stream = new ReadByteStream(data);

            var correlationId = stream.ReadInt();

            var topicCount = stream.ReadInt();
            for (int i = 0; i < topicCount; i++)
            {
                var topic = stream.ReadInt16String();

                var partitionCount = stream.ReadInt();
                for (int j = 0; j < partitionCount; j++)
                {
                    var response = new OffsetResponse()
                    {
                        Topic = topic,
                        PartitionId = stream.ReadInt(),
                        Error = stream.ReadInt16(),
                        Offsets = new List<long>()
                    };
                    var offsetCount = stream.ReadInt();
                    for (int k = 0; k < offsetCount; k++)
                    {
                        response.Offsets.Add(stream.ReadLong());
                    }

                    yield return response;
                }
            }
        }

        public byte[] EncodeOffsetCommitRequest(OffsetCommitRequest request)
        {
            var message = new WriteByteStream();
            if (request.OffsetCommits == null) request.OffsetCommits = new List<OffsetCommit>();

            message.Pack(EncodeHeader(request));
            message.Pack(request.ConsumerGroup.ToInt16SizedBytes());

            var topicGroups = request.OffsetCommits.GroupBy(x => x.Topic).ToList();
            message.Pack(topicGroups.Count.ToBytes());

            foreach (var topicGroup in topicGroups)
            {
                var partitions = topicGroup.GroupBy(x => x.PartitionId).ToList();
                message.Pack(topicGroup.Key.ToInt16SizedBytes(), partitions.Count.ToBytes());

                foreach (var partition in partitions)
                {
                    foreach (var commit in partition)
                    {
                        message.Pack(partition.Key.ToBytes(), commit.Offset.ToBytes(), commit.Metadata.ToInt16SizedBytes());
                    }
                }
            }

            message.Prepend(message.Length().ToBytes());

            return message.Payload();
        }

        public IEnumerable<OffsetCommitResponse> DecodeOffsetCommitResponse(byte[] data)
        {
            var stream = new ReadByteStream(data);

            var correlationId = stream.ReadInt();

            var topicCount = stream.ReadInt();
            for (int i = 0; i < topicCount; i++)
            {
                var topic = stream.ReadInt16String();

                var partitionCount = stream.ReadInt();
                for (int j = 0; j < partitionCount; j++)
                {
                    var response = new OffsetCommitResponse()
                    {
                        Topic = topic,
                        PartitionId = stream.ReadInt(),
                        Error = stream.ReadInt16()
                    };
                   
                    yield return response;
                }
            }
        }
    }
}
