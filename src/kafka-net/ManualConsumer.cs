using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using KafkaNet.Interfaces;
using KafkaNet.Protocol;

namespace KafkaNet
{
    /// <summary>
    /// This class implements the ManualConsumer in a thread safe manner.
    /// </summary>
    public class ManualConsumer : IManualConsumer
    {
        private readonly string _topic;
        private readonly int _partitionId;
        private readonly ProtocolGateway _gateway;
        private int _maxBytesInOneMessageIncludingHeader;
        private readonly string _clientId;
        private List<Message> _lastMessages;

        private const int MaxWaitTimeForKafka = 0;
        private const int UseBrokerTimestamp = -1;
        private const int NoOffsetFound = -1;
        private const double MessageSizeMultiplier = 1.5;

        public ManualConsumer(int partitionId, string topic, ProtocolGateway gateway, string clientId)
        {
            if (string.IsNullOrEmpty(topic)) throw new ArgumentNullException("topic");
            if (gateway == null) throw new ArgumentNullException("gateway");

            _gateway = gateway;
            _partitionId = partitionId;
            _topic = topic;            
            _clientId = clientId;

            _maxBytesInOneMessageIncludingHeader = FetchRequest.DefaultBufferSize;
        }

        /// <summary>
        /// Updating the cosumerGroup's offset for the partition in topic
        /// </summary>
        /// <param name="consumerGroup">The consumer group</param>
        /// <param name="offset">The new offset. must be larger than or equal to zero</param>
        /// <returns></returns>
        public async Task UpdateOrCreateOffset(string consumerGroup, long offset)
        {
            if (string.IsNullOrEmpty(consumerGroup)) throw new ArgumentNullException("consumerGroup");
            if (offset < 0) throw new ArgumentOutOfRangeException("offset", "offset must be positive or zero");

            OffsetCommitRequest request = CreateOffsetCommitRequest(offset, consumerGroup);
            await _gateway.SendProtocolRequest(request, _topic, _partitionId);                                
        }

        /// <summary>
        /// Get the max offset of the partition in the topic.
        /// </summary>
        /// <returns>The max offset, if no such offset found then returns -1</returns>
        public async Task<long> GetLastOffset()
        {
            var request = CreateGetLastOffsetRequest();
            var response = await _gateway.SendProtocolRequest(request, _topic, _partitionId);
            return response.Offsets.Count > 0 ? response.Offsets.First() : NoOffsetFound;
        }

        /// <summary>
        /// Getting the offset of a specific consumer group
        /// </summary>
        /// <param name="consumerGroup">The name of the consumer group</param>
        /// <returns>The current offset of the consumerGroup</returns>
        public async Task<long> GetOffset(string consumerGroup)
        {
            if (string.IsNullOrEmpty(consumerGroup)) throw new ArgumentNullException("consumerGroup");

            OffsetFetchRequest offsetFetchRequest = CreateOffsetFetchRequest(consumerGroup);

            // TODO: Should also bring timeout?
            var response = await _gateway.SendProtocolRequest(offsetFetchRequest, _topic, _partitionId);
            return response.Offset;
        }

        /// <summary>
        /// Getting messages from the kafka queue
        /// </summary>
        /// <param name="maxCount">The maximum amount of messages wanted. The function will return at most the wanted number of messages</param>
        /// <param name="offset">The offset to start from</param>
        /// <returns>An enumerable of the messages</returns>
        public async Task<IEnumerable<Message>> GetMessages(int maxCount, long offset)
        {
            if (offset < 0) throw new ArgumentOutOfRangeException("offset", "offset must be positive or zero");

            try
            {                 
                // Checking if the last fetch task has the wanted batch of messages
                if (_lastMessages != null)
                {
                    var startIndex = _lastMessages.FindIndex(m => m.Meta.Offset == offset);
                    var containsAllMessage = startIndex != -1 && startIndex + maxCount <= _lastMessages.Count;
                    if (containsAllMessage)
                    {
                        return _lastMessages.GetRange(startIndex, maxCount);
                    }
                }

                // If we arrived here, then we need to make a new fetch request and work with it
                FetchRequest request = CreateFetchRequest(offset);

                var response = await _gateway.SendProtocolRequest(request, _topic, _partitionId);

                if (response.Messages.Count == 0)
                {
                    return response.Messages;
                }

                // Saving the last consumed offset and Returning the wanted amount                
                _lastMessages = response.Messages;
                var messagesToReturn = response.Messages.Take(maxCount);

                return messagesToReturn;
            }
            catch (BufferUnderRunException ex)
            {
                _maxBytesInOneMessageIncludingHeader = (int)((ex.RequiredBufferSize + ex.MessageHeaderSize) * MessageSizeMultiplier);
                return null;
            }         
        }

        private FetchRequest CreateFetchRequest(long offset)
        {
            Fetch fetch = new Fetch() { Offset = offset, PartitionId = _partitionId, Topic = _topic,MaxBytes = _maxBytesInOneMessageIncludingHeader};

            FetchRequest request = new FetchRequest()
            {
                MaxWaitTime = MaxWaitTimeForKafka,
                MinBytes = 0,
                Fetches = new List<Fetch>() { fetch },
                ClientId = _clientId
            };

            return request;
        }

        private OffsetFetchRequest CreateOffsetFetchRequest(string consumerGroup)
        {
            OffsetFetch topicFetch = new OffsetFetch() {PartitionId = _partitionId, Topic = _topic};
            OffsetFetchRequest request = new OffsetFetchRequest()
            {
                ConsumerGroup = consumerGroup,
                Topics = new List<OffsetFetch>() {topicFetch},
                ClientId = _clientId
            };

            return request;            
        }

        private OffsetCommitRequest CreateOffsetCommitRequest(long offset, string consumerGroup)
        {
            OffsetCommit commit = new OffsetCommit()
            {
                Offset = offset,
                Topic = _topic,
                PartitionId = _partitionId,
                TimeStamp = UseBrokerTimestamp
            };

            OffsetCommitRequest request = new OffsetCommitRequest()
            {
                ConsumerGroup = consumerGroup, 
                OffsetCommits = new List<OffsetCommit>(){commit}, 
                ClientId = _clientId
            };

            return request;
        }

        private OffsetRequest CreateGetLastOffsetRequest()
        {
            Offset offset = new Offset() {PartitionId = _partitionId, Topic = _topic, MaxOffsets = 1};

            OffsetRequest request = new OffsetRequest() {Offsets = new List<Offset>() {offset}, ClientId = _clientId};
            return request;            
        }
    }
}
