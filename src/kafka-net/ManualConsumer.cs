using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using KafkaNet.Interfaces;
using KafkaNet.Protocol;

namespace KafkaNet
{
    /// <summary>
    /// This class implements the ManualConsumer in a NON thread safe manner
    /// </summary>
    public class ManualConsumer : IManualConsumer
    {
        private readonly string _topic;
        private readonly int _partitionId;
        private readonly SemaphoreSlim _semaphore;
        private Task<FetchResponse> _lastFetchTask;
        private readonly ProtocolGateway _gateway;
        private int _numOfBytesToWaitFor;        

        private const int MaxWaitTimeForKafka = 100;
        private const int UseBrokerTimestamp = -1;
        private const int NoOffsetFound = -1;

        public ManualConsumer(int partitionId, string topic, ProtocolGateway gateway)
        {
            if (string.IsNullOrEmpty(topic)) throw new ArgumentNullException("topic");
            if (gateway == null) throw new ArgumentNullException("gateway");

            _gateway = gateway;
            _partitionId = partitionId;
            _topic = topic;
            _semaphore = new SemaphoreSlim(1);
            _lastFetchTask = null;

            _numOfBytesToWaitFor = FetchRequest.DefaultBufferSize;
        }

        public async Task UpdateOffset(string consumerGroup, long offset)
        {
            await _semaphore.WaitAsync();

            try
            {
                OffsetCommitRequest request = CreateOffsetCommitRequest(offset, consumerGroup);
                await _gateway.SendProtocolRequest(request, _topic, _partitionId);                                
            }
            finally
            {
                _semaphore.Release();
            }
        }

        public async Task<long> GetLastOffset()
        {
            await _semaphore.WaitAsync();

            try
            {
                var request = CreateOffsetRequest();
                var response = await _gateway.SendProtocolRequest(request, _topic, _partitionId);
                return response.Offsets.Count > 0 ? response.Offsets.First() : NoOffsetFound;
            }
            finally
            {
                _semaphore.Release();
            }
        }

        public async Task<long> GetOffset(string consumerGroup)
        {
            await _semaphore.WaitAsync();

            try
            {
                OffsetFetchRequest offsetFetchrequest = CreateOffsetFetchRequest(consumerGroup);

                // TODO: Should also bring timeout?
                var response = await _gateway.SendProtocolRequest(offsetFetchrequest, _topic, _partitionId);
                return response.Offset;
            }
            finally
            {
                _semaphore.Release();
            }
        }

        public async Task<IEnumerable<Message>> GetMessages(int maxCount, long offset, TimeSpan timeout)
        {
            await _semaphore.WaitAsync();

            try
            {
                if (_lastFetchTask != null && 
                    _lastFetchTask.IsCompleted)
                {
                    if (_lastFetchTask.IsFaulted && _lastFetchTask.Exception != null)
                    {
                        throw _lastFetchTask.Exception;
                    }                    

                    // Checking if the last fetch task has the wanted batch of messages
                    var messages = _lastFetchTask.Result.Messages;
                    var startIndex = messages.FindIndex(m => m.Meta.Offset == offset);
                    if (startIndex != -1 &&
                        startIndex + maxCount <= messages.Count)
                    {
                        return messages.GetRange(startIndex, maxCount);
                    }
                }

                // If we arrived here, then we need to make a new fetch request and work with it

                FetchRequest request = CreateFetchRequest(offset);

                var taskToCheck = _gateway.SendProtocolRequest(request, _topic, _partitionId);
                await Task.WhenAny(taskToCheck, Task.Delay(timeout));

                // Saving the current task
                _lastFetchTask = taskToCheck;

                if (!taskToCheck.IsCompleted)
                {
                    // Didn't manage to fetch this round                    
                    return null;
                }
                
                // Task is completed
                if (taskToCheck.IsFaulted && taskToCheck.Exception != null)
                {
                    throw taskToCheck.Exception;
                }

                // We have a response
                FetchResponse response = taskToCheck.Result;

                if (response.Messages.Count == 0)
                {
                    return response.Messages;
                }

                // Saving the last consumed offset and Returning the wanted amount                
                var messagesToReturn = response.Messages.Take(maxCount);

                return messagesToReturn;
            }
            catch (BufferUnderRunException ex)
            {
                _numOfBytesToWaitFor = ex.RequiredBufferSize + ex.MessageHeaderSize;
                return null;
            }
            finally
            {
                _semaphore.Release();
            }            
        }

        private FetchRequest CreateFetchRequest(long offset)
        {
            Fetch fetch = new Fetch() { Offset = offset, PartitionId = _partitionId, Topic = _topic };

            FetchRequest request = new FetchRequest()
            {
                MaxWaitTime = MaxWaitTimeForKafka,
                MinBytes = _numOfBytesToWaitFor,
                Fetches = new List<Fetch>() { fetch }
            };

            return request;
        }

        private OffsetFetchRequest CreateOffsetFetchRequest(string consumerGroup)
        {
            OffsetFetch topicFetch = new OffsetFetch() {PartitionId = _partitionId, Topic = _topic};
            OffsetFetchRequest request = new OffsetFetchRequest()
            {
                ConsumerGroup = consumerGroup,
                Topics = new List<OffsetFetch>() {topicFetch}
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

            OffsetCommitRequest request = new OffsetCommitRequest() {ConsumerGroup = consumerGroup, OffsetCommits = new List<OffsetCommit>(){commit}};
            return request;
        }

        private OffsetRequest CreateOffsetRequest()
        {
            Offset offset = new Offset() {PartitionId = _partitionId, Topic = _topic, MaxOffsets = 1};

            OffsetRequest request = new OffsetRequest() {Offsets = new List<Offset>() {offset}};
            return request;            
        }
    }
}
