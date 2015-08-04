using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
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
        private string _topic;
        private int _partitionId;
        private readonly SemaphoreSlim _semaphore;
        private Message[] _messages;
        private Task<FetchResponse> _lastFetchTask;
        private long _lastConsumedOffset;

        private const double RatioToMoveToArray = 0.01;

        public ManualConsumer(BrokerRouter router, int partitionId, string topic)
        {
            if (string.IsNullOrEmpty(topic)) throw new ArgumentNullException("topic");

            _partitionId = partitionId;
            _topic = topic;
            _semaphore = new SemaphoreSlim(1);
            _lastFetchTask = null;
        }

        public async Task UpdateOffset(string consumerGroup, long offset)
        {
            await _semaphore.WaitAsync();

            try
            {

                // TODO: Implement!
                throw new NotImplementedException();
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

                // TODO: Implement!
                throw new NotImplementedException();
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

                // TODO: Implement!
                throw new NotImplementedException();
            }
            finally
            {
                _semaphore.Release();
            }
        }

        public async Task<IEnumerable<Protocol.Message>> GetMessages(int maxCount, long offset, TimeSpan timeout)
        {
            await _semaphore.WaitAsync();

            try
            {
                FetchResponse response = null;

                if (_lastFetchTask != null)
                {
                    if (_lastFetchTask.IsCompleted)
                    {
                        response = _lastFetchTask.Result;
                    }
                }
                else
                {
                    int startIndex;

                    if (_messages != null &&
                        TryInRange(_messages, offset, maxCount, out startIndex))
                    {
                        // the bulk is in range, we can take messages from the array
                        return new ArraySegment<Message>(_messages, startIndex, maxCount).AsEnumerable();                        
                    }
                }

                // Checking if I have a response
                if (response == null)
                {
                    // TODO: Making a fetch request here
                    Task<FetchResponse> fetchTask = null;

                    await Task.WhenAny(fetchTask, Task.Delay(timeout));

                    if (!fetchTask.IsCompleted)
                    {
                        // Didn't manage to fetch this round
                        _lastFetchTask = fetchTask;
                        return null;
                    }
                }                

                // Checking the ratio..
                if (response.Messages.Count > 0 &&
                    (double) maxCount/response.Messages.Count <= RatioToMoveToArray)
                {
                    // Moving some of the messages to the array
                    _messages = response.Messages.Skip(maxCount).ToArray();
                }

                // Returning the wanted amount
                return response.Messages.Take(maxCount);                
            }
            finally
            {
                _semaphore.Release();
            }
        }

        private bool TryInRange(Message[] arrayMessages, long offset, int maxCount, out int startIndex)
        {
            bool isInRange = false;
            startIndex = -1;

            if (_messages.First().Meta.Offset <= offset &&
                _messages.Last().Meta.Offset >= offset + maxCount)
            {
                startIndex = Array.FindIndex(_messages, m => m.Meta.Offset == offset);

                if ((startIndex + maxCount) < _messages.Length)
                {
                    isInRange = true;
                }
            }

            return isInRange;
        }
    }
}
