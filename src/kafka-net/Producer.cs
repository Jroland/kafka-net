using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using KafkaNet.Common;
using KafkaNet.Protocol;


namespace KafkaNet
{
    /// <summary>
    /// Provides a simplified high level API for producing messages on a topic.
    /// </summary>
    public class Producer : IMetadataQueries
    {
        private const int MaxDisposeWaitSeconds = 30;
        private const int DefaultAckTimeoutMS = 1000;
        private const int MaximumAsyncRequests = 20;
        private const int MaximumMessageBuffer = 1000;
        private const int DefaultBatchDelayMS = 100;
        private const int DefaultBatchSize = 100;

        private readonly CancellationTokenSource _stopToken = new CancellationTokenSource();
        private readonly int _maximumAsyncRequests;
        private readonly AsyncCollection<TopicMessage> _asyncCollection;
        private readonly SemaphoreSlim _semaphoreMaximumAsync;
        private readonly IMetadataQueries _metadataQueries;
        private readonly Task _postTask;

        private int _inFlightMessageCount = 0;

        /// <summary>
        /// Get the number of messages sitting in the buffer waiting to be sent. 
        /// </summary>
        public int BufferCount { get { return _asyncCollection.Count; } }

        /// <summary>
        /// Get the number of messages staged for Async upload.
        /// </summary>
        public int InFlightMessageCount { get { return _inFlightMessageCount; } }

        /// <summary>
        /// Get the number of active async threads sending messages.
        /// </summary>
        public int AsyncCount { get { return _maximumAsyncRequests - _semaphoreMaximumAsync.CurrentCount; } }

        /// <summary>
        /// The number of messages to wait for before sending to kafka.  Will wait <see cref="BatchDelayTime"/> before sending whats received.
        /// </summary>
        public int BatchSize { get; set; }

        /// <summary>
        /// The time to wait for a batch size of <see cref="BatchSize"/> before sending messages to kafka.
        /// </summary>
        public TimeSpan BatchDelayTime { get; set; }

        /// <summary>
        /// The broker router this producer uses to route messages.
        /// </summary>
        public IBrokerRouter BrokerRouter { get; private set; }

        /// <summary>
        /// Construct a Producer class.
        /// </summary>
        /// <param name="brokerRouter">The router used to direct produced messages to the correct partition.</param>
        /// <param name="maximumAsyncRequests">The maximum async calls allowed before blocking new requests.  -1 indicates unlimited.</param>
        /// <param name="maximumMessageBuffer">The maximum amount of messages to buffer if the async calls are blocking from sending.</param>
        /// <remarks>
        /// The maximumAsyncRequests parameter provides a mechanism for minimizing the amount of async requests in flight at any one time
        /// by blocking the caller requesting the async call.  This affectively puts an upper limit on the amount of times a caller can 
        /// call SendMessageAsync before the caller is blocked.
        /// 
        /// The MaximumMessageBuffer parameter provides a way to limit the max amount of memory the driver uses should the send pipeline get
        /// overwhelmed and the buffer starts to fill up.  This is an inaccurate limiting memory use as the amount of memory actually used is 
        /// dependant on the general message size being buffered.
        /// 
        /// A message will start its timeout countdown as soon as it is added to the producer async queue.  If there are a large number of 
        /// messages sitting in the async queue then a message may spend its entire timeout cycle waiting in this queue and never getting
        /// attempted to send to Kafka before a timeout exception is thrown.
        /// </remarks>
        public Producer(IBrokerRouter brokerRouter, int maximumAsyncRequests = MaximumAsyncRequests, int maximumMessageBuffer = MaximumMessageBuffer)
        {
            BrokerRouter = brokerRouter;
            _maximumAsyncRequests = maximumAsyncRequests;
            _metadataQueries = new MetadataQueries(BrokerRouter);
            _asyncCollection = new AsyncCollection<TopicMessage>();
            _semaphoreMaximumAsync = new SemaphoreSlim(maximumAsyncRequests, maximumAsyncRequests);

            BatchSize = DefaultBatchSize;
            BatchDelayTime = TimeSpan.FromMilliseconds(DefaultBatchDelayMS);

            _postTask = Task.Run(async () =>
            {
                await BatchSendAsync().ConfigureAwait(false);
                //TODO add log for ending the sending thread.
            });

        }

        /// <summary>
        /// Send an enumerable of message objects to a given topic.
        /// </summary>
        /// <param name="topic">The name of the kafka topic to send the messages to.</param>
        /// <param name="messages">The enumerable of messages that will be sent to the given topic.</param>
        /// <param name="acks">The required level of acknowlegment from the kafka server.  0=none, 1=writen to leader, 2+=writen to replicas, -1=writen to all replicas.</param>
        /// <param name="timeout">Interal kafka timeout to wait for the requested level of ack to occur before returning. Defaults to 1000ms.</param>
        /// <param name="codec">The codec to apply to the message collection.  Defaults to none.</param>
        /// <returns>List of ProduceResponses from each partition sent to or empty list if acks = 0.</returns>
        public async Task<List<ProduceResponse>> SendMessageAsync(string topic, IEnumerable<Message> messages, Int16 acks = 1,
            TimeSpan? timeout = null, MessageCodec codec = MessageCodec.CodecNone)
        {
            if (_stopToken.IsCancellationRequested)
                throw new ObjectDisposedException("Cannot send new documents as producer is disposing.");
            if (timeout == null) timeout = TimeSpan.FromMilliseconds(DefaultAckTimeoutMS);

            var batch = messages.Select(message => new TopicMessage
            {
                Acks = acks,
                Codec = codec,
                Timeout = timeout.Value,
                Topic = topic,
                Message = message
            }).ToList();

            _asyncCollection.AddRange(batch);

            await Task.WhenAll(batch.Select(x => x.Tcs.Task));

            return batch.Select(topicMessage => topicMessage.Tcs.Task.Result)
                                .Distinct()
                                .ToList();
        }

        /// <summary>
        /// Get the metadata about a given topic.
        /// </summary>
        /// <param name="topic">The name of the topic to get metadata for.</param>
        /// <returns>Topic with metadata information.</returns>
        public Topic GetTopic(string topic)
        {
            return _metadataQueries.GetTopic(topic);
        }


        public Task<List<OffsetResponse>> GetTopicOffsetAsync(string topic, int maxOffsets = 2, int time = -1)
        {
            return _metadataQueries.GetTopicOffsetAsync(topic, maxOffsets, time);
        }

        /// <summary>
        /// Stops the producer from accepting new messages, and optionally waits for in-flight messages to be sent before returning.
        /// </summary>
        /// <param name="waitForRequestsToComplete">True to wait for in-flight requests to complete, false otherwise.</param>
        /// <param name="maxWait">Maximum time to wait for in-flight requests to complete. Has no effect if <c>waitForRequestsToComplete</c> is false</param>
        public void Stop(bool waitForRequestsToComplete = true, TimeSpan? maxWait = null)
        {
            //block incoming data
            _asyncCollection.CompleteAdding();

            if (waitForRequestsToComplete)
            {
                //wait for the collection to drain
                _postTask.Wait(maxWait ?? TimeSpan.FromSeconds(MaxDisposeWaitSeconds));
            }

            _stopToken.Cancel();
        }

        private async Task BatchSendAsync()
        {
            var outstandingSendTasks = new System.Collections.Concurrent.ConcurrentDictionary<Task, Task>();
            while (_asyncCollection.IsCompleted == false || _asyncCollection.Count > 0)
            {
                List<TopicMessage> batch = null;

                try
                {
                    try
                    {
                        await _asyncCollection.OnHasDataAvailable(_stopToken.Token).ConfigureAwait(false);

                        batch = await _asyncCollection.TakeAsync(BatchSize, BatchDelayTime, _stopToken.Token).ConfigureAwait(false);
                    }
                    catch (OperationCanceledException ex)
                    {
                        //TODO log that the operation was canceled, this only happens during a dispose
                    }

                    if (_asyncCollection.IsCompleted && _asyncCollection.Count > 0)
                    {
                        batch = batch ?? new List<TopicMessage>(_asyncCollection.Count);

                        //Drain any messages remaining in the queue and add them to the send batch
                        batch.AddRange(_asyncCollection.Drain());
                    }

                    //we want to fire the batch without blocking and then move on to fire another one
                    var sendTask = ProduceAndSendBatchAsync(batch, _stopToken.Token);

                    outstandingSendTasks.TryAdd(sendTask, sendTask);

                    var sendTaskCleanup = sendTask.ContinueWith(result =>
                    {
                        if (result.IsFaulted && batch != null)
                        {
                            batch.ForEach(x => x.Tcs.TrySetException(result.ExtractException()));
                        }

                        //TODO add statistics tracking
                        outstandingSendTasks.TryRemove(sendTask, out sendTask);
                    });

                }
                catch (Exception ex)
                {
                    if (batch != null)
                    {
                        batch.ForEach(x => x.Tcs.TrySetException(ex));
                    }
                }
            }

            var referenceToOutstanding = outstandingSendTasks.Values.ToList();
            if (referenceToOutstanding.Count > 0)
            {
                await Task.WhenAll(referenceToOutstanding).ConfigureAwait(false);
            }
        }

        private async Task ProduceAndSendBatchAsync(List<TopicMessage> messages, CancellationToken cancellationToken)
        {
            Interlocked.Add(ref _inFlightMessageCount, messages.Count);

            //we must send a different produce request for each ack level and timeout combination.
            foreach (var ackLevelBatch in messages.GroupBy(batch => new { batch.Acks, batch.Timeout }))
            {
                var messageByRouter = ackLevelBatch.Select(batch => new
                {
                    TopicMessage = batch,
                    Route = BrokerRouter.SelectBrokerRoute(batch.Topic, batch.Message.Key),
                })
                                         .GroupBy(x => new { x.Route, x.TopicMessage.Topic, x.TopicMessage.Codec });

                var sendTasks = new List<BrokerRouteSendBatch>();
                foreach (var group in messageByRouter)
                {
                    var payload = new Payload
                    {
                        Codec = group.Key.Codec,
                        Topic = group.Key.Topic,
                        Partition = group.Key.Route.PartitionId,
                        Messages = group.Select(x => x.TopicMessage.Message).ToList()
                    };

                    var request = new ProduceRequest
                    {
                        Acks = ackLevelBatch.Key.Acks,
                        TimeoutMS = (int)ackLevelBatch.Key.Timeout.TotalMilliseconds,
                        Payload = new List<Payload> { payload }
                    };

                    await _semaphoreMaximumAsync.WaitAsync(cancellationToken).ConfigureAwait(false);

                    var brokerSendTask = new BrokerRouteSendBatch
                    {
                        Route = group.Key.Route,
                        Task = group.Key.Route.Connection.SendAsync(request),
                        MessagesSent = group.Select(x => x.TopicMessage).ToList()
                    };

                    //ensure the async is released as soon as each task is completed
                    brokerSendTask.Task.ContinueWith(t => { _semaphoreMaximumAsync.Release(); }, cancellationToken);

                    sendTasks.Add(brokerSendTask);
                }

                try
                {
                    await Task.WhenAll(sendTasks.Select(x => x.Task)).ConfigureAwait(false);

                    foreach (var task in sendTasks)
                    {
                        //TODO when we dont ask for an ACK, result is an empty list.  Which FirstOrDefault returns null.  Dont like this...
                        task.MessagesSent.ForEach(x => x.Tcs.TrySetResult(task.Task.Result.FirstOrDefault()));
                    }
                }
                catch
                {
                    //if an error occurs here, all we know is some or all of the messages in this ackBatch failed.
                    var failedTask = sendTasks.FirstOrDefault(t => t.Task.IsFaulted);
                    if (failedTask != null)
                    {
                        foreach (var topicMessageBatch in ackLevelBatch)
                        {
                            topicMessageBatch.Tcs.TrySetException(
                                new KafkaApplicationException(
                                    "An exception occured while executing a send operation against {0}.  Exception:{1}",
                                    failedTask.Route, failedTask.Task.Exception));
                        }
                    }
                }
                finally
                {
                    Interlocked.Add(ref _inFlightMessageCount, messages.Count * -1);
                }
            }
        }

        #region Dispose...
        public void Dispose()
        {
            //Clients really should call Stop() first, but just in case they didn't...
            this.Stop(false);

            //dispose
            using (_stopToken)
            using (_metadataQueries)
            {
            }
        }
        #endregion
    }

    class TopicMessage
    {
        public TaskCompletionSource<ProduceResponse> Tcs { get; set; }
        public short Acks { get; set; }
        public TimeSpan Timeout { get; set; }
        public MessageCodec Codec { get; set; }
        public string Topic { get; set; }
        public Message Message { get; set; }

        public TopicMessage()
        {
            Tcs = new TaskCompletionSource<ProduceResponse>();
        }
    }

    class BrokerRouteSendBatch
    {
        public BrokerRoute Route { get; set; }
        public Task<List<ProduceResponse>> Task { get; set; }
        public List<TopicMessage> MessagesSent { get; set; }
    }
}
