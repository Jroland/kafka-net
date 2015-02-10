using System;
using System.Collections.Generic;
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
        private const int MaximumMessageBuffer = 100;
        private const int DefaultBatchDelayMS = 100;
        private const int DefaultBatchSize = 10;

        private readonly CancellationTokenSource _disposeToken = new CancellationTokenSource();
        private readonly IBrokerRouter _router;
        private readonly NagleBlockingCollection<TopicMessageBatch> _nagleBlockingCollection;
        private readonly IMetadataQueries _metadataQueries;
        private readonly Task _postTask;

        /// <summary>
        /// Get the lower bound of the message batches waiting to be sent. 
		/// Some messages may have been pulled into a send queue already, but not actually sent yet.
        /// </summary>
        public int ActiveCount { get { return _nagleBlockingCollection.Count; } }
        public int BatchSize { get; set; }
        public TimeSpan BatchDelayTime { get; set; }

        /// <summary>
        /// Construct a Producer class.
        /// </summary>
        /// <param name="brokerRouter">The router used to direct produced messages to the correct partition.</param>
        /// <param name="maximumMessageBuffer">The maximum async calls allowed before blocking new requests.  -1 indicates unlimited.</param>
        /// <remarks>
        /// The maximumAsyncQueue parameter provides a mechanism for blocking an async request return if the amount of requests queue is 
        /// over a certain limit.  This is usefull if a client is trying to push a large stream of documents through the producer and
        /// wants to block downstream if the producer is overloaded.
        /// 
        /// A message will start its timeout countdown as soon as it is added to the producer async queue.  If there are a large number of 
        /// messages sitting in the async queue then a message may spend its entire timeout cycle waiting in this queue and never getting
        /// attempted to send to Kafka before a timeout exception is thrown.
        /// </remarks>
        public Producer(IBrokerRouter brokerRouter, int maximumMessageBuffer = MaximumMessageBuffer)
        {
            _router = brokerRouter;
            _metadataQueries = new MetadataQueries(_router);
            _nagleBlockingCollection = new NagleBlockingCollection<TopicMessageBatch>(maximumMessageBuffer);

            BatchSize = DefaultBatchSize;
            BatchDelayTime = TimeSpan.FromMilliseconds(DefaultBatchDelayMS);

            _postTask = Task.Run(async () =>
            {
                await BatchSendAsync();
                //TODO add log for ending the sending thread.
            });
        }

        /// <summary>
        /// Send a enumerable of message objects to a given topic.
        /// </summary>
        /// <param name="topic">The name of the kafka topic to send the messages to.</param>
        /// <param name="messages">The enumerable of messages that will be sent to the given topic.</param>
        /// <param name="acks">The required level of acknowlegment from the kafka server.  0=none, 1=writen to leader, 2+=writen to replicas, -1=writen to all replicas.</param>
        /// <param name="timeout">Interal kafka timeout to wait for the requested level of ack to occur before returning. Defaults to 1000ms.</param>
        /// <param name="codec">The codec to apply to the message collection.  Defaults to none.</param>
        /// <returns>List of ProduceResponses from each partition sent to or empty list if acks = 0.</returns>
        public Task<List<ProduceResponse>> SendMessageAsync(string topic, IEnumerable<Message> messages, Int16 acks = 1,
            TimeSpan? timeout = null, MessageCodec codec = MessageCodec.CodecNone)
        {
            if (_disposeToken.IsCancellationRequested) throw new ObjectDisposedException("Cannot send new documents as producer is disposing.");
            if (timeout == null) timeout = TimeSpan.FromMilliseconds(DefaultAckTimeoutMS);

            var batch = new TopicMessageBatch
            {
                Acks = acks,
                Codec = codec,
                Timeout = timeout.Value,
                Topic = topic,
                Messages = messages.ToList()
            };

            _nagleBlockingCollection.Add(batch);
            return batch.Tcs.Task;
        }

        public Topic GetTopic(string topic)
        {
            return _metadataQueries.GetTopic(topic);
        }

        public Task<List<OffsetResponse>> GetTopicOffsetAsync(string topic, int maxOffsets = 2, int time = -1)
        {
            return _metadataQueries.GetTopicOffsetAsync(topic, maxOffsets, time);
        }

        public void Dispose()
        {
            //block incoming data
            _disposeToken.Cancel();

            //wait for the collection to drain
            _postTask.Wait(TimeSpan.FromSeconds(MaxDisposeWaitSeconds));

            //dispose
            using (_disposeToken)
            using (_nagleBlockingCollection)
            using (_metadataQueries)
            {


            }
        }

        private async Task BatchSendAsync()
        {
            while (_nagleBlockingCollection.IsComplete == false || _nagleBlockingCollection.Count > 0)
            {
                try
                {
                    if (_disposeToken.IsCancellationRequested && _nagleBlockingCollection.Count <= 0) break;

                    var batch = await _nagleBlockingCollection.TakeBatch(BatchSize, BatchDelayTime, _disposeToken.Token);

                    await ProduceAndSendBatchAsync(batch, _disposeToken.Token);
                }
                catch (OperationCanceledException ex)
                {
                    //TODO log that the operation was canceled, this only happens during a dispose
                }
                catch (Exception ex)
                {
                    //TODO log the failure and keep going
                }
            }
        }

        private async Task ProduceAndSendBatchAsync(List<TopicMessageBatch> batchs, CancellationToken cancellationToken)
        {
            //we must send a different produce request for each ack level and timeout combination.
            foreach (var ackBatch in batchs.GroupBy(batch => new { batch.Acks, batch.Timeout }))
            {
                var messageByRouter = ackBatch.SelectMany(topicBatch => topicBatch.Messages.Select(message => new
                                            {
                                                Topic = topicBatch.Topic,
                                                Codec = topicBatch.Codec,
                                                Route = _router.SelectBrokerRoute(topicBatch.Topic, message.Key),
                                                Message = message
                                            }))
                                         .GroupBy(x => new { Route = x.Route, x.Topic, x.Codec });

                var sendTasks = new List<BrokerRouteTaskTuple>();
                foreach (var group in messageByRouter)
                {
                    var request = new ProduceRequest
                    {
                        Acks = ackBatch.Key.Acks,
                        TimeoutMS = (int)ackBatch.Key.Timeout.TotalMilliseconds,
                        Payload = new List<Payload>
                                {
                                    new Payload
                                        {
                                            Codec = group.Key.Codec,
                                            Topic = group.Key.Topic,
                                            Partition = group.Key.Route.PartitionId,
                                            Messages = group.Select(x => x.Message).ToList()
                                        }
                                }
                    };

                    sendTasks.Add(new BrokerRouteTaskTuple { Route = group.Key.Route, Task = group.Key.Route.Connection.SendAsync(request) });
                }

                try
                {
                    await Task.WhenAll(sendTasks.Select(x => x.Task));

                    //match results to the sent batches and set the response
                    var results = sendTasks.SelectMany(x => x.Task.Result).ToList();

                    var batchResponses = ackBatch.GroupJoin(results, batch => batch.Topic, response => response.Topic,
                        (batch, responses) => new { Batch = batch, Responses = responses });

                    foreach (var batchResponse in batchResponses)
                    {
                        batchResponse.Batch.Tcs.TrySetResult(batchResponse.Responses.ToList());
                    }
                }
                catch
                {
                    //if an error occurs here, all we know is some or all of the messages in this ackBatch failed.
                    var failedTask = sendTasks.FirstOrDefault(t => t.Task.IsFaulted);
                    if (failedTask != null)
                    {
                        foreach (var topicMessageBatch in ackBatch)
                        {
                            topicMessageBatch.Tcs.TrySetException(new KafkaApplicationException("An exception occured while executing a send operation against {0}.  Exception:{1}",
                                failedTask.Route, failedTask.Task.Exception));
                        }
                    }
                }
            }
        }
    }


    class TopicMessageBatch
    {
        public TaskCompletionSource<List<ProduceResponse>> Tcs { get; set; }
        public short Acks { get; set; }
        public TimeSpan Timeout { get; set; }
        public MessageCodec Codec { get; set; }
        public string Topic { get; set; }
        public List<Message> Messages { get; set; }

        public TopicMessageBatch()
        {
            Tcs = new TaskCompletionSource<List<ProduceResponse>>();
        }
    }

    class BrokerRouteTaskTuple
    {
        public BrokerRoute Route { get; set; }
        public Task<List<ProduceResponse>> Task { get; set; } 
    }
}
