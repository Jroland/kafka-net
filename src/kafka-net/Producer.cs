using System;
using System.Collections.Generic;
using System.Linq;
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
        private const int DefaultAckTimeoutMS = 1000;
        private const int MaximumMessageBuffer = 100;
        private const int DefaultBatchDelayMS = 100;
        private const int DefaultBatchSize = 10;

        private readonly IBrokerRouter _router;
        private readonly NagleBlockingCollection<TopicMessageBatch> _nagleBlockingCollection;
        private readonly IMetadataQueries _metadataQueries;

        /// <summary>
        /// Get the current message awaiting send
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

            Task.Run(async () =>
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
            using (_nagleBlockingCollection)
            using (_metadataQueries)
            {


            }
        }

        private async Task BatchSendAsync()
        {
            while (_nagleBlockingCollection.IsComplete == false || _nagleBlockingCollection.Count > 0)
            {
                var batch = await _nagleBlockingCollection.TakeBatch(BatchSize, BatchDelayTime);
                //TODO handle exceptions
                ProduceAndSendBatch(batch);
            }
        }

        private void ProduceAndSendBatch(List<TopicMessageBatch> batchs)
        {
            //we must send a different produce request for each ack level and timeout combination.
            foreach (var ackBatch in batchs.GroupBy(batch => new { batch.Acks, batch.Timeout }))
            {
                var messageByRouter = ackBatch.SelectMany(topicBatch => topicBatch.Messages.Select(message => new
                                            {
                                                Topic = topicBatch.Topic,
                                                Codec = topicBatch.Codec,
                                                Router = _router.SelectBrokerRoute(topicBatch.Topic, message.Key),
                                                Message = message
                                            }))
                                         .GroupBy(x => new { x.Router, x.Topic, x.Codec });

                var sendTasks = new List<Task<List<ProduceResponse>>>();
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
                                            Partition = group.Key.Router.PartitionId,
                                            Messages = group.Select(x => x.Message).ToList()
                                        }
                                }
                    };

                    sendTasks.Add(group.Key.Router.Connection.SendAsync(request));
                }

                //match results to the sent batches and set the response
                var results = sendTasks.SelectMany(x => x.Result).ToList();

                var batchResponses = batchs.GroupJoin(results, batch => batch.Topic, response => response.Topic,
                    (batch, responses) => new { Batch = batch, Responses = responses });

                foreach (var batchResponse in batchResponses)
                {
                    batchResponse.Batch.Tcs.TrySetResult(batchResponse.Responses.ToList());
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
}
