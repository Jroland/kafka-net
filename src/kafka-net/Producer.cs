using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using KafkaNet.Model;
using KafkaNet.Protocol;
using System.Threading;


namespace KafkaNet
{
    /// <summary>
    /// Provides a simplified high level API for producing messages on a topic.
    /// </summary>
    public class Producer : CommonQueries
    {
        private readonly IBrokerRouter _router;
        private int _currentAsyncQueue;

        /// <summary>
        /// Semaphore for reducing concurrent threads in SendMessageAsync
        /// </summary>
        private readonly SemaphoreSlim _sendSemaphore;

        /// <summary>
        /// Construct a Producer class.
        /// </summary>
        /// <param name="brokerRouter">The router used to direct produced messages to the correct partition.</param>
        /// <param name="maximumAsyncQueue">The maximum async calls allowed before blocking new requests.  -1 indicates unlimited.</param>
        /// <remarks>
        /// The maximumAsyncQueue parameter provides a mechanism for blocking an async request return if the amount of requests queue is 
        /// over a certain limit.  This is usefull if a client is trying to push a large stream of documents through the producer and
        /// wants to block downstream if the producer is overloaded.
        /// 
        /// A message will start its timeout countdown as soon as it is added to the producer async queue.  If there are a large number of 
        /// messages sitting in the async queue then a message may spend its entire timeout cycle waiting in this queue and never getting
        /// attempted to send to Kafka before a timeout exception is thrown.
        /// </remarks>
        public Producer(IBrokerRouter brokerRouter, int maximumAsyncQueue = -1)
            : base(brokerRouter)
        {
            _router = brokerRouter;

            maximumAsyncQueue = maximumAsyncQueue == -1 ? int.MaxValue : maximumAsyncQueue;
            _sendSemaphore = new SemaphoreSlim(maximumAsyncQueue, maximumAsyncQueue);
        }

        /// <summary>
        /// Send a enumerable of message objects to a given topic.
        /// </summary>
        /// <param name="topic">The name of the kafka topic to send the messages to.</param>
        /// <param name="messages">The enumerable of messages that will be sent to the given topic.</param>
        /// <param name="acks">The required level of acknowlegment from the kafka server.  0=none, 1=writen to leader, 2+=writen to replicas, -1=writen to all replicas.</param>
        /// <param name="timeoutMS">Interal kafka timeout to wait for the requested level of ack to occur before returning.</param>
        /// <param name="codec">The codec to apply to the message collection.  Defaults to none.</param>
        /// <returns>List of ProduceResponses for each message sent or empty list if acks = 0.</returns>
        public async Task<List<ProduceResponse>> SendMessageAsync(string topic, IEnumerable<Message> messages, Int16 acks = 1, int timeoutMS = 1000, MessageCodec codec = MessageCodec.CodecNone)
        {
            _sendSemaphore.Wait();

            try
            {
                //group message by the server connection they will be sent to
                var routeGroup = from message in messages
                                 select new {Route = _router.SelectBrokerRoute(topic, message.Key), Message = message}
                                 into routes
                                 group routes by routes.Route;
                
                var sendTasks = new List<Task<List<ProduceResponse>>>();
                foreach (var route in routeGroup)
                {
                    var request = new ProduceRequest
                        {
                            Acks = acks,
                            TimeoutMS = timeoutMS,
                            Payload = new List<Payload>
                                {
                                    new Payload
                                        {
                                            Codec = codec,
                                            Topic = route.Key.Topic,
                                            Partition = route.Key.PartitionId,
                                            Messages = route.Select(x => x.Message).ToList()
                                        }
                                }
                        };

                    sendTasks.Add(route.Key.Connection.SendAsync(request));
                }

                await Task.WhenAll(sendTasks.ToArray());
                return sendTasks.SelectMany(t => t.Result).ToList();
            }
            finally
            {
                _sendSemaphore.Release();
            }
        }


    }

}
