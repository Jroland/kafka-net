using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using KafkaNet.Model;
using KafkaNet.Protocol;


namespace KafkaNet
{
    /// <summary>
    /// Provides a simplified high level API for producing messages on a topic.
    /// </summary>
    public class Producer : IDisposable
    {
        private readonly KafkaOptions _kafkaOptions;
        private readonly BrokerRouter _router;

        public Producer(KafkaOptions kafkaOptions)
        {
            _kafkaOptions = kafkaOptions;
            _router = new BrokerRouter(kafkaOptions);
        }

        public async Task<List<ProduceResponse>> SendMessageAsync(string topic, IEnumerable<Message> messages, Int16 acks = 1, int timeoutMS = 1000)
        {
            var routeGroup = new ConcurrentDictionary<BrokerRoute, List<Message>>();

            foreach (var message in messages)
            {
                var messageTemp = message;
                var route = await _router.SelectBrokerRouteAsync(topic, messageTemp.Key);
                routeGroup.AddOrUpdate(route, b => new List<Message>(new[] { messageTemp }), (b, list) => { list.Add(messageTemp); return list; });
            }

            var sendTasks = new List<Task<List<ProduceResponse>>>();
            foreach (var route in routeGroup.Keys)
            {
                var request = new ProduceRequest
                    {
                        Acks = acks,
                        TimeoutMS = timeoutMS,
                        Payload = new List<Payload>(new[] {new Payload{
                            Topic = route.Topic,
                            Partition = route.PartitionId,
                            Messages = routeGroup[route]
                        }})
                    };

                sendTasks.Add(route.Connection.SendAsync(request));
            }

            
            await Task.WhenAll(sendTasks.ToArray());
            
            return sendTasks.SelectMany(t => t.Result).ToList();
        }

        public async Task<Topic> GetTopicAsync(string topic)
        {
            var response = await _router.GetTopicMetadataAsync(topic);
            
            return response.First();
        }

        public void Dispose()
        {
            using (_router)
            {

            }
        }
    }

}
