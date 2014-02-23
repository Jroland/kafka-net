using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Threading;
using System.Threading.Tasks;
using KafkaNet.Model;
using KafkaNet.Common;
using KafkaNet.Protocol;


namespace KafkaNet
{
    /// <summary>
    /// TODO: need to put in place a way to record and log errors.
    /// </summary>
    public class KafkaClient : IDisposable
    {
        private readonly KafkaClientOptions _kafkaOptions;
        private readonly BrokerRouter _router;

        public KafkaClient(KafkaClientOptions kafkaOptions)
        {
            _kafkaOptions = kafkaOptions;
            _router = new BrokerRouter(kafkaOptions);
        }

        public async Task<List<ProduceResponse>> SendMessageAsync(string topic, List<Message> messages, Int16 acks = 1, int timeoutMS = 1000)
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
            var response = await _router.GetTopicMetadataASync(topic);
            
            return response.Topics.First();
        }

        
        //public Task<MetadataResponse> SendAsync(MetadataRequest request)
        //{
        //    request.CorrelationId = NextCorrelationId();

        //    var responseCallback = RegisterAsyncResponse(request, _protocol.DecodeMetadataResponse);
        //    _conn.SendAsync(_protocol.EncodeMetadataRequest(request));
        //    return responseCallback;
        //}

        //public Task<List<FetchResponse>> SendAsync(FetchRequest request)
        //{
        //    request.CorrelationId = NextCorrelationId();

        //    var responseCallback = RegisterAsyncResponse(request, x => _protocol.DecodeFetchResponse(x).ToList());
        //    _conn.SendAsync(_protocol.EncodeFetchRequest(request));
        //    return responseCallback;
        //}

        //public Task<List<OffsetResponse>> SendAsync(OffsetRequest request)
        //{
        //    request.CorrelationId = NextCorrelationId();

        //    var responseCallback = RegisterAsyncResponse(request, x => _protocol.DecodeOffsetResponse(x).ToList());
        //    _conn.SendAsync(_protocol.EncodeOffsetRequest(request));
        //    return responseCallback;
        //}

        //public Task<List<OffsetCommitResponse>> SendAsync(OffsetCommitRequest request)
        //{
        //    throw new NotSupportedException("Not currently supported in kafka version 0.8.  https://issues.apache.org/jira/browse/KAFKA-993");
        //    //var response = await _conn.SendReceiveAsync(_protocol.EncodeOffsetCommitRequest(request));
        //    //return _protocol.DecodeOffsetCommitResponse(response).ToList();
        //}
        
        public void Dispose()
        {
            using (_router)
            {

            }
        }
    }

}
