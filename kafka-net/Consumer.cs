using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using KafkaNet.Model;
using KafkaNet.Protocol;

namespace KafkaNet
{
    public class Consumer
    {
        private readonly KafkaOptions _options;
        private BrokerRouter _router;

        public Consumer(KafkaOptions options)
        {
            _options = options;
            _router = new BrokerRouter(_options);
        }

        public void SubscribeAsync(string topic, int partition, Func<string, Task> asyncCallback)
        {
            
        }

        private void PollRequests(string topic, int partitionId, int offset, int requestBatchSize = 10)
        {

            var route = _router.SelectBrokerRouteAsync(topic, partitionId).Result;

            var request = new OffsetRequest
                {
                    Offsets = new List<Offset>(new[]
                        {
                            new Offset
                                {
                                    Topic = topic,
                                    PartitionId = partitionId,
                                    MaxOffsets = 1,
                                    Time = -1
                                }
                        })
                };

            var offsetResponse = route.Connection.SendAsync(request).Result;

            var topicOffset = offsetResponse.FirstOrDefault(x => x.Topic == topic);
            if (topicOffset == null || topicOffset.Offsets.Count <= 0) return;

            //The max number returned is the next unassigned value in the kafka log.
            var maxOffset = topicOffset.Offsets.First();
            while (offset < maxOffset)
            {
                var fetches = new List<Fetch>();
                for (int i = 0; i < requestBatchSize; i++)
                {
                    fetches.Add(new Fetch{
                        Topic = topic,
                        PartitionId = partitionId,
                        Offset = offset++
                    });
                }
                var fetchRequest = new FetchRequest
                    {
                        Fetches = fetches
                    };

            }
        }

        private readonly ConcurrentDictionary<Topic, List<OffsetResponse>> _topicMaxOffset = new ConcurrentDictionary<Topic, List<OffsetResponse>>();
        private void UpdateTopicMaxOffset(Topic topic)
        {
            var route = _router.SelectBrokerRouteAsync(topic.Name).Result;
            var request = new OffsetRequest
                {
                    Offsets = new List<Offset>(topic.Partitions.Select(x => new Offset
                        {
                            Topic = topic.Name,
                            PartitionId = x.PartitionId,
                            MaxOffsets = 1,
                            Time = -1
                        }))
                };

            var response = route.Connection.SendAsync(request).Result;
            _topicMaxOffset.AddOrUpdate(topic, t => response, (t, list) => response);
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
    }
}
