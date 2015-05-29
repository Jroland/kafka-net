using KafkaNet;
using KafkaNet.Model;
using KafkaNet.Protocol;
using System;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace kafka_tests.Fakes
{
    public class FakeKafkaConnection : IKafkaConnection
    {
        public Func<ProduceResponse> ProduceResponseFunction;
        public Func<MetadataResponse> MetadataResponseFunction;
        public Func<OffsetResponse> OffsetResponseFunction;
        public Func<FetchResponse> FetchResponseFunction;

        public FakeKafkaConnection(Uri address)
        {
            Endpoint = new DefaultKafkaConnectionFactory().Resolve(address, new DefaultTraceLog());
        }

        public int MetadataRequestCallCount { get; set; }
        public int ProduceRequestCallCount { get; set; }
        public int OffsetRequestCallCount { get; set; }
        public int FetchRequestCallCount { get; set; }

        public KafkaEndpoint Endpoint { get; private set; }

        public bool ReadPolling
        {
            get { return true; }
        }

        public Task SendAsync(KafkaDataPayload payload)
        {
            throw new NotImplementedException();
        }

        public Task<List<T>> SendAsync<T>(IKafkaRequest<T> request)
        {
            //start a thread to handle the request and return
            var task = new Task<List<T>>(() =>
            {
                if (typeof(T) == typeof(ProduceResponse))
                {
                    ProduceRequestCallCount++;
                    return new List<T> { (T)(object)ProduceResponseFunction() };
                }
                else if (typeof(T) == typeof(MetadataResponse))
                {
                    MetadataRequestCallCount++;
                    return new List<T> { (T)(object)MetadataResponseFunction() };
                }
                else if (typeof(T) == typeof(OffsetResponse))
                {
                    OffsetRequestCallCount++;
                    return new List<T> { (T)(object)OffsetResponseFunction() };
                }
                else if (typeof(T) == typeof(FetchResponse))
                {
                    FetchRequestCallCount++;
                    return new List<T> { (T)(object)FetchResponseFunction() };
                }

                return null;
            });

            task.Start();
            return task;
        }

        public void Dispose()
        {

        }
    }
}
