using KafkaNet;
using KafkaNet.Protocol;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace kafka_tests.Fakes
{
    public class FakeKafkaConnection : IKafkaConnection
    {
        private Uri _address;

        public Func<ProduceResponse> ProduceResponseFunction;
        public Func<MetadataResponse> MetadataResponseFunction;
        public Func<OffsetResponse> OffsetResponseFunction;
        public Func<FetchResponse> FetchResponseFunction;

        public FakeKafkaConnection(Uri address)
        {
            _address = address;
        }

        public int MetadataRequestCallCount { get; set; }
        public int ProduceRequestCallCount { get; set; }
        public int OffsetRequestCallCount { get; set; }
        public int FetchRequestCallCount { get; set; }

        public Uri KafkaUri
        {
            get { return _address; }
        }

        public bool ReadPolling
        {
            get { return true; }
        }

        public Task SendAsync(byte[] payload)
        {
            throw new NotImplementedException();
        }

        public Task<List<T>> SendAsync<T>(IKafkaRequest<T> request)
        {
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
