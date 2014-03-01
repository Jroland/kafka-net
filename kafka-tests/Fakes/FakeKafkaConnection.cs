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

        public FakeKafkaConnection(Uri address)
        {
            _address = address;
        }

        public int MetadataRequestCallCount { get; set; }
        public int ProduceRequestCallCount { get; set; }

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
            var tcs = new TaskCompletionSource<List<T>>();

            if (typeof(T) == typeof(ProduceResponse))
            {
                ProduceRequestCallCount++;
                tcs.SetResult(new List<T> { (T)(object)ProduceResponseFunction() });
            }
            else if (typeof(T) == typeof(MetadataResponse))
            {
                MetadataRequestCallCount++;
                tcs.SetResult(new List<T> { (T)(object)MetadataResponseFunction() });
            }

            return tcs.Task;
        }

        public void Dispose()
        {

        }
    }
}
